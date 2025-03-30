import requests
import pandas as pd
import time
import os
from datetime import datetime
import logging

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# Configuration
BATCH_SIZE = 1000
MAX_RETRIES = 10
RETRY_DELAY = 20
TIMEOUT = 300
SAVE_EVERY_N_BATCHES = 50
WIKIDATA_URL = "https://query.wikidata.org/sparql"
HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "wiki_german_de/1.0 (soc.evgeniiatcoi@gmail.com)",
    "Accept-Encoding": "gzip,deflate"
}

# Function to query Wikidata
def query_wikidata(query, max_retries=MAX_RETRIES, retry_delay=RETRY_DELAY, timeout=TIMEOUT):
    for attempt in range(max_retries):
        try:
            logger.info(f"Attempt {attempt + 1}/{max_retries}: Querying Wikidata...")
            response = requests.get(WIKIDATA_URL, headers=HEADERS, params={"query": query}, timeout=timeout)
            response.raise_for_status()

            data = response.json()
            results = data.get("results", {}).get("bindings", [])

            if results:
                return pd.DataFrame.from_records(results)
            else:
                return pd.DataFrame()  # Return an empty DataFrame if no results

        except requests.exceptions.RequestException as e:
            logger.error(f"Request error (Attempt {attempt + 1}/{max_retries}): {e}")
            time.sleep(retry_delay * (2 ** attempt))  # Exponential backoff

    logger.error("Max retries reached. Unable to fetch data.")
    return None

# Function to save the ID pointer
def save_pointer(pointer, filename="id_pointer.txt"):
    with open(filename, "w") as f:
        f.write(pointer)
    logger.info(f"Pointer saved: {pointer}")

# Function to load the ID pointer
def load_pointer(filename="id_pointer.txt"):
    try:
        if os.path.exists(filename):
            with open(filename, "r") as f:
                pointer = f.read().strip()
            logger.info(f"Resuming from pointer: {pointer}")
            return pointer
    except Exception as e:
        logger.error(f"Error reading pointer file '{filename}': {e}")
    return None

# Function to save intermediate results
def save_intermediate_results(results, filename_prefix="missing_results"):
    try:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{filename_prefix}_{timestamp}.csv"
        pd.concat(results, ignore_index=True).to_csv(filename, index=False)
        logger.info(f"Saved intermediate results to {filename}.")
    except Exception as e:
        logger.error(f"Error saving intermediate results: {e}")

# Function to process and clean the DataFrame
def process_results(df):
    """
    Extracts the 'value' field from dictionary-like columns in the DataFrame.
    """
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.get("value", None) if isinstance(x, dict) else x)
    return df

# Main function to fetch data for a list of Wikidata IDs
def fetch_data_for_ids(file_path="missing_persons.csv"):
    # Read the list of Wikidata IDs from the CSV file
    if not os.path.exists(file_path):
        logger.error(f"File not found: {file_path}")
        return

    try:
        df = pd.read_csv(file_path)
        if "wikidata_id" not in df.columns:
            logger.error(f"CSV file must contain a 'wikidata_id' column.")
            return
        wikidata_ids = df["wikidata_id"].dropna().apply(lambda x: x.split("/")[-1] if "/" in x else x).tolist()
    except Exception as e:
        logger.error(f"Error reading CSV file: {e}")
        return

    if not wikidata_ids:
        logger.warning("No Wikidata IDs found in the file.")
        return

    # Load the last processed ID
    last_pointer = load_pointer()
    if last_pointer:
        try:
            last_index = wikidata_ids.index(last_pointer)
            wikidata_ids = wikidata_ids[last_index + 1:]  # Resume from the next ID
        except ValueError:
            logger.warning(f"Pointer ID '{last_pointer}' not found in the list. Starting from the beginning.")

    all_results = []
    batch_count = 0

    for wikidata_id in wikidata_ids:
        logger.info(f"Fetching data for Wikidata ID: {wikidata_id}...")

        query = f"""
            PREFIX schema: <http://schema.org/>
            PREFIX wd: <http://www.wikidata.org/entity/>
            PREFIX wdt: <http://www.wikidata.org/prop/direct/>
            PREFIX wikibase: <http://wikiba.se/ontology#>
            PREFIX bd: <http://www.bigdata.com/rdf#>
            PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
            PREFIX geo: <http://www.opengis.net/ont/geosparql#>

            SELECT ?person ?personLabel ?birthdate ?birthplace ?placeOfDeath ?birthplaceCoordinates
            WHERE {{
                BIND(wd:{wikidata_id} AS ?person)  # Specific Wikidata ID
                ?person wdt:P569 ?birthdate.  # Ensure the person has a birthdate
                ?person wdt:P19 ?birthplace.  # Birthplace
                OPTIONAL {{ ?person wdt:P20 ?placeOfDeath. }}  # Place of death (optional)
                OPTIONAL {{ ?birthplace wdt:P625 ?birthplaceCoordinates. }}  # Geo-coordinates of birthplace

                SERVICE wikibase:label {{
                    bd:serviceParam wikibase:language "de".
                    ?person rdfs:label ?personLabel .
                }}
            }}
        """

        batch_results = query_wikidata(query)

        if batch_results is None:  # Query failed after max retries
            logger.error(f"Failed to fetch data for Wikidata ID: {wikidata_id}. Skipping...")
            continue

        if batch_results.empty:  # No data for the current ID
            logger.info(f"No results for Wikidata ID: {wikidata_id}.")
            continue

        # Process and clean the results
        batch_results = process_results(batch_results)
        all_results.append(batch_results)
        batch_count += 1
        logger.info(f"Fetched {len(batch_results)} records for Wikidata ID: {wikidata_id}.")

        # Save the current ID as the pointer
        save_pointer(wikidata_id)

        # Save intermediate results periodically
        if batch_count % SAVE_EVERY_N_BATCHES == 0:
            save_intermediate_results(all_results, filename_prefix="missing_persons_results")
            all_results = []  # Clear memory to avoid excessive usage

    # Save final results
    if all_results:
        try:
            final_df = pd.concat(all_results, ignore_index=True)
            final_df.to_csv("missing_persons_geodata.csv", index=False)
            logger.info("Final data saved to 'missing_persons_geodata.csv'.")
        except Exception as e:
            logger.error(f"Error saving final results: {e}")
    else:
        logger.warning("No data fetched.")

if __name__ == "__main__":
    fetch_data_for_ids()