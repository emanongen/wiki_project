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
BATCH_SIZE = 500
MAX_RETRIES = 10
RETRY_DELAY = 20
TIMEOUT = 300
SAVE_EVERY_N_BATCHES = 20
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
            time.sleep((attempt + 1) * 10)  # Exponential backoff

    logger.error("Max retries reached. Unable to fetch data.")
    return None

# Function to save the birthdate pointer
def save_pointer(pointer, filename="pointer.txt"):
    with open(filename, "w") as f:
        f.write(pointer)
    logger.info(f"Pointer saved: {pointer}")

# Function to load the birthdate pointer
def load_pointer(filename="pointer.txt"):
    if os.path.exists(filename):
        with open(filename, "r") as f:
            pointer = f.read().strip()
        logger.info(f"Resuming from pointer: {pointer}")
        return pointer
    return None

# Function to save intermediate results
def save_intermediate_results(results, filename_prefix="intermediate_results"):
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{filename_prefix}_{timestamp}.csv"
    pd.concat(results, ignore_index=True).to_csv(filename, index=False)
    logger.info(f"Saved intermediate results to {filename}.")

# Function to process and clean the DataFrame
def process_results(df):
    """
    Extracts the 'value' field from dictionary-like columns in the DataFrame.
    """
    for col in df.columns:
        df[col] = df[col].apply(lambda x: x.get("value", None) if isinstance(x, dict) else x)
    return df

# Main function to fetch data
def fetch_data():
    all_results = []
    batch_count = 0

    for start_year in range(1525, 2026, 20):  # Iterate through 20-year periods
        end_year = start_year + 19
        logger.info(f"Fetching data for birth years {start_year} to {end_year}...")

        last_pointer = load_pointer()  # Resume from last pointer (birthdate and person)
        last_birthdate, last_person = (None, None) if not last_pointer else last_pointer.split("|")

        while True:
            query = f"""
                PREFIX schema: <http://schema.org/>
                PREFIX wd: <http://www.wikidata.org/entity/>
                PREFIX wdt: <http://www.wikidata.org/prop/direct/>
                PREFIX wikibase: <http://wikiba.se/ontology#>
                PREFIX bd: <http://www.bigdata.com/rdf#>
                PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>

                SELECT ?person ?personLabel ?birthdate ?birthYear ?genderLabel 
                       (COALESCE(?germanPageUrl, "No") AS ?GermanWikipedia)
                       (COALESCE(?englishPageUrl, "No") AS ?EnglishWikipedia)
                WHERE {{
                    ?person wdt:P27 wd:Q183.  # People from Germany
                    ?person wdt:P569 ?birthdate.  # Ensure the person has a birthdate
                    ?person wdt:P21 ?gender .  # Ensure the person has a gender
                    ?gender rdfs:label ?genderLabel .
                    FILTER(LANG(?genderLabel) = "de")

                    # Birth year filter
                    FILTER(YEAR(?birthdate) >= {start_year} && YEAR(?birthdate) <= {end_year})
                    BIND(YEAR(?birthdate) AS ?birthYear)

                    # Ensure the person has a German Wikipedia page (mandatory)
                    ?germanPage schema:about ?person ;
                                schema:inLanguage "de" ;
                                schema:isPartOf <https://de.wikipedia.org/> .
                    BIND(STR(?germanPage) AS ?germanPageUrl)

                    # Check if the person has an English Wikipedia page
                    OPTIONAL {{
                        ?englishPage schema:about ?person ;
                                     schema:inLanguage "en" ;
                                     schema:isPartOf <https://en.wikipedia.org/> .
                        BIND(STR(?englishPage) AS ?englishPageUrl)
                    }}

                    SERVICE wikibase:label {{
                        bd:serviceParam wikibase:language "de".
                        ?person rdfs:label ?personLabel .
                    }}

                    # Pagination using last_birthdate and last_person
                    {f'FILTER((?birthdate > "{last_birthdate}"^^xsd:dateTime) || (?birthdate = "{last_birthdate}"^^xsd:dateTime && STR(?person) > "{last_person}"))' if last_birthdate and last_person else ""}
                }}
                ORDER BY ?birthdate ?person
                LIMIT {BATCH_SIZE}
            """

            batch_results = query_wikidata(query)

            if batch_results is None:  # Query failed after max retries
                logger.error("Too many failures. Exiting...")
                return

            if batch_results.empty:  # No more data for the current year range
                logger.info(f"No more results for {start_year}-{end_year}. Moving to next range.")
                break  # Move to next year range

            # Process and clean the results
            batch_results = process_results(batch_results)

            # Extract last birthdate and person for pagination
            try:
                last_birthdate = batch_results["birthdate"].iloc[-1]
                last_person = batch_results["person"].iloc[-1]
                save_pointer(f"{last_birthdate}|{last_person}")  # Save progress after successful pointer update
                logger.info(f"Pointer updated to: {last_birthdate} | {last_person}")
            except Exception as e:
                logger.error(f"Error extracting pointer: {e}")
                break

            all_results.append(batch_results)
            batch_count += 1
            logger.info(f"Fetched {len(batch_results)} records in this batch...")

            # Save intermediate results periodically
            if batch_count % SAVE_EVERY_N_BATCHES == 0:
                save_intermediate_results(all_results)
                all_results = []  # Clear memory

            # If fewer results than BATCH_SIZE, we've reached the end of the data for this year range
            if len(batch_results) < BATCH_SIZE:
                logger.info(f"Reached the end of data for {start_year}-{end_year}. Moving to next range.")
                break

    # Save final results
    if all_results:
        final_df = pd.concat(all_results, ignore_index=True)
        final_df.to_csv("german_wikipedia_notables_de.csv", index=False)
        logger.info("Final data saved to 'german_wikipedia_notables_de.csv'.")
    else:
        logger.warning("No data fetched.")

if __name__ == "__main__":
    fetch_data()