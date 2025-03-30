import os
import pandas as pd
import requests
import logging
import datetime
from time import sleep
import httpx
from dateutil.relativedelta import relativedelta
import calendar

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

HEADERS = {
    "Accept": "application/sparql-results+json",
    "User-Agent": "wiki_german_de/1.0 (soc.evgeniiatcoi@gmail.com)",
    "Accept-Encoding": "gzip,deflate"
}

def fetch_wikipedia_data(page_url):
    """Fetches Wikipedia total page views and description."""
    if not isinstance(page_url, str) or not page_url.strip():
        return 0, "No description available"
    
    wiki_code = "de" if "de.wikipedia.org" in page_url else "en" if "en.wikipedia.org" in page_url else None
    if not wiki_code:
        return 0, "No description available"
    
    article = page_url.split("/")[-1]
    description_url = f"https://{wiki_code}.wikipedia.org/api/rest_v1/page/summary/{article}"
    total_views = 0
    description = "No description available"

    # Fetch Description
    try:
        response = httpx.get(description_url, timeout=10)
        response.raise_for_status()
        summary_data = response.json()
        description = summary_data.get("extract", "No description available")
    except Exception as e:
        logger.error(f"Error fetching description for {article}: {e}")

    # Fetch Total Views (Summing Monthly Data)
    try:
        current_year = datetime.datetime.now().year
        start_date = "20150701"  # Earliest available Wikipedia pageview data (July 2015)
        end_date = f"{current_year}1231"  # End of the current year
        
        views_url = f"https://wikimedia.org/api/rest_v1/metrics/pageviews/per-article/{wiki_code}.wikipedia/all-access/all-agents/{article}/monthly/{start_date}/{end_date}"
        response = httpx.get(views_url, timeout=10)
        response.raise_for_status()
        data = response.json()

        total_views = sum(item["views"] for item in data.get("items", []))
    except Exception as e:
        logger.error(f"Error fetching pageviews for {article}: {e}")

    return total_views, description

def fetch_occupation_and_death(person_id):
    """Fetches occupation and date of death for a given person ID from Wikidata."""
    logger.info(f"Fetching data for person ID: {person_id}")
    query = f"""
        PREFIX wd: <http://www.wikidata.org/entity/>
        PREFIX wdt: <http://www.wikidata.org/prop/direct/>
        PREFIX rdfs: <http://www.w3.org/2000/01/rdf-schema#>
        SELECT ?occupationLabel ?dateOfDeath WHERE {{
            wd:{person_id} wdt:P106 ?occupation .
            ?occupation rdfs:label ?occupationLabel .
            OPTIONAL {{ wd:{person_id} wdt:P570 ?dateOfDeath. }}
            FILTER(LANG(?occupationLabel) = "de")
        }}
    """
    try:
        response = requests.get("https://query.wikidata.org/sparql", params={"query": query, "format": "json"}, headers=HEADERS)
        response.raise_for_status()
        results = response.json().get("results", {}).get("bindings", [])
        if not results:
            logger.warning(f"No data found for person ID: {person_id}")
            return "No occupation found", "No date of death"

        occupations = ", ".join([r["occupationLabel"]["value"] for r in results if "occupationLabel" in r])
        date_of_death = results[0].get("dateOfDeath", {}).get("value", "No date of death")
        return occupations if occupations else "No occupation found", date_of_death
    except Exception as e:
        logger.error(f"Error fetching data for {person_id}: {e}")
        return "No occupation found", "No date of death"

def enrich_data(input_csv, output_csv, checkpoint_csv="checkpoint.csv"):
    """Enriches CSV data with occupation, date of death, and Wikipedia views."""
    logger.info(f"Processing input CSV: {input_csv}")
    try:
        df = pd.read_csv(input_csv)
    except Exception as e:
        logger.error(f"Error reading input CSV: {e}")
        return

    enriched_data = []
    last_processed_index = -1

    if os.path.exists(checkpoint_csv):
        logger.info("Resuming from checkpoint.")
        try:
            checkpoint_df = pd.read_csv(checkpoint_csv)
            enriched_data = checkpoint_df.to_dict(orient="records")  # Load checkpoint data
            last_processed_index = len(checkpoint_df) - 1  # Infer last processed row
            logger.info(f"Resuming from row {last_processed_index + 1}.")
        except Exception as e:
            logger.error(f"Error reading checkpoint CSV: {e}")
            return
    else:
        last_processed_index = -1

    for idx, row in df.iterrows():
        if idx <= last_processed_index:
            continue  # Skip rows already processed

        person_id = row["person"].split("/")[-1]
        try:
            person_data = fetch_occupation_and_death(person_id)  # Fetch occupation and date of death
            german_total, german_desc = fetch_wikipedia_data(row["GermanWikipedia"])
            english_total, english_desc = fetch_wikipedia_data(row["EnglishWikipedia"])
        except Exception as e:
            logger.error(f"Error processing row for person ID {person_id}: {e}")
            continue

        enriched_row = row.to_dict()
        enriched_row.update({
            "occupation": person_data[0],
            "date_of_death": person_data[1],
            "german_total_views": german_total,
            "german_description": german_desc,
            "english_total_views": english_total,
            "english_description": english_desc
        })
        enriched_data.append(enriched_row)

        if len(enriched_data) % 100 == 0:
            try:
                pd.DataFrame(enriched_data).to_csv(checkpoint_csv, index=False)
                logger.info(f"Checkpoint saved at {len(enriched_data)} rows.")
            except Exception as e:
                logger.error(f"Error saving checkpoint: {e}")

        sleep(0.3)
    
    try:
        pd.DataFrame(enriched_data).to_csv(output_csv, index=False)
        logger.info(f"Enrichment complete. Data saved to {output_csv}")
    except Exception as e:
        logger.error(f"Error saving output CSV: {e}")
    
if __name__ == "__main__":
    enrich_data("final_combined_results.csv", "enriched_final_results.csv")