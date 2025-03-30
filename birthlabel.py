import logging
import pandas as pd
import requests
import time
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[logging.StreamHandler()]
)
logger = logging.getLogger(__name__)

# Constants
WIKIDATA_API_URL = "https://www.wikidata.org/w/api.php"
MAX_RETRIES = 5
REQUEST_DELAY = 1  # seconds between requests to avoid rate limiting
BATCH_SIZE = 50  # default batch size, will fall back to single requests if needed

def load_or_create_dataframe(input_csv, output_csv):
    """Load existing results or create new file if doesn't exist"""
    try:
        if Path(output_csv).exists():
            df = pd.read_csv(output_csv)
            logger.info(f"Loaded existing results from {output_csv}")
            return df
    except Exception as e:
        logger.warning(f"Error loading {output_csv}, creating new file: {e}")
    
    # Create new empty dataframe with expected columns
    return pd.DataFrame(columns=['birthplace_id', 'label'])

def get_single_label(entity_id, retries=MAX_RETRIES):
    """Fetch label for a single Wikidata entity with retries"""
    for attempt in range(retries):
        try:
            time.sleep(REQUEST_DELAY)  # Be gentle with the API
            logger.debug(f"Fetching label for {entity_id} (attempt {attempt + 1})")
            
            response = requests.get(
                WIKIDATA_API_URL,
                params={
                    'action': 'wbgetentities',
                    'ids': entity_id,
                    'format': 'json',
                    'languages': 'en',
                    'props': 'labels'
                },
                timeout=10
            )
            response.raise_for_status()
            data = response.json()

            entity_data = data.get('entities', {}).get(entity_id, {})
            if 'labels' in entity_data and 'en' in entity_data['labels']:
                return entity_data['labels']['en']['value']
            return None

        except requests.exceptions.RequestException as e:
            logger.warning(f"Attempt {attempt + 1} failed for {entity_id}: {str(e)}")
            if attempt == retries - 1:
                logger.error(f"Failed to fetch {entity_id} after {retries} attempts")
                return None
            time.sleep(2 ** attempt)  # Exponential backoff

    return None

def get_batch_labels(entity_ids, retries=MAX_RETRIES):
    """Fetch labels for a batch of IDs with fallback to single requests"""
    # First try batch request
    batch_result = {}
    missing_ids = []
    
    try:
        time.sleep(REQUEST_DELAY)
        logger.info(f"Attempting batch request for {len(entity_ids)} items")
        
        response = requests.get(
            WIKIDATA_API_URL,
            params={
                'action': 'wbgetentities',
                'ids': '|'.join(entity_ids),
                'format': 'json',
                'languages': 'en',
                'props': 'labels'
            },
            timeout=20  # Longer timeout for batches
        )
        response.raise_for_status()
        data = response.json()

        for entity_id in entity_ids:
            entity_data = data.get('entities', {}).get(entity_id, {})
            if 'labels' in entity_data and 'en' in entity_data['labels']:
                batch_result[entity_id] = entity_data['labels']['en']['value']
            else:
                batch_result[entity_id] = None
                missing_ids.append(entity_id)
                
        logger.info(f"Batch succeeded with {len(entity_ids) - len(missing_ids)} labels found")
        
    except Exception as e:
        logger.warning(f"Batch request failed: {str(e)}")
        missing_ids = entity_ids  # If batch fails, try all individually
        batch_result = {id: None for id in entity_ids}
    
    # Process missing IDs individually
    if missing_ids:
        logger.info(f"Processing {len(missing_ids)} items individually")
        for entity_id in missing_ids:
            label = get_single_label(entity_id)
            batch_result[entity_id] = label
    
    return batch_result

def process_all_birthplaces(dataset_path, output_csv):
    """Main function to process all birthplaces directly from the dataset"""
    # Load existing results
    results_df = load_or_create_dataframe(None, output_csv)
    processed_ids = set(results_df['birthplace_id'].astype(str))
    
    # Load dataset and extract birthplace URLs
    df = pd.read_csv(dataset_path)
    df['birthplace_id'] = df['birthplace'].apply(
        lambda x: x.split('/')[-1] if isinstance(x, str) else None
    )
    all_ids = df['birthplace_id'].dropna().unique().tolist()
    
    # Determine which IDs need processing
    ids_to_process = [id for id in all_ids if str(id) not in processed_ids]
    logger.info(f"IDs to process: {ids_to_process}")
    logger.info(f"Total IDs to process: {len(ids_to_process)} (already have {len(processed_ids)})")
    
    # Process in batches with progress saving
    for i in range(0, len(ids_to_process), BATCH_SIZE):
        batch = ids_to_process[i:i + BATCH_SIZE]
        logger.info(f"Processing batch {i//BATCH_SIZE + 1} (IDs {i} to {i + len(batch) - 1})")
        
        try:
            batch_labels = get_batch_labels(batch)
            
            # Create dataframe for this batch's results
            batch_df = pd.DataFrame({
                'birthplace_id': batch,
                'label': [batch_labels.get(id, None) for id in batch]
            })
            
            # Append to results and save
            results_df = pd.concat([results_df, batch_df], ignore_index=True)
            results_df.to_csv(output_csv, index=False)
            logger.info(f"Saved results for batch {i//BATCH_SIZE + 1}")
            
        except Exception as e:
            logger.error(f"Fatal error processing batch {i//BATCH_SIZE + 1}: {str(e)}")
            logger.info("Attempting to process items individually...")
            
            # Fall back to individual processing
            for entity_id in batch:
                try:
                    label = get_single_label(entity_id)
                    new_row = pd.DataFrame({
                        'birthplace_id': [entity_id],
                        'label': [label]
                    })
                    results_df = pd.concat([results_df, new_row], ignore_index=True)
                    results_df.to_csv(output_csv, index=False)
                except Exception as single_e:
                    logger.error(f"Failed to process {entity_id}: {str(single_e)}")
                    # Store as None to indicate failure
                    new_row = pd.DataFrame({
                        'birthplace_id': [entity_id],
                        'label': [None]
                    })
                    results_df = pd.concat([results_df, new_row], ignore_index=True)
                    results_df.to_csv(output_csv, index=False)
    
    logger.info(f"Processing complete. Results saved to {output_csv}")
    return results_df

def merge_labels_to_dataset(dataset_path, labels_path, output_path):
    """Merge the fetched labels back into the original dataset"""
    logger.info("Merging labels with original dataset")
    
    # Load datasets
    df = pd.read_csv(dataset_path)
    labels_df = pd.read_csv(labels_path)
    
    # Extract birthplace IDs from original dataset
    df['birthplace_id'] = df['birthplace'].apply(
        lambda x: x.split('/')[-1] if isinstance(x, str) else None
    )
    
    # Merge labels
    merged_df = df.merge(labels_df, on='birthplace_id', how='left')
    
    # Save result
    merged_df.to_csv(output_path, index=False)
    logger.info(f"Merged dataset saved to {output_path}")
    return merged_df

if __name__ == "__main__":
    # Step 1: Process all birthplaces directly from the final dataset
    dataset_path = 'final_dataset.csv'
    output_labels_path = 'birthplace_labels.csv'
    output_merged_path = 'final_dataset_with_labels.csv'

    # Step 2: Process all birthplace IDs to get labels
    process_all_birthplaces(dataset_path, output_labels_path)

    # Step 3: Merge the labels back into the original dataset
    merge_labels_to_dataset(
        dataset_path,
        output_labels_path,
        output_merged_path
    )