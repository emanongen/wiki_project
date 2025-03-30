import deepl
import logging
import pandas as pd
import os

# Load the data from csv file
data = pd.read_csv('unique_occupations.csv')

# Define the DeepL API key
DEEPL_API_KEY = "e9a08eae-d904-4615-8677-442ab5e31359"

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Initialize the DeepL Translator
translator = deepl.Translator(DEEPL_API_KEY)

def translate_text(text, source_lang="DE", target_lang="EN-US"):
    """Translates text from source_lang to target_lang using DeepL API."""
    try:
        result = translator.translate_text(text, source_lang=source_lang, target_lang=target_lang)
        return result.text
    except Exception as e:
        logger.error(f"Error translating text: {e}")
        return text

# Check if the required column exists in the dataset
if 'occupation' not in data.columns:
    logger.error("Missing required column: 'occupation'")
    raise KeyError("Column 'occupation' is missing from the dataset.")

# Translate 'occupation' column
logger.info("Translating 'occupation' column.")
data['translated_occupation'] = data['occupation'].apply(
    lambda x: translate_text(x) if pd.notnull(x) else x
)

# Save the translated data to a new csv file
output_file = 'translated_unique_occupations.csv'
logger.info(f"Saving translated dataset to {output_file}.")
data.to_csv(output_file, index=False)
logger.info("Translation process completed.")