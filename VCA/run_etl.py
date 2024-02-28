"""
This script performs Extract, Transform, Load (ETL) operations by scraping data from the VCA website, cleaning it, exporting it to a JSON file, and uploading it to a MongoDB database.

It reads configuration settings from a TOML file ('config.toml') to set up runtime variables such as MongoDB credentials, export path for scraped data, and log path for storing log files.

Logging is configured to capture informational and error messages during script execution. Informational messages include details about scraping data from the VCA website, exporting data to a file, cleaning the data, and inserting data into the MongoDB database. Error messages are logged in case of exceptions during scraping, cleaning, or database operations.

Overview:
    - The script scrapes data about dog breeds from the VCA website, extracts relevant information, cleans the data, and loads it into a MongoDB database.
    - Configuration settings are read from 'config.toml' to customize the behavior of the script.
    - Logging is utilized to track the progress of the ETL process and capture any errors that may occur.

"""

import json
import toml
import vca
import os
import logging
import duckdb
import sys
from datetime import datetime
from pymongo.mongo_client import MongoClient
from pymongo.server_api import ServerApi

# Load runtime variables from configuration file
with open('.\\config.toml', 'r') as f:
    config = toml.load(f)
username = config['mongodb']['username']
password = config['mongodb']['password']
cluster = config['mongodb']['cluster']
app_name = config['mongodb']['app_name']
export_path = config['scraper']['export_path']
log_path = config['log']['log_path']

# Set up logging configuration
logging.basicConfig(filename=os.path.join(log_path, 'etl.log'), filemode='w', format='%(asctime)s | %(levelname)s | %(message)s')
log = logging.getLogger()
log.setLevel(logging.INFO)

def get_data():
    """
    Scrapes data from the VCA website and exports it to a JSON file.

    This function initializes a VCA scraper object, fetches breed information and details from the VCA website, and dumps the scraped data into a JSON file.

    Returns:
        None
    """
    try:
        log.info('Scraping data from VCA website...')
        # Initialize VCA Scraper
        scraper = vca.VCAScrape(export_path)
        scraper.get_breeds()
        scraper.get_details()

        log.info('Exporting data to file...')
        # Dump data to JSON file
        with open(os.path.join(scraper.export_path, 'vca_detail.json'), 'w') as f:
            json.dump(scraper.details, f)
        log.info(f'Data exported to: {export_path}')
    except Exception as e:
        log.error(f'Error scraping data from VCA: {e}')
        sys.exit()

def clean_data():
    """
    Cleans the scraped data.

    This function reads the scraped data from the JSON file, performs cleaning operations using a SQL script, and exports the cleaned data to a new JSON file.

    Returns:
        None
    """
    try:
        log.info('Cleaning data...')
        # Read JSON data into a DuckDB DataFrame
        vca = duckdb.read_json('.\\vca_detail.json')
        # Read SQL script for cleaning operations
        with open(os.path.join(export_path, 'clean_data.sql'), 'r') as f:
            clean_sql = f.read()
        # Apply cleaning operations
        vca = duckdb.sql(clean_sql).to_df()
        # Export cleaned data to JSON file
        vca.to_json('vca_cleaned.json', orient='records', index=False)
        log.info('Data cleaning completed.')
    except Exception as e:
        log.error(f'Error cleaning data: {e}')
        sys.exit()

def upload_data():
    """
    Uploads data from a JSON file to a MongoDB database.

    This function reads data from a JSON file ('vca_detail.json'), connects to a MongoDB database using
    credentials provided in the configuration, drops an existing collection named 'vca' in the 'dogs_nlp' database,
    inserts the data into the 'vca' collection, creates an index on the 'breed' field, and prints the total number of
    records inserted into the collection.

    Returns:
        None
    """
    try:
        log.info('Setting up database connection...')
        # Create MongoDB connection URI
        uri = f"mongodb+srv://{username}:{password}@{cluster}/?retryWrites=true&w=majority&appName={app_name}"
        with MongoClient(uri, server_api=ServerApi('1')) as client:
            # Connect to MongoDB and drop existing collection
            db = client.dogs_nlp
            log.info('Dropping existing collection...')
            db.vca.drop()

            # Read data from JSON file
            log.info('Preparing scraped data for insertion...')
            with open(os.path.join(export_path, 'vca_cleaned.json'), 'r') as f:
                raw_data = json.load(f)

            # Insert data into 'vca' collection
            log.info('Inserting data into database...')
            db.vca.insert_many(raw_data)
            log.info('Creating index for breed...')
            db.vca.create_index('breed')
            total = db.vca.count_documents({})
            log.info(f"Total records in collection: {total}")
    except Exception as e:
        log.error(f'Error inserting data to MongoDB: {e}')
        sys.exit()


if __name__ == "__main__":
    get_data()
    clean_data()
    upload_data()