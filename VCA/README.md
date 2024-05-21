# VCA Data ETL Script

## Overview

This script performs Extract, Transform, Load (ETL) operations by scraping data from the VCA website, cleaning it, exporting it to a JSON file, and uploading it to a MongoDB database.

## Installation

1. Install the required dependencies:

    ```bash
    pip install -r requirements.txt
    ```

2. Configure the runtime variables:

    - Create a `config.toml` file in the root directory with the following structure:

        ```toml
        [mongodb]
        username = "your_mongodb_username"
        password = "your_mongodb_password"
        cluster = "your_mongodb_cluster"
        app_name = "your_app_name"

        [scraper]
        export_path = "path_to_export_data"
        
        [log]
        log_path = "path_to_log_directory"
        ```

        Replace placeholders with your MongoDB credentials, export path for scraped data, and log path.

## Usage

Run the script using the following command:

```bash
python etl_script.py
```

## Functionality

- **get_data()**: Scrapes data from the VCA website and exports it to a JSON file named `vca_detail.json`.
  
- **clean_data()**: Cleans the scraped data. Reads the scraped data from the JSON file, performs cleaning operations using a SQL script, and exports the cleaned data to a new JSON file named `vca_cleaned.json`.

- **upload_data()**: Uploads data from `vca_cleaned.json` to a MongoDB database. Drops an existing collection named `vca` in the `dogs_nlp` database, inserts the data into the `vca` collection, creates an index on the `breed` field, and prints the total number of records inserted into the collection.

## Logging

The script logs informational and error messages to a log file named `etl.log` located in the specified log directory. Informational messages include details about scraping data from the VCA website, cleaning the data, exporting data to a file, and inserting data into the MongoDB database. Error messages are logged in case of exceptions during scraping, cleaning, or database operations.

## VCA Module

The `vca` module provides functionality for scraping data from the VCA website. It includes the following classes and methods:

- **VCAScrape**: A class for scraping data from the VCA website. It has the following methods:
    - `get_breeds()`: Fetches breed information from the VCA website.
    - `get_details()`: Fetches detailed information about each breed.
    - `_breed_info(link)`: Private method to retrieve information about a specific breed.
    - `_breed_details(soup)`: Private method to extract details about a breed from HTML soup.
    - `_breed_stats(soup)`: Private method to extract statistics about a breed from HTML soup.
    - `_breed_traits(soup)`: Private method to extract traits of a breed from HTML soup.

## Contributing

Contributions are welcome! If you find any issues or have suggestions for improvements, please open an issue or create a pull request.

## License

This project is licensed under the [MIT License](LICENSE).