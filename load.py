import duckdb
import os
import logging
import requests
import time
import random

logging.basicConfig(
    level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
    filename='load.log'
)
logger = logging.getLogger(__name__)


DB_FILE = "traffic.duckdb"
DOWNLOAD_DIR = "temp_downloads" # Directory to temporarily store files


### Added this because I was getting request errors when downloading straight from the url
def download_file_with_retries(url, dest_folder, retries=15, delay=random.randint(4,15)):
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
    }
    
    if not os.path.exists(dest_folder):
        os.makedirs(dest_folder)
        
    local_filename = os.path.join(dest_folder, url.split('/')[-1])

    for attempt in range(retries):
        try:
            logger.info(f"  Attempt {attempt + 1} to download {url}")
            with requests.get(url, headers=headers, stream=True) as r:
                r.raise_for_status() # This will raise an error for bad responses (4xx or 5xx)
                with open(local_filename, 'wb') as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        f.write(chunk)
            logger.info(f"  -> Successfully downloaded to {local_filename}")
            return local_filename
        except requests.exceptions.RequestException as e:
            logger.warning(f"  -> Download attempt {attempt + 1} failed: {e}")
            if attempt < retries - 1:
                # Wait before the next attempt
                time.sleep(delay)
            else:
                logger.error(f"  -> All download attempts for {url} failed.")
                return None

def process_data_for_color(con, color, years, months):

    logger.info(f"--- Starting processing for {color} taxi data ---")
    
    con.execute(f"DROP TABLE IF EXISTS {color};")
    logger.info(f"Dropped table '{color}' if it existed.")

    first_file_loaded = False
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"

    for year in years:
        for month in months:
            url = f"{base_url}/{color}_tripdata_{year}-{month}.parquet"
            logger.info(f"Processing {color} data for {year}-{month} from {url}")

            local_file_path = download_file_with_retries(url, DOWNLOAD_DIR)

            if local_file_path is None:
                logger.warning(f"-> Skipping file for {year}-{month} due to download failure.")
                continue
            
            try:
                if not first_file_loaded:
                    sql = f"CREATE TABLE {color} AS SELECT * FROM read_parquet('{local_file_path}');"
                    logger.info(f"-> Creating table '{color}' with data from {year}-{month}.")
                    first_file_loaded = True
                else:
                    sql = f"INSERT INTO {color} SELECT * FROM read_parquet('{local_file_path}');"
                
                con.execute(sql)
                logger.info(f"-> Successfully loaded {month}-{year} into '{color}' table.")

            except Exception as e:
                logger.error(f"-> DuckDB error while processing {local_file_path}: {e}")
            finally:
                if os.path.exists(local_file_path):
                    os.remove(local_file_path)
                    logger.info(f"Removed local file for {color}: {year}-{month}")

    logger.info(f"Finished processing for {color} taxi data")

def load_parquet_files():
    con = None
    try:
        con = duckdb.connect(database=DB_FILE, read_only=False)
        logger.info(f"Connected to DuckDB database: '{DB_FILE}'")

        years = [2015, 2016, 2017, 2018, 2019, 2020, 2021, 2022, 2023, 2024, 2025]
        months = [f"{m:02d}" for m in range(1, 13)] # Generates ['01', '02', ..., '12']

        process_data_for_color(con, 'yellow', years, months)
        process_data_for_color(con, 'green', years, months)
        os.remove(DOWNLOAD_DIR)

    except Exception as e:
        logger.critical(f"A critical error occurred in the main script: {e}")
    finally:
        if con:
            con.close()
            logger.info("DuckDB connection closed.")


if __name__ == "__main__":
    load_parquet_files()