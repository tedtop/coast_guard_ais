#!/usr/bin/env python3
"""
Script to download AIS data from NOAA, convert it to parquet files, and optionally upload to S3.
The parquet files are organized in a hierarchical directory structure:
year=YYYY/month=MM/day=DD/hour=HH/AIS_YYYY_MM_DD_processed_hourHH.parquet
"""

import os
import sys
import time
import shutil
import logging
import re
import concurrent.futures
from datetime import datetime
from pathlib import Path
from typing import List, Dict, Optional, Tuple

import requests
from bs4 import BeautifulSoup
import pandas as pd
import numpy as np
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import boto3
import zipfile

# Set up logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("zip2parquet")

# =============================================================================
# CONFIGURATION - Edit these values as needed
# =============================================================================

# Basic Configuration
BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024"
TMP_DIR = Path("tmp")
OUTPUT_DIR = Path(".")
CHUNK_SIZE = 500_000  # Number of rows to process at a time

# CSV Column Names and Data Types (based on the data dictionary)
CSV_COLUMNS = [
    "MMSI", "BaseDateTime", "LAT", "LON", "SOG", "COG", "Heading",
    "VesselName", "IMO", "CallSign", "VesselType", "Status",
    "Length", "Width", "Draft", "Cargo", "TransceiverClass"
]

# Parallel Processing
MAX_WORKERS = 4  # Number of parallel threads for processing

# S3 Configuration
ENABLE_S3_UPLOAD = False  # Set to True to enable S3 upload
S3_REGION = 'sfo3'
S3_ENDPOINT = 'https://ais-bucket.sfo3.digitaloceanspaces.com'
S3_ACCESS_KEY = 'ACCESS_KEY'
S3_SECRET_KEY = 'SECRET_KEY'
S3_BUCKET_NAME = 'ais-data'

# Create required directories
TMP_DIR.mkdir(exist_ok=True)


class AISDataProcessor:
    """Class for processing AIS data from NOAA"""

    def __init__(self):
        """Initialize the processor"""
        # Initialize S3 client if uploads are enabled
        self.s3_client = None
        if ENABLE_S3_UPLOAD:
            session = boto3.session.Session()
            self.s3_client = session.client(
                's3',
                region_name=S3_REGION,
                endpoint_url=S3_ENDPOINT,
                aws_access_key_id=S3_ACCESS_KEY,
                aws_secret_access_key=S3_SECRET_KEY
            )
            logger.info(f"Initialized S3 client with endpoint {S3_ENDPOINT}, bucket {S3_BUCKET_NAME}")

    def get_zip_urls(self) -> List[str]:
        """
        Extract all ZIP file URLs from the NOAA AIS data handler index page

        Returns:
            List of URLs to ZIP files
        """
        logger.info(f"Fetching ZIP URLs from {BASE_URL}")
        response = requests.get(BASE_URL)
        response.raise_for_status()

        soup = BeautifulSoup(response.text, 'html.parser')
        urls = []

        for link in soup.find_all('a', href=re.compile(r'\.zip$')):
            href = link.get('href')
            # Handle relative URLs properly
            if href.startswith('http'):
                url = href  # It's already an absolute URL
            elif href.startswith('/'):
                # Extract the base domain and protocol
                base_domain = '/'.join(BASE_URL.split('/')[:3])  # Gets "https://coast.noaa.gov"
                url = base_domain + href
            else:
                # It's a relative URL, join with base URL
                url = BASE_URL + ('/' if not BASE_URL.endswith('/') else '') + href

            urls.append(url)
            logger.debug(f"Found URL: {url}")

        logger.info(f"Found {len(urls)} ZIP files")
        return urls

    def download_file(self, url: str, dest_path: Path) -> Path:
        """
        Download a file from a URL with progress reporting

        Args:
            url: URL to download
            dest_path: Path to save the file to

        Returns:
            Path to the downloaded file
        """
        logger.info(f"Downloading {url} to {dest_path}")

        # Create parent directory if it doesn't exist
        dest_path.parent.mkdir(parents=True, exist_ok=True)

        # Stream download with progress reporting
        response = requests.get(url, stream=True)
        response.raise_for_status()

        total_size = int(response.headers.get('content-length', 0))
        block_size = 1024  # 1 KB
        progress_bar = tqdm(total=total_size, unit='iB', unit_scale=True, desc=dest_path.name)

        with open(dest_path, 'wb') as f:
            for data in response.iter_content(block_size):
                progress_bar.update(len(data))
                f.write(data)

        progress_bar.close()

        if total_size != 0 and progress_bar.n != total_size:
            logger.warning(f"Download incomplete for {url}. Expected {total_size} bytes, got {progress_bar.n} bytes.")

        return dest_path

    def extract_zip(self, zip_path: Path) -> Path:
        """
        Extract a ZIP file

        Args:
            zip_path: Path to the ZIP file

        Returns:
            Path to the extracted CSV file
        """
        logger.info(f"Extracting {zip_path}")

        csv_path = None
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file in zip_ref.namelist():
                if file.endswith('.csv'):
                    csv_path = TMP_DIR / file
                    zip_ref.extract(file, TMP_DIR)
                    logger.info(f"Extracted {file} to {csv_path}")

        if not csv_path:
            raise ValueError(f"No CSV file found in {zip_path}")

        return csv_path

    def process_csv_chunk(self, csv_path: Path) -> None:
        """
        Process a CSV file in chunks and convert to Parquet files

        Args:
            csv_path: Path to the CSV file
        """
        logger.info(f"Processing {csv_path} in chunks of {CHUNK_SIZE:,} rows")

        # Define column types based on the data dictionary
        dtypes = {
            'MMSI': str,                 # Text (9)
            'LAT': float,                # Double (8)
            'LON': float,                # Double (8)
            'SOG': float,                # Float (4)
            'COG': float,                # Float (4)
            'Heading': float,            # Float (4)
            'VesselName': str,           # Text (32)
            'IMO': str,                  # Text (7)
            'CallSign': str,             # Text (8)
            'VesselType': 'Int32',       # Integer short
            'Status': 'Int32',           # Integer short
            'Length': float,             # Float (4)
            'Width': float,              # Float (4)
            'Draft': float,              # Float (4)
            'Cargo': str,                # Text (4)
            'TransceiverClass': str      # Text (2)
        }

        # Read BaseDateTime column as object and convert to datetime after reading
        # Instead of using deprecated date_parser parameter
        csv_reader = pd.read_csv(
            csv_path,
            chunksize=CHUNK_SIZE,
            dtype={**dtypes, 'BaseDateTime': str},  # Read datetime as string initially
            low_memory=False
        )

        # Create a dictionary to store dataframes by hour key
        hour_data = {}

        # Process each chunk
        total_rows = 0

        for i, chunk in enumerate(csv_reader):
            chunk_size = len(chunk)
            total_rows += chunk_size
            logger.info(f"Processing chunk {i+1} ({chunk_size:,} rows), Total: {total_rows:,} rows")

            # Convert BaseDateTime from string to datetime
            chunk['BaseDateTime'] = pd.to_datetime(chunk['BaseDateTime'], format='%Y-%m-%dT%H:%M:%S')

            # Group by hour
            chunk['year'] = chunk['BaseDateTime'].dt.year
            chunk['month'] = chunk['BaseDateTime'].dt.month
            chunk['day'] = chunk['BaseDateTime'].dt.day
            chunk['hour'] = chunk['BaseDateTime'].dt.hour

            # Process each hour group
            grouped = chunk.groupby(['year', 'month', 'day', 'hour'])

            # Append data to the corresponding hour key
            for (year, month, day, hour), group in grouped:
                # Create a key for this hour
                key = (int(year), int(month), int(day), int(hour))

                # Drop year, month, day, hour columns to save memory
                data_to_append = group.drop(columns=['year', 'month', 'day', 'hour'])

                # If this hour already has data, append to it, otherwise create new entry
                if key in hour_data:
                    hour_data[key] = pd.concat([hour_data[key], data_to_append])
                    logger.debug(f"Appended {len(data_to_append):,} rows to existing data for {key}, total now: {len(hour_data[key]):,}")
                else:
                    hour_data[key] = data_to_append
                    logger.debug(f"Created new data for {key} with {len(data_to_append):,} rows")

        # After processing all chunks, write the aggregated data to parquet files
        logger.info(f"Writing {len(hour_data)} parquet files with a total of {total_rows:,} rows")

        with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            futures = []

            for (year, month, day, hour), data in hour_data.items():
                # Create the output directory structure
                output_dir = OUTPUT_DIR / f"year={year}" / f"month={month:02d}" / f"day={day:02d}" / f"hour={hour:02d}"
                output_dir.mkdir(parents=True, exist_ok=True)

                # Output file path
                output_file = output_dir / f"AIS_{year}_{month:02d}_{day:02d}_processed_hour{hour:02d}.parquet"

                logger.info(f"Will save {len(data):,} rows to {output_file}")

                # Submit the task to the executor
                future = executor.submit(
                    self.save_to_parquet_and_upload,
                    data,
                    output_file,
                    year,
                    month,
                    day,
                    hour
                )
                futures.append(future)

            # Wait for all tasks to complete
            for future in concurrent.futures.as_completed(futures):
                try:
                    future.result()
                except Exception as e:
                    logger.error(f"Error processing chunk: {e}")

    def save_to_parquet_and_upload(
        self,
        df: pd.DataFrame,
        output_file: Path,
        year: int,
        month: int,
        day: int,
        hour: int
    ) -> None:
        """
        Save DataFrame to Parquet file and optionally upload to S3

        Args:
            df: DataFrame to save
            output_file: Path to save the Parquet file to
            year, month, day, hour: Time components for partitioning
        """
        # Check if the file already exists
        if output_file.exists():
            logger.info(f"File {output_file} already exists. Checking if we need to append data...")

            try:
                # Read existing data
                existing_df = pd.read_parquet(output_file)
                logger.info(f"Read {len(existing_df):,} existing rows from {output_file}")

                # Concatenate with new data
                combined_df = pd.concat([existing_df, df], ignore_index=True)
                logger.info(f"Combined with {len(df):,} new rows, total: {len(combined_df):,} rows")

                # Use the combined DataFrame
                df = combined_df
            except Exception as e:
                logger.error(f"Error reading existing file {output_file}: {e}")
                logger.info(f"Will overwrite the file with new data")

        # Create the table with partition columns as metadata
        table = pa.Table.from_pandas(df)

        # Write the Parquet file with partition information as metadata
        # Updated to use newer Parquet version 2.6 instead of deprecated 2.0
        pq.write_table(
            table,
            output_file,
            compression='snappy',
            use_dictionary=True,
            version='2.6',  # Updated from '2.0' to '2.6'
            data_page_size=1048576,  # 1 MB pages
            write_statistics=True
        )

        logger.info(f"Saved {len(df):,} rows to {output_file}")

        # Upload to S3 if enabled
        if ENABLE_S3_UPLOAD and self.s3_client:
            s3_key = str(output_file.relative_to(OUTPUT_DIR))
            logger.info(f"Uploading {output_file} to s3://{S3_BUCKET_NAME}/{s3_key}")

            try:
                self.s3_client.upload_file(
                    str(output_file),
                    S3_BUCKET_NAME,
                    s3_key
                )
                logger.info(f"Upload complete: s3://{S3_BUCKET_NAME}/{s3_key}")

                # Verify the upload was successful by checking if the file exists in S3
                try:
                    self.s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
                    logger.info(f"Verified file exists in S3: s3://{S3_BUCKET_NAME}/{s3_key}")

                    # Only delete the local file after successful upload and verification
                    output_file.unlink()
                    logger.info(f"Deleted local file {output_file}")
                except Exception as e:
                    logger.error(f"Upload verification failed, keeping local file: {e}")
            except Exception as e:
                logger.error(f"Error uploading to S3: {e}")

    def process_zip_file(self, url: str) -> None:
        """
        Download, extract, and process a single ZIP file

        Args:
            url: URL to the ZIP file
        """
        try:
            # Extract filename from URL
            filename = url.split('/')[-1]
            zip_path = TMP_DIR / filename

            # Download the file
            self.download_file(url, zip_path)

            # Extract the ZIP file
            csv_path = self.extract_zip(zip_path)

            # Process the CSV file
            self.process_csv_chunk(csv_path)

            # Clean up the temporary files
            os.remove(zip_path)
            logger.info(f"Deleted {zip_path}")

            os.remove(csv_path)
            logger.info(f"Deleted {csv_path}")

        except Exception as e:
            logger.error(f"Error processing {url}: {e}")

    def run(self) -> None:
        """
        Run the entire process
        """
        # Get the list of ZIP URLs
        urls = self.get_zip_urls()

        # Process each ZIP file
        for i, url in enumerate(urls):
            logger.info(f"Processing file {i+1}/{len(urls)}: {url}")
            self.process_zip_file(url)

        logger.info("Processing complete!")


def main():
    """Main function"""
    processor = AISDataProcessor()
    processor.run()


if __name__ == "__main__":
    main()