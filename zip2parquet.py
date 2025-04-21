#!/usr/bin/env python3
"""
Script to download AIS data from NOAA, convert it to parquet files, and optionally upload to S3.
The parquet files are organized in a hierarchical directory structure:
year=YYYY/month=MM/day=DD/hour=HH/AIS_YYYY_MM_DD_processed_hourHH.parquet
"""

import os
import sys
import logging
import re
from datetime import datetime
from pathlib import Path
import zipfile

import requests
from bs4 import BeautifulSoup
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from tqdm import tqdm
import boto3
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

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

# S3 Configuration
ENABLE_S3_UPLOAD = False  # Set to True to enable S3 upload
S3_REGION = os.getenv('S3_REGION', 'sfo3')
S3_ENDPOINT = os.getenv('S3_ENDPOINT', 'https://sfo3.digitaloceanspaces.com')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY', 'ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY', 'SECRET_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME', 'noaa-ais-data')

# Create required directories
TMP_DIR.mkdir(exist_ok=True)

def get_zip_urls():
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

    logger.info(f"Found {len(urls)} ZIP files")
    return urls

def download_file(url, dest_path):
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

def extract_zip(zip_path):
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

def process_csv(csv_path):
    """
    Process a CSV file line by line and write to Parquet files

    Args:
        csv_path: Path to the CSV file
    """
    logger.info(f"Processing {csv_path}")

    # Dictionary to keep track of rows per hour
    hour_counts = {}

    # Dictionary to store dataframes by hour
    hour_data = {}

    # Define column types based on the data dictionary
    dtypes = {
        'MMSI': str,                 # Text (9)
        'BaseDateTime': str,         # Read as string, will convert to datetime
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

    # Read the entire CSV file
    logger.info(f"Reading {csv_path}")
    df = pd.read_csv(csv_path, dtype=dtypes)
    total_rows = len(df)
    logger.info(f"Read {total_rows:,} rows from {csv_path}")

    # Convert BaseDateTime from string to datetime
    df['BaseDateTime'] = pd.to_datetime(df['BaseDateTime'], format='%Y-%m-%dT%H:%M:%S')

    # Add year, month, day, hour columns
    df['year'] = df['BaseDateTime'].dt.year
    df['month'] = df['BaseDateTime'].dt.month
    df['day'] = df['BaseDateTime'].dt.day
    df['hour'] = df['BaseDateTime'].dt.hour

    # Group by year, month, day, hour
    grouped = df.groupby(['year', 'month', 'day', 'hour'])

    # Process each group
    for (year, month, day, hour), group_df in grouped:
        # Convert to integers to ensure proper directory naming
        year, month, day, hour = int(year), int(month), int(day), int(hour)

        # Create the output directory structure
        output_dir = OUTPUT_DIR / f"year={year}" / f"month={month:02d}" / f"day={day:02d}" / f"hour={hour:02d}"
        output_dir.mkdir(parents=True, exist_ok=True)

        # Create output file path
        output_file = output_dir / f"AIS_{year}_{month:02d}_{day:02d}_processed_hour{hour:02d}.parquet"

        # Drop the partition columns before saving
        data_to_save = group_df.drop(columns=['year', 'month', 'day', 'hour'])

        # Check if the file already exists
        if output_file.exists():
            logger.info(f"File {output_file} already exists. It will be recreated.")
            try:
                # Delete the existing file
                output_file.unlink()
                logger.info(f"Deleted existing file {output_file}")
            except Exception as e:
                logger.error(f"Error deleting existing file {output_file}: {e}")

        # Convert to pyarrow table
        table = pa.Table.from_pandas(data_to_save)

        # Write to parquet
        pq.write_table(
            table,
            output_file,
            compression='snappy',
            use_dictionary=True,
            version='2.6',
            data_page_size=1048576,  # 1 MB pages
            write_statistics=True
        )

        # Track the number of rows written to this file
        hour_counts[(year, month, day, hour)] = len(data_to_save)
        logger.info(f"Saved {len(data_to_save):,} rows to {output_file}")

        # Upload to S3 if enabled
        if ENABLE_S3_UPLOAD:
            upload_to_s3(output_file)

    # Log summary of rows written
    logger.info("Summary of rows written to each output file:")
    total_written = 0
    for (year, month, day, hour), count in hour_counts.items():
        total_written += count
        logger.info(f"  {year}-{month:02d}-{day:02d} Hour {hour:02d}: {count:,} rows")

    logger.info(f"Total rows processed: {total_rows:,}")
    logger.info(f"Total rows written: {total_written:,}")

    if total_written != total_rows:
        logger.warning(f"Row count mismatch! Read {total_rows:,} rows but wrote {total_written:,} rows")

def upload_to_s3(file_path):
    """
    Upload a file to S3

    Args:
        file_path: Path to the file to upload
    """
    # Initialize S3 client
    session = boto3.session.Session()
    s3_client = session.client(
        's3',
        region_name=S3_REGION,
        endpoint_url=S3_ENDPOINT,
        aws_access_key_id=S3_ACCESS_KEY,
        aws_secret_access_key=S3_SECRET_KEY
    )

    # Upload the file
    s3_key = str(file_path.relative_to(OUTPUT_DIR))
    logger.info(f"Uploading file to s3://{S3_BUCKET_NAME}/{s3_key}")

    try:
        s3_client.upload_file(
            str(file_path),
            S3_BUCKET_NAME,
            s3_key
        )
        logger.info(f"Upload complete: s3://{S3_BUCKET_NAME}/{s3_key}")

        # Verify the upload
        try:
            s3_client.head_object(Bucket=S3_BUCKET_NAME, Key=s3_key)
            logger.info(f"Verified file exists in S3: s3://{S3_BUCKET_NAME}/{s3_key}")

            # Delete local file after successful upload
            file_path.unlink()
            logger.info(f"Deleted local file {file_path}")
        except Exception as e:
            logger.error(f"Upload verification failed, keeping local file: {e}")
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")

def process_zip_file(url):
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
        download_file(url, zip_path)

        # Extract the ZIP file
        csv_path = extract_zip(zip_path)

        # Process the CSV file
        process_csv(csv_path)

        # Clean up temporary files
        os.remove(zip_path)
        logger.info(f"Deleted {zip_path}")

        os.remove(csv_path)
        logger.info(f"Deleted {csv_path}")

    except Exception as e:
        logger.error(f"Error processing {url}: {e}")

def main():
    """Main function"""
    # Get the list of ZIP URLs
    urls = get_zip_urls()

    # Process each ZIP file one at a time
    for i, url in enumerate(urls):
        logger.info(f"Processing file {i+1}/{len(urls)}: {url}")
        process_zip_file(url)

    logger.info("Processing complete!")

if __name__ == "__main__":
    main()