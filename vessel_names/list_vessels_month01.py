import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pyarrow.fs import S3FileSystem
import pandas as pd
from dotenv import load_dotenv
import os
import time
from datetime import datetime

# Output filename for the results
OUTPUT_FILE = "vessel_names_month01.csv"

def log_message(message):
    """Log a message with timestamp to console"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] {message}")

# Load environment variables from .env file
log_message("Loading environment variables from .env file...")
load_dotenv()

# S3 Configuration
S3_REGION = os.getenv('S3_REGION')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

log_message(f"S3 Configuration loaded:")
log_message(f"  Region: {S3_REGION}")
log_message(f"  Endpoint: {S3_ENDPOINT}")
log_message(f"  Bucket: {S3_BUCKET_NAME}")

# Define the schema based on provided dtypes
schema = pa.schema([
    ('MMSI', pa.string()),
    ('BaseDateTime', pa.string()),
    ('LAT', pa.float64()),
    ('LON', pa.float64()),
    ('SOG', pa.float32()),
    ('COG', pa.float32()),
    ('Heading', pa.float32()),
    ('VesselName', pa.string()),
    ('IMO', pa.string()),
    ('CallSign', pa.string()),
    ('VesselType', pa.int32()),
    ('Status', pa.int32()),
    ('Length', pa.float32()),
    ('Width', pa.float32()),
    ('Draft', pa.float32()),
    ('Cargo', pa.string()),
    ('TransceiverClass', pa.string())
])

def get_unique_vessel_names_month01():
    """
    Get unique vessel names and their record counts from month=01 parquet files
    """
    start_time = time.time()
    log_message("Initializing S3 filesystem...")
    
    # Initialize S3 filesystem
    s3 = S3FileSystem(
        region=S3_REGION,
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY
    )
    
    # Define the base path for month=01
    base_path = f"{S3_BUCKET_NAME}/year=2024/month=01"
    log_message(f"Creating dataset from S3 path: {base_path}")
    
    # Create dataset for month=01 only
    dataset = ds.dataset(
        base_path,
        filesystem=s3,
        format="parquet",
        schema=schema
    )
    
    log_message(f"Dataset created successfully. Scanning for vessel names...")
    
    # Filter out empty vessel names and extract unique vessel names and their counts
    log_message("Extracting VesselName column...")
    table = dataset.to_table(columns=["VesselName"])
    log_message("Filtering out empty or null vessel names...")
    
    # Filter out null values
    mask = pc.is_valid(table["VesselName"])
    table = table.filter(mask)
    
    # Filter out empty strings
    mask = pc.not_equal(table["VesselName"], "")
    table = table.filter(mask)
    
    log_message("Grouping vessel names and counting records...")
    # Group by vessel name and count occurrences
    vessel_counts = table.group_by("VesselName").aggregate([("VesselName", "count")]).to_pandas()
    vessel_counts.columns = ["VesselName", "RecordCount"]
    vessel_counts = vessel_counts.sort_values("RecordCount", ascending=False)
    
    elapsed_time = time.time() - start_time
    log_message(f"Processing completed in {elapsed_time:.2f} seconds")
    log_message(f"Found {len(vessel_counts)} unique vessel names")
    
    return vessel_counts

def main():
    try:
        # Get unique vessel names and their record counts for month=01
        log_message("Starting vessel name extraction for month=01...")
        vessel_counts = get_unique_vessel_names_month01()
        
        # Display results
        log_message(f"\nTop 20 vessels by record count:")
        print(vessel_counts.head(20))
        log_message(f"\nTotal unique vessel names: {len(vessel_counts)}")
        
        # Save results to file
        log_message(f"Saving results to {OUTPUT_FILE}...")
        vessel_counts.to_csv(OUTPUT_FILE, index=False)
        log_message(f"Results saved successfully to {OUTPUT_FILE}")
        
    except Exception as e:
        log_message(f"Error: {str(e)}")

if __name__ == "__main__":
    main()