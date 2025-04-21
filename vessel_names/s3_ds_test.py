import pyarrow as pa
import pyarrow.dataset as ds
import pyarrow.compute as pc
from pyarrow.fs import S3FileSystem
import pandas as pd
from dotenv import load_dotenv
import os

# Load environment variables from .env file
load_dotenv()

# S3 Configuration
S3_REGION = os.getenv('S3_REGION')
S3_ENDPOINT = os.getenv('S3_ENDPOINT')
S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_KEY = os.getenv('S3_SECRET_KEY')
S3_BUCKET_NAME = os.getenv('S3_BUCKET_NAME')

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

def get_unique_vessel_names():
    # Initialize S3 filesystem
    s3 = S3FileSystem(
        region=S3_REGION,
        endpoint_override=S3_ENDPOINT,
        access_key=S3_ACCESS_KEY,
        secret_key=S3_SECRET_KEY
    )

    # Create dataset directly from S3 bucket
    print(f"Creating dataset from S3 bucket: {S3_BUCKET_NAME}")
    dataset = ds.dataset(
        S3_BUCKET_NAME,
        filesystem=s3,
        format="parquet",
        schema=schema
    )

    # Extract unique vessel names and their counts
    print("Analyzing vessel names...")
    table = dataset.to_table(columns=["VesselName"])
    vessel_counts = table.group_by("VesselName").aggregate([("VesselName", "count")]).to_pandas()
    vessel_counts.columns = ["VesselName", "RecordCount"]
    vessel_counts = vessel_counts.sort_values("RecordCount", ascending=False)

    return vessel_counts

def main():
    try:
        # Get unique vessel names and their record counts
        vessel_counts = get_unique_vessel_names()

        # Display results
        print("\nTop 20 vessels by record count:")
        print(vessel_counts.head(20))
        print(f"\nTotal unique vessel names: {len(vessel_counts)}")

    except Exception as e:
        print(f"Error: {str(e)}")

if __name__ == "__main__":
    main()