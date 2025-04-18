# AIS Data Processor (zip2parquet.py)

This script downloads AIS (Automatic Identification System) data from NOAA, processes it, and converts it to Parquet format following a specific directory structure.

## Features

- Extracts all ZIP file URLs from the NOAA AIS data page
- Downloads ZIP files with progress reporting
- Processes CSV data in memory-efficient chunks
- Converts data to Apache Parquet format
- Organizes files in a hierarchical directory structure by time
- Optional upload to S3 with automatic local file cleanup

## Setup

### Setting up a Virtual Environment

It's recommended to use a virtual environment to keep the dependencies isolated from your system Python:

1. Clone this repository and navigate to the project directory:
   ```bash
   git clone https://github.com/yourusername/ais-data-processor.git
   cd ais-data-processor
   ```

2. Create a virtual environment:
   ```bash
   # Using venv (built into Python 3)
   python -m venv venv
   
   # Activate the virtual environment
   # On Windows:
   venv\Scripts\activate
   # On macOS and Linux:
   source venv/bin/activate
   ```

3. Install the required dependencies:
   ```bash
   pip install -r requirements.txt
   ```

### Requirements

The following packages are required:
- requests
- beautifulsoup4 
- pandas
- numpy
- pyarrow
- tqdm
- boto3

These are all listed in the `requirements.txt` file.

## Configuration

All configuration is handled at the top of the script in the CONFIGURATION section:

```python
# Basic Configuration
BASE_URL = "https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024/"
TMP_DIR = Path("tmp")
OUTPUT_DIR = Path(".")
CHUNK_SIZE = 500_000  # Number of rows to process at a time

# Parallel Processing
MAX_WORKERS = 4  # Number of parallel threads for processing

# S3 Configuration
ENABLE_S3_UPLOAD = False  # Set to True to enable S3 upload
S3_REGION = 'sfo3'
S3_ENDPOINT = 'https://ais-bucket.sfo3.digitaloceanspaces.com'
S3_ACCESS_KEY = 'ACCESS_KEY'
S3_SECRET_KEY = 'SECRET_KEY'
S3_BUCKET_NAME = 'ais-data'
```

## Usage

1. Activate your virtual environment (if you haven't already):
   ```bash
   # On Windows:
   venv\Scripts\activate
   # On macOS and Linux:
   source venv/bin/activate
   ```

2. Run the script:
   ```bash
   python zip2parquet.py
   ```

## Directory Structure

The script creates the following directory structure for Parquet files:

```
year=YYYY/
└── month=MM/
    └── day=DD/
        └── hour=HH/
            └── AIS_YYYY_MM_DD_processed_hourHH.parquet
```

## Data Format

The script processes AIS data with the following columns:

- MMSI (Maritime Mobile Service Identity)
- BaseDateTime
- LAT (Latitude)
- LON (Longitude)
- SOG (Speed Over Ground)
- COG (Course Over Ground)
- Heading
- VesselName
- IMO (International Maritime Organization number)
- CallSign
- VesselType
- Status
- Length
- Width
- Draft
- Cargo
- TransceiverClass

## Notes

- The script creates a `tmp` directory for temporary files
- Local Parquet files are deleted after successful S3 upload when ENABLE_S3_UPLOAD is set to True
- Processing is done in parallel using ThreadPoolExecutor for efficiency

