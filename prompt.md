Create a Python script called zip2parquet.py that:

1. Extracts ZIP file URLs from NOAA's AIS data page (https://coast.noaa.gov/htdata/CMSP/AISDataHandler/2024)
2. Downloads these ZIP files to a temporary directory with progress tracking
3. Extracts CSV files from the ZIP archives
4. Processes the CSV data in chunks, with these specific column types:
   - MMSI (str), BaseDateTime (datetime), LAT/LON (float)
   - SOG/COG/Heading/Length/Width/Draft (float)
   - VesselType/Status (Int32)
   - VesselName/IMO/CallSign/Cargo/TransceiverClass (str)
5. Organizes the data into a hierarchical directory structure by time:
   - year=YYYY/month=MM/day=DD/hour=HH/AIS_YYYY_MM_DD_processed_hourHH.parquet
6. Includes an optional S3 upload feature (disabled by default)
7. Uses proper error handling and logging throughout

The script should:
- Use a configurable chunk size (default 500,000 rows)
- Process files in parallel using ThreadPoolExecutor
- Allow configuration of all parameters at the top of the script
- Handle date parsing correctly with format '%Y-%m-%dT%H:%M:%S'
- Maintain all original data values without modification
- Remove temporary files after processing

Also create a requirements.txt file listing dependencies: requests, beautifulsoup4, pandas, numpy, pyarrow, tqdm, boto3.