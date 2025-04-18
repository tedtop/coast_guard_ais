# Detailed Setup Guide for AIS Data Processor

This guide provides step-by-step instructions for setting up the AIS Data Processor project on different operating systems.

## Prerequisites

- Python 3.8 or higher
- Git (optional, for cloning the repository)

## Setting Up on Windows

1. **Clone or download the repository**
   ```
   git clone https://github.com/yourusername/ais-data-processor.git
   cd ais-data-processor
   ```
   
   Alternatively, download and extract the ZIP file from GitHub.

2. **Create a virtual environment**
   ```
   python -m venv venv
   ```

3. **Activate the virtual environment**
   ```
   venv\Scripts\activate
   ```
   
   Your command prompt should now show `(venv)` at the beginning of the line.

4. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

5. **Configure the script**
   
   Edit the configuration section at the top of `zip2parquet.py` as needed.

6. **Run the script**
   ```
   python zip2parquet.py
   ```

## Setting Up on macOS/Linux

1. **Clone or download the repository**
   ```
   git clone https://github.com/yourusername/ais-data-processor.git
   cd ais-data-processor
   ```
   
   Alternatively, download and extract the ZIP file from GitHub.

2. **Create a virtual environment**
   ```
   python3 -m venv venv
   ```

3. **Activate the virtual environment**
   ```
   source venv/bin/activate
   ```
   
   Your terminal should now show `(venv)` at the beginning of the line.

4. **Install dependencies**
   ```
   pip install -r requirements.txt
   ```

5. **Configure the script**
   
   Edit the configuration section at the top of `zip2parquet.py` as needed.

6. **Run the script**
   ```
   python zip2parquet.py
   ```

## Troubleshooting

### Common Issues

1. **Python version issues**
   
   Ensure you have Python 3.8 or higher:
   ```
   python --version
   ```
   
   On some systems, you may need to use `python3` instead of `python`.

2. **Package installation failures**
   
   If you encounter issues installing packages, try upgrading pip:
   ```
   pip install --upgrade pip
   ```
   
   Then try installing the requirements again.

3. **Memory issues when processing large files**
   
   Adjust the `CHUNK_SIZE` parameter in the configuration section to a smaller value:
   ```python
   CHUNK_SIZE = 100_000  # Reduced from 500_000
   ```

4. **S3 connection issues**
   
   If you get S3 connection errors, verify your credentials and network connectivity. 
   For DigitalOcean Spaces specifically, ensure the endpoint URL format is correct.

## Updating Dependencies

If you need to update the project dependencies later:

1. Activate your virtual environment
2. Run:
   ```
   pip install -r requirements.txt --upgrade
   ```

## Deactivating the Virtual Environment

When you're done working with the project, deactivate the virtual environment:

```
deactivate
```
