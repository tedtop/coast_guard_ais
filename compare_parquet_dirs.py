import os
import pandas as pd
import argparse
import glob
from datetime import datetime
import re
from tabulate import tabulate
import sys

def extract_datetime_from_path(file_path):
    """Extract year, month, day, hour from file path based on the structure."""
    # Extract components from directory structure
    pattern = r'year=(\d{4})/month=(\d{2})/day=(\d{2})/hour=(\d{2})/'
    match = re.search(pattern, file_path)

    if match:
        year, month, day, hour = match.groups()
        return f"{year}-{month}-{day}-{hour}"

    return None

def get_all_parquet_files(directory):
    """Get all parquet files in the directory with their date and row count."""
    # Check if we need to append year=* to the path
    if not os.path.exists(os.path.join(directory, "year=2024")):
        print(f"Warning: Could not find 'year=2024' in {directory}", file=sys.stderr)
        print(f"Contents of {directory}: {os.listdir(directory)}", file=sys.stderr)

    # Try multiple possible patterns to locate the files
    patterns = [
        os.path.join(directory, "year=*/month=*/day=*/hour=*/*.parquet"),
        os.path.join(directory, "*/year=*/month=*/day=*/hour=*/*.parquet"),
        os.path.join(directory, "*/*/*/*.parquet")  # Last resort - any deeply nested parquet file
    ]

    files = []
    for pattern in patterns:
        files = glob.glob(pattern)
        if files:
            print(f"Found files using pattern: {pattern}")
            break

    file_info = []
    for file_path in files:
        datetime_str = extract_datetime_from_path(file_path)
        if datetime_str:
            try:
                df = pd.read_parquet(file_path)
                row_count = len(df)
                file_info.append({
                    'path': file_path,
                    'datetime': datetime_str,
                    'row_count': row_count
                })
            except Exception as e:
                print(f"Error reading {file_path}: {str(e)}", file=sys.stderr)

    return file_info

def compare_directories(dir1, dir2):
    """Compare row counts of parquet files in two directories."""
    print(f"Processing files in {dir1}...")
    files1 = get_all_parquet_files(dir1)
    print(f"Found {len(files1)} parquet files in {dir1}")

    print(f"\nProcessing files in {dir2}...")
    files2 = get_all_parquet_files(dir2)
    print(f"Found {len(files2)} parquet files in {dir2}")

    # Create dictionaries for quick lookup by datetime
    files1_dict = {f['datetime']: f for f in files1}
    files2_dict = {f['datetime']: f for f in files2}

    # Get all unique datetimes
    all_datetimes = sorted(set(list(files1_dict.keys()) + list(files2_dict.keys())))

    # Prepare comparison results
    comparison_results = []

    for dt in all_datetimes:
        in_dir1 = dt in files1_dict
        in_dir2 = dt in files2_dict

        row_count1 = files1_dict[dt]['row_count'] if in_dir1 else 0
        row_count2 = files2_dict[dt]['row_count'] if in_dir2 else 0

        diff = row_count1 - row_count2
        pct_diff = 0
        if row_count2 > 0:
            pct_diff = (diff / row_count2) * 100

        comparison_results.append({
            'datetime': dt,
            'in_' + os.path.basename(dir1): in_dir1,
            'in_' + os.path.basename(dir2): in_dir2,
            os.path.basename(dir1) + '_rows': row_count1,
            os.path.basename(dir2) + '_rows': row_count2,
            'difference': diff,
            'pct_difference': round(pct_diff, 2)
        })

    # Print results as a table
    print("\nComparison Results:")

    # Calculate totals
    total_rows1 = sum(f['row_count'] for f in files1)
    total_rows2 = sum(f['row_count'] for f in files2)
    total_diff = total_rows1 - total_rows2
    total_pct_diff = 0
    if total_rows2 > 0:
        total_pct_diff = (total_diff / total_rows2) * 100

    print(f"\nTotal row counts:")
    print(f"{os.path.basename(dir1)}: {total_rows1} rows")
    print(f"{os.path.basename(dir2)}: {total_rows2} rows")
    print(f"Difference: {total_diff} rows ({round(total_pct_diff, 2)}%)")

    # Print only files with differences
    diff_results = [r for r in comparison_results if r['difference'] != 0 or not (r['in_' + os.path.basename(dir1)] and r['in_' + os.path.basename(dir2)])]

    if diff_results:
        print("\nFiles with missing data or row count differences:")
        headers = diff_results[0].keys()
        rows = [list(result.values()) for result in diff_results]
        print(tabulate(rows, headers=headers, tablefmt="grid"))
    else:
        print("\nAll files have identical row counts.")

    # Summary statistics
    missing_in_dir1 = sum(1 for r in comparison_results if not r['in_' + os.path.basename(dir1)])
    missing_in_dir2 = sum(1 for r in comparison_results if not r['in_' + os.path.basename(dir2)])
    files_with_less_rows = sum(1 for r in comparison_results
                             if r['in_' + os.path.basename(dir1)]
                             and r['in_' + os.path.basename(dir2)]
                             and r[os.path.basename(dir1) + '_rows'] < r[os.path.basename(dir2) + '_rows'])

    print(f"\nSummary:")
    print(f"Files missing in {os.path.basename(dir1)}: {missing_in_dir1}")
    print(f"Files missing in {os.path.basename(dir2)}: {missing_in_dir2}")
    print(f"Files in {os.path.basename(dir1)} with fewer rows than {os.path.basename(dir2)}: {files_with_less_rows}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Compare row counts of parquet files in two directories')
    parser.add_argument('--dir1', default='coast_guard_ais', help='First directory path (default: coast_guard_ais)')
    parser.add_argument('--dir2', default='ships_visualizer', help='Second directory path (default: ships_visualizer)')
    parser.add_argument('--recursive', action='store_true', help='Search recursively in all subdirectories')
    args = parser.parse_args()

    compare_directories(args.dir1, args.dir2)