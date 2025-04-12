#!/usr/bin/env python3
import os
import subprocess
import shutil
import gzip
import glob
import duckdb
import logging
import math
import json
import re
import time
import csv
import zstandard as zstd
import concurrent.futures
import threading
import gc
import random
import queue
from collections import defaultdict, deque

# Logging configuration
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s',
                    handlers=[logging.FileHandler("processing.log"), logging.StreamHandler()])
logger = logging.getLogger()

# Script directory
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

# Configurable parameters
FIELD_PRESENCE_THRESHOLD = 0.20  # Field must be present in at least
AAC_FIELD_PRESENCE_THRESHOLD = 0.20  # Same threshold for AAC records, adjust if needed
FILE_SAMPLE_RATE = 0.20  # Analyze only 20% of files

# Patterns to identify different datasets
DATASET_PATTERNS = {
    'records': re.compile(r'aarecords_(?!metadata|journals|digital_lending)..json.gz'),
    'metadata': re.compile(r'aarecords_metadata.*json.gz'),
    'journals': re.compile(r'aarecords_journals.*json.gz'),
    'digital_lending': re.compile(r'aarecords_digital_lending.*json.gz')
}

# Pattern for AAC datasets
AAC_PATTERN = re.compile(r'annas_archive_meta__aacid__(.+?)__.*\.jsonl(?:\.seekable)?\.zst')

# Pattern for MariaDB datasets
MARIADB_PATTERN = re.compile(r'allthethings\.([^.]+)\..*\.dat\.gz')

# Thread-safe dictionaries for tracking field counts and record counts
DATASET_FIELDS_LOCK = threading.Lock()
DATASET_RECORD_COUNTS_LOCK = threading.Lock()
DATASET_FIELDS = defaultdict(lambda: defaultdict(int))
DATASET_RECORD_COUNTS = defaultdict(int)


# OPTIMIZED SETTINGS
MAX_WORKERS_ANALYSIS = 50  # Increased to 50 for analysis
MAX_WORKERS_CONVERSION = 4
MAX_WORKERS_CONVERSION_MARIADB = 1
MAX_WORKERS_CONVERSION_ELASTICSEARCH = 1
MAX_WORKERS_CONVERSION_ELASTICSEARCHAUX = 1
MAX_WORKERS_CONVERSION_AAC = 1

MAX_WORKERS_MERGE = 4  # For merging parquet files
mega_batch_size = 500  # The last merge

# Target size for split files (in MB)
TARGET_SPLIT_SIZE_MB = 10  # 10MB parts for better parallelization


def create_directories():
    """Creates necessary directories if they don't exist."""
    dirs = [
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_json'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_json'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'aac'),
        os.path.join(SCRIPT_DIR, 'data', 'aac_json'),
        os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'mariadb'),
        os.path.join(SCRIPT_DIR, 'data', 'mariadb_dat'),
        os.path.join(SCRIPT_DIR, 'data', 'mariadb_parts'),
        os.path.join(SCRIPT_DIR, 'data', 'torrent'),
        os.path.join(SCRIPT_DIR, 'data', 'error'),
        os.path.join(SCRIPT_DIR, 'reports'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchF'),
        os.path.join(SCRIPT_DIR, 'data', 'elasticsearchauxF'),
        os.path.join(SCRIPT_DIR, 'data', 'aacF'),
        os.path.join(SCRIPT_DIR, 'data', 'mariadbF')
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)
        logging.info(f"Created directory: {d}")


def determine_dataset(filename):
    """Determines which dataset a file belongs to based on its name."""
    # First check for AAC files
    aac_match = AAC_PATTERN.match(filename)
    if aac_match:
        return f"aac_{aac_match.group(1)}"

    # Check for MariaDB files
    mariadb_match = MARIADB_PATTERN.match(filename)
    if mariadb_match:
        return f"mariadb_{mariadb_match.group(1)}"

    # Then check for regular datasets
    for dataset, pattern in DATASET_PATTERNS.items():
        if pattern.match(filename):
            return dataset
    return 'other'  # Default category if no pattern matches


def find_and_move_files():
    """
    Searches for files in the structure:
    data/torrent/aa*/elasticsearch/.gz, data/torrent/aa/elasticsearchaux/*.gz,
    data/torrent/aa*/aac/*.zst, and data/torrent/aa*/mariadb/*.gz
    Then moves files to their respective directories, ignoring size limit for MariaDB files.
    """
    min_size = 2 * 1024 * 1024  # 2MB in bytes (for non-MariaDB files)
    moved_files = 0

    torrent_dir = os.path.join(SCRIPT_DIR, 'data', 'torrent')
    aa_dirs = glob.glob(os.path.join(torrent_dir, 'aa*'))

    if not aa_dirs:
        logging.warning("No directories starting with 'aa' found in data/torrent")
        return

    for aa_dir in aa_dirs:
        for folder in ['elasticsearch', 'elasticsearchaux', 'aac', 'mariadb']:
            source_dir = os.path.join(aa_dir, folder)
            if os.path.exists(source_dir) and os.path.isdir(source_dir):
                target_dir = os.path.join(SCRIPT_DIR, 'data', folder)

                # Different patterns based on the folder
                if folder == 'aac':
                    files = glob.glob(os.path.join(source_dir, '*.zst'))
                else:
                    files = glob.glob(os.path.join(source_dir, '*.gz'))

                for file_path in files:
                    file_name = os.path.basename(file_path)

                    # Special handling for MariaDB files - ignore size limit
                    if folder == 'mariadb' and file_name.endswith('.dat.gz'):
                        should_move = True
                        logging.info(f"Found MariaDB file {file_name} (moving regardless of size)")
                    else:
                        # For other file types, check size
                        should_move = os.path.getsize(file_path) >= min_size

                    if should_move:
                        target_path = os.path.join(target_dir, file_name)
                        shutil.move(file_path, target_path)
                        moved_files += 1
                        logging.info(f"Moved {file_name} to {target_dir}")

    if moved_files == 0:
        logging.warning("No files were moved. Verify that structure and sizes are correct.")
    else:
        logging.info(f"Total files moved: {moved_files}")


def extract_nested_fields(data, prefix=""):
    """
    Recursively extracts all fields from a nested JSON using dot notation.
    Returns a set of field paths.
    """
    fields = set()
    queue = deque([(data, prefix)])

    while queue:
        current_data, current_prefix = queue.popleft()
        if isinstance(current_data, dict):
            for key, value in current_data.items():
                field_path = f"{current_prefix}.{key}" if current_prefix else key
                fields.add(field_path)
                if isinstance(value, (dict, list)):
                    queue.append((value, field_path))
        elif isinstance(current_data, list):
            if current_data and isinstance(current_data[0], (dict, list)):
                array_path = f"{current_prefix}[0]" if current_prefix else "[0]"
                queue.append((current_data[0], array_path))

    return fields


def analyze_json_structure(file_path, dataset_name):
    """Analyzes entire JSON file and counts field frequencies."""
    logging.info(f"Analyzing ALL records in {os.path.basename(file_path)} for dataset: {dataset_name}")
    record_count = 0
    local_fields = defaultdict(int)

    with open(file_path, 'r') as f:
        for line in f:
            try:
                record = json.loads(line.strip())
                record_count += 1
                all_fields = extract_nested_fields(record)
                for field in all_fields:
                    local_fields[field] += 1
            except json.JSONDecodeError:
                logging.warning(f"Invalid JSON record in {os.path.basename(file_path)}")
                continue

    # Thread-safe update of global counters using dynamic sample rate scaling
    scaling_factor = 1.0 / FILE_SAMPLE_RATE
    with DATASET_FIELDS_LOCK:
        for field, count in local_fields.items():
            # Apply scaling because we only analyzed a percentage of files
            DATASET_FIELDS[dataset_name][field] += int(count * scaling_factor)

    with DATASET_RECORD_COUNTS_LOCK:
        # Apply scaling to record count too
        DATASET_RECORD_COUNTS[dataset_name] += int(record_count * scaling_factor)

    logging.info(f"Processed ALL {record_count} records from {os.path.basename(file_path)}")
    logging.info(f"Found {len(local_fields)} unique fields")
    return record_count


def analyze_json_column(file_path, column_index, column_name, dataset_name):
    """Analyzes JSON in a specific column of a CSV file and counts field frequencies."""
    logging.info(
        f"Analyzing JSON in column '{column_name}' in {os.path.basename(file_path)} for dataset: {dataset_name}")
    record_count = 0
    local_fields = defaultdict(int)

    with open(file_path, 'r', newline='') as f:
        try:
            reader = csv.reader(f, delimiter='\t')
            header = next(reader)  # Skip header

            for row in reader:
                try:
                    if len(row) > column_index and row[column_index]:
                        json_data = json.loads(row[column_index])
                        record_count += 1
                        all_fields = extract_nested_fields(json_data, prefix=column_name)
                        for field in all_fields:
                            local_fields[field] += 1
                except (json.JSONDecodeError, IndexError) as e:
                    continue  # Skip invalid JSON or short rows
        except Exception as e:
            logging.warning(f"Error processing CSV file {os.path.basename(file_path)}: {str(e)}")

    # Thread-safe update of global counters using dynamic sample rate scaling
    scaling_factor = 1.0 / FILE_SAMPLE_RATE
    with DATASET_FIELDS_LOCK:
        for field, count in local_fields.items():
            # Apply scaling because we only analyzed a percentage of files
            DATASET_FIELDS[dataset_name][field] += int(count * scaling_factor)

    logging.info(f"Processed {record_count} JSON records from column '{column_name}' in {os.path.basename(file_path)}")
    logging.info(f"Found {len(local_fields)} unique JSON fields in column '{column_name}'")
    return record_count


def split_for_target_size(jsonl_path, parts_dir, file_name, target_size_mb=10, is_csv=False):
    """
    Splits a large file into smaller files, with automatic detection and handling of binary CSV files.
    """
    logging.info(f"Splitting {os.path.basename(jsonl_path)} for target size of {target_size_mb}MB...")

    # Detect if file contains binary data by trying to read a portion with UTF-8
    is_binary_csv = False
    try:
        with open(jsonl_path, 'r', encoding='utf-8') as test_f:
            test_f.read(1024)  # Try to read a small chunk
    except UnicodeDecodeError:
        logging.info(f"Binary CSV detected for {os.path.basename(jsonl_path)}, using latin-1 encoding")
        is_binary_csv = True

    # Choose encoding based on detection
    if is_binary_csv:
        encoding = 'latin-1'
        errors = 'surrogateescape'
    else:
        encoding = 'utf-8'
        errors = 'replace'

    target_size_bytes = target_size_mb * 1024 * 1024  # Convert target size to bytes
    current_part = 1
    current_size = 0
    current_lines = []
    header_line = None
    part_files = []

    # Open the input file with appropriate encoding
    with open(jsonl_path, 'r', encoding=encoding, errors=errors) as f:
        # If CSV, read and save the header first
        if is_csv:
            header_line = f.readline()

        for line in f:
            current_lines.append(line)
            current_size += len(line.encode(encoding))

            # If the accumulated size exceeds the target size, write to a new part file
            if current_size >= target_size_bytes:
                part_file = os.path.join(parts_dir, f"{file_name}_part_{current_part}.{'csv' if is_csv else 'jsonl'}")

                with open(part_file, 'w', encoding=encoding, errors=errors) as part_f:
                    # For CSV files, write the header line first
                    if is_csv and header_line:
                        part_f.write(header_line)
                    part_f.writelines(current_lines)

                part_files.append(part_file)

                # Reset for the next part
                current_part += 1
                current_size = 0
                current_lines = []

        # If there are any remaining lines that didn't exceed the size limit
        if current_lines:
            part_file = os.path.join(parts_dir, f"{file_name}_part_{current_part}.{'csv' if is_csv else 'jsonl'}")
            with open(part_file, 'w', encoding=encoding, errors=errors) as part_f:
                # For CSV files, write the header line first
                if is_csv and header_line:
                    part_f.write(header_line)
                part_f.writelines(current_lines)
            part_files.append(part_file)

    logging.info(f"Created {len(part_files)} split files.")
    return part_files


def decompress_zst_file(zst_file, output_file):
    """Decompress a Zstandard compressed file."""
    logging.info(f"Decompressing {os.path.basename(zst_file)}...")

    with open(zst_file, 'rb') as f_in:
        dctx = zstd.ZstdDecompressor()
        with open(output_file, 'wb') as f_out:
            dctx.copy_stream(f_in, f_out)

    logging.info(f"Decompressed to {os.path.basename(output_file)}")


def decompress_file(compressed_file, json_dir, compression):
    """Decompress a single compressed file."""
    base_name = os.path.basename(compressed_file)

    # More robust pattern matching
    if compression == 'gzip':
        # MariaDB dat files
        if base_name.endswith('.dat.gz'):
            file_name = base_name[:-7]  # Remove .dat.gz
            output_name = f"{file_name}.dat"
        # Match anything ending with .json.gz
        elif base_name.endswith('.json.gz'):
            file_name = base_name[:-8]  # Remove .json.gz
            output_name = f"{file_name}.json"
        else:
            logging.error(f"Unexpected filename format for gzip file: {base_name}")
            return None
    else:  # zstd
        # Handle various zst file patterns
        if base_name.endswith('.jsonl.seekable.zst'):
            file_name = base_name[:-18]  # Remove .jsonl.seekable.zst
            output_name = f"{file_name}.jsonl"
        elif base_name.endswith('.jsonl.zst'):
            file_name = base_name[:-10]  # Remove .jsonl.zst
            output_name = f"{file_name}.jsonl"
        elif base_name.endswith('.zst'):
            file_name = base_name[:-4]  # Remove .zst
            # Check if remaining part ends with .jsonl
            if file_name.endswith('.jsonl'):
                output_name = file_name
            else:
                output_name = f"{file_name}.jsonl"  # Assume it should be jsonl
        else:
            logging.error(f"Unexpected filename format for zst file: {base_name}")
            return None

    output_path = os.path.join(json_dir, output_name)

    logging.info(f"Decompressing {base_name}...")

    try:
        # Decompress the file
        if compression == 'gzip':
            with gzip.open(compressed_file, 'rb') as f_in:
                with open(output_path, 'wb') as f_out:
                    shutil.copyfileobj(f_in, f_out)
        else:  # zstd
            decompress_zst_file(compressed_file, output_path)

        logging.info(f"Successfully decompressed {base_name}")
        return output_path

    except Exception as e:
        logging.error(f"Error decompressing {base_name}: {e}")
        return None


def process_directory(config):
    """
    Process an entire directory with sequential decompression and splitting:
    - Files are decompressed and split one at a time
    - Analysis starts concurrently after splitting completes
    """
    source_dir = config['source_dir']
    temp_dir = config['temp_dir']
    parts_dir = config['parts_dir']
    target_dir = config['target_dir']
    compression = config['compression']
    file_type = config.get('file_type', 'json')  # Default to JSON

    # Clear out old field data for this processing batch
    global DATASET_FIELDS, DATASET_RECORD_COUNTS
    DATASET_FIELDS.clear()
    DATASET_RECORD_COUNTS.clear()

    # 1. Find all compressed files in this directory
    if compression == 'gzip':
        if file_type == 'csv':
            compressed_files = glob.glob(os.path.join(source_dir, '*.dat.gz'))
        else:
            compressed_files = glob.glob(os.path.join(source_dir, '*.json.gz'))
    else:  # zstd
        compressed_files = glob.glob(os.path.join(source_dir, '*.zst'))

    if not compressed_files:
        logging.info(f"No compressed files found in {source_dir}, skipping directory")
        return

    logging.info(f"Processing directory {source_dir} with {len(compressed_files)} compressed files")

    # Track all split parts for each dataset
    all_split_parts = defaultdict(list)

    # Process each file one by one (non-concurrent)
    for i, compressed_file in enumerate(compressed_files, 1):
        base_name = os.path.basename(compressed_file)
        logging.info(f"Processing file {i}/{len(compressed_files)}: {base_name}")

        # 1. Decompress the file (non-concurrent)
        decompressed_file = decompress_file(compressed_file, temp_dir, compression)
        if not decompressed_file or not os.path.exists(decompressed_file):
            logging.error(f"Failed to decompress {base_name}, skipping")
            continue

        # 2. Split the file (non-concurrent)
        file_name = os.path.splitext(os.path.basename(decompressed_file))[0]
        # For .dat files, keep the .dat extension in the file name
        if decompressed_file.endswith('.dat'):
            file_name = os.path.basename(decompressed_file)

        try:
            # Split the file into parts, specifying if it's CSV (MariaDB .dat files are tab-separated)
            is_csv = file_type == 'csv' or decompressed_file.endswith('.dat')
            parts = split_for_target_size(decompressed_file, parts_dir, file_name,
                                          target_size_mb=TARGET_SPLIT_SIZE_MB, is_csv=is_csv)

            # Determine dataset for these parts
            if compression == 'gzip':
                if file_type == 'csv' or decompressed_file.endswith('.dat'):
                    full_name = f"{file_name}.dat.gz"
                else:
                    full_name = f"{file_name}.json.gz"
            else:  # zstd
                full_name = f"{file_name}.jsonl.seekable.zst"

            dataset_name = determine_dataset(full_name)

            # Add parts to the appropriate dataset group
            all_split_parts[dataset_name].extend(parts)

            # Remove original decompressed file
            os.remove(decompressed_file)
            logging.info(f"Removed {os.path.basename(decompressed_file)} after splitting")

        except Exception as e:
            logging.error(f"Error splitting {os.path.basename(decompressed_file)}: {str(e)}")

    logging.info(f"All files decompressed and split. Found {len(all_split_parts)} datasets.")

    # Handle CSV files specially - analyze JSON columns if present
    if file_type == 'csv':
        process_csv_datasets(all_split_parts)
    else:
        # Now that all files are split and grouped by dataset, sample and analyze each dataset
        for dataset_name, parts in all_split_parts.items():
            logging.info(f"Dataset {dataset_name} has {len(parts)} split files")

            # Sample a percentage of files randomly for this dataset
            sample_size = max(1, int(len(parts) * FILE_SAMPLE_RATE))
            sampled_files = random.sample(parts, sample_size)

            logging.info(f"Analyzing {sample_size} files ({FILE_SAMPLE_RATE * 100:.1f}%) for dataset {dataset_name}")

            # Analyze all sampled files in parallel
            with concurrent.futures.ThreadPoolExecutor(max_workers=MAX_WORKERS_ANALYSIS) as executor:
                # Submit analysis tasks
                futures = [
                    executor.submit(analyze_json_structure, file, dataset_name)
                    for file in sampled_files
                ]

                # Collect all results
                for future in concurrent.futures.as_completed(futures):
                    try:
                        future.result()
                    except Exception as e:
                        logging.error(f"Error during JSON analysis: {e}")

    # Write field reports for this directory
    write_field_reports_for_directory(source_dir)

    # Now convert all files to Parquet
    convert_to_parquet(all_split_parts, target_dir, file_type=file_type)

    # Merge Parquet files for this directory
    merge_parquet_files_for_directory(config)

    # Cleanup temporary files for this directory
    cleanup_temp_files(temp_dir, parts_dir)

    logging.info(f"Completed processing directory {source_dir}")


def process_csv_datasets(all_split_parts):
    """
    Analyzes CSV files and generates comprehensive field reports.
    """
    for dataset_name, parts in all_split_parts.items():
        logging.info(f"Processing CSV dataset {dataset_name} with {len(parts)} parts")

        if not parts:
            continue

        # Sample a percentage of files for column analysis
        sample_size = max(1, int(len(parts) * FILE_SAMPLE_RATE))
        sampled_files = random.sample(parts, sample_size) if len(parts) > 1 else parts

        # Analyze CSV structure
        try:
            # First detect if it's a binary CSV by trying to read with UTF-8
            is_binary_csv = False
            csv_encoding = 'utf-8'
            csv_errors = 'replace'
            csv.field_size_limit(min(2147483647, 16777216))

            try:
                with open(sampled_files[0], 'r', encoding='utf-8') as test_f:
                    test_f.read(1024)
            except UnicodeDecodeError:
                is_binary_csv = True
                csv_encoding = 'latin-1'
                csv_errors = 'surrogateescape'
                logging.info(f"Binary CSV detected for {dataset_name}, using latin-1 encoding")

            # Detect delimiter by examining the first line
            delimiter = '\t'  # Default to tab
            with open(sampled_files[0], 'r', encoding=csv_encoding, errors=csv_errors) as f:
                first_line = f.readline().strip()
                if ',' in first_line and first_line.count(',') > first_line.count('\t'):
                    delimiter = ','
                    logging.info(f"Detected comma delimiter for {dataset_name}")
                else:
                    logging.info(f"Detected tab delimiter for {dataset_name}")

            # Only get header once with appropriate encoding and delimiter
            with open(sampled_files[0], 'r', encoding=csv_encoding, errors=csv_errors) as f:
                # Use csv module for proper parsing
                reader = csv.reader(f, delimiter=delimiter)
                header = next(reader)

                # Save header fields to DATASET_FIELDS with 100% frequency
                total_records = 0  # We'll count these below
                with DATASET_FIELDS_LOCK:
                    for col in header:
                        # Clean column names for reporting
                        clean_col = col.strip('"\'').replace('\n', ' ')
                        DATASET_FIELDS[dataset_name][clean_col] = 1  # Will set actual count below

            # Count records and update field frequency
            for file_path in sampled_files:
                with open(file_path, 'r', encoding=csv_encoding, errors=csv_errors) as f:
                    reader = csv.reader(f, delimiter=delimiter)
                    next(reader)  # Skip header
                    file_records = 0
                    for _ in reader:
                        file_records += 1
                    total_records += file_records

            # Update the frequency for each field to match total_records
            with DATASET_FIELDS_LOCK:
                for field in DATASET_FIELDS[dataset_name]:
                    DATASET_FIELDS[dataset_name][field] = total_records

            # Thread-safe update of global record count with scaling
            scaling_factor = 1.0 / FILE_SAMPLE_RATE
            estimated_total = int(total_records * scaling_factor)

            with DATASET_RECORD_COUNTS_LOCK:
                DATASET_RECORD_COUNTS[dataset_name] = estimated_total

            logging.info(f"Found {len(header)} columns in dataset {dataset_name}")
            logging.info(f"Estimated {estimated_total:,} total records in dataset {dataset_name}")

        except Exception as e:
            logging.error(f"Error analyzing CSV structure for {dataset_name}: {e}")
            import traceback
            logging.error(traceback.format_exc())


def get_common_fields(dataset_name, threshold=None):
    """
    Calculates fields that appear in at least a percentage (threshold) of records.
    Returns a list of tuples (field_path, sql_column_name).
    """
    if threshold is None:
        # Use different thresholds based on dataset type
        if dataset_name.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD

    if dataset_name not in DATASET_RECORD_COUNTS:
        return []

    total_records = DATASET_RECORD_COUNTS[dataset_name]
    if total_records == 0:
        return []

    threshold_count = total_records * threshold
    common_fields = []

    for field, count in DATASET_FIELDS[dataset_name].items():
        if count >= threshold_count:
            # Create SQL-friendly column name
            column_name = field.replace('.', '_').replace('[', '_').replace(']', '_')
            common_fields.append((field, column_name))

    return sorted(common_fields)
    """
    Calculates fields that appear in at least a percentage (threshold) of records.
    Returns a list of tuples (field_path, sql_column_name).
    """
    if threshold is None:
        # Use different thresholds based on dataset type
        if dataset_name.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD

    if dataset_name not in DATASET_RECORD_COUNTS:
        return []

    total_records = DATASET_RECORD_COUNTS[dataset_name]
    if total_records == 0:
        return []

    threshold_count = total_records * threshold
    common_fields = []

    for field, count in DATASET_FIELDS[dataset_name].items():
        if count >= threshold_count:
            # Create SQL-friendly column name
            column_name = field.replace('.', '_').replace('[', '_').replace(']', '_')
            common_fields.append((field, column_name))

    return sorted(common_fields)


def get_combined_fields_for_dataset_group(dataset_group):
    """
    Creates a combined set of fields from all datasets in a group.
    Returns a list of field info dicts to use for Parquet generation.
    """
    # Identify datasets in the group
    if dataset_group == 'elasticsearchaux':
        datasets = ['metadata', 'journals', 'digital_lending']
    elif dataset_group == 'aac':
        datasets = [d for d in DATASET_RECORD_COUNTS.keys() if d.startswith('aac_')]
    elif dataset_group == 'mariadb':
        datasets = [d for d in DATASET_RECORD_COUNTS.keys() if d.startswith('mariadb_')]
    else:
        return []

    # Get the common fields for each dataset in the group
    all_fields = {}

    for dataset in datasets:
        if dataset not in DATASET_RECORD_COUNTS or DATASET_RECORD_COUNTS[dataset] == 0:
            continue

        # Use threshold based on dataset type
        if dataset.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD

        for field_path, column_name in get_common_fields(dataset, threshold):
            if field_path not in all_fields:
                all_fields[field_path] = {
                    'field': field_path,
                    'column': column_name,
                    'datasets': [dataset]
                }
            else:
                all_fields[field_path]['datasets'].append(dataset)

    return list(all_fields.values())


def find_common_fields_across_datasets(dataset_group):
    """
    Finds fields that are common across all datasets in a group.
    Returns a sorted list of field names.
    """
    # Identify datasets in the group
    if dataset_group == 'elasticsearchaux':
        datasets = ['metadata', 'journals', 'digital_lending']
    elif dataset_group == 'aac':
        datasets = [d for d in DATASET_RECORD_COUNTS.keys() if d.startswith('aac_')]
    elif dataset_group == 'mariadb':
        datasets = [d for d in DATASET_RECORD_COUNTS.keys() if d.startswith('mariadb_')]
    else:
        return []

    # Only consider datasets that have records
    active_datasets = [d for d in datasets if d in DATASET_RECORD_COUNTS and DATASET_RECORD_COUNTS[d] > 0]

    if len(active_datasets) <= 1:
        return []

    # Get common fields for each dataset using appropriate threshold
    dataset_common_fields = {}
    for dataset in active_datasets:
        # Use threshold based on dataset type
        if dataset.startswith('aac_'):
            threshold = AAC_FIELD_PRESENCE_THRESHOLD
        else:
            threshold = FIELD_PRESENCE_THRESHOLD

        fields = set(field for field, _ in get_common_fields(dataset, threshold))
        dataset_common_fields[dataset] = fields

    # Find intersection of all sets
    common_fields = set.intersection(*dataset_common_fields.values()) if dataset_common_fields else set()
    return sorted(common_fields)


def write_field_reports_for_directory(source_dir):
    """Writes reports about field frequencies and common fields for this directory."""
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')
    dir_name = os.path.basename(source_dir)

    # Determine dataset group based on directory
    if 'elasticsearchaux' in dir_name:
        dataset_group = 'elasticsearchaux'
    elif 'elasticsearch' in dir_name and 'aux' not in dir_name:
        dataset_group = 'elasticsearch'
    elif 'aac' in dir_name:
        dataset_group = 'aac'
    elif 'mariadb' in dir_name:
        dataset_group = 'mariadb'
    else:
        dataset_group = dir_name

    # Get combined fields for dataset groups
    combined_fields = []
    if dataset_group in ['elasticsearchaux', 'aac', 'mariadb']:
        combined_fields = get_combined_fields_for_dataset_group(dataset_group)

    # Find fields common across datasets in each group
    common_across_datasets = []
    if dataset_group in ['elasticsearchaux', 'aac', 'mariadb']:
        common_across_datasets = find_common_fields_across_datasets(dataset_group)

    # Write individual dataset reports
    for dataset_name in DATASET_FIELDS.keys():
        report_path = os.path.join(reports_dir, f"{dataset_group}_{dataset_name}_fields_report.txt")
        with open(report_path, 'w') as f:
            f.write(f"==== Field Analysis Report for Dataset: {dataset_name} in {dataset_group} ====\n\n")

            # Get total records safely with a default of 0
            total_records = DATASET_RECORD_COUNTS.get(dataset_name, 0)

            # Special handling for MariaDB datasets with zero records
            is_mariadb = dataset_group == 'mariadb' or dataset_name.startswith('mariadb_')

            if is_mariadb and total_records == 0:
                # For MariaDB with zero records, just list the columns without percentages
                f.write("This MariaDB dataset has no record count information. Only showing column names.\n\n")
                f.write(f"Total Columns Found: {len(DATASET_FIELDS[dataset_name])}\n\n")
                f.write("=== Columns in this dataset ===\n")
                for field in sorted(DATASET_FIELDS[dataset_name].keys()):
                    f.write(f"- {field}\n")
            else:
                # Normal processing for datasets with record counts
                f.write(
                    f"NOTICE: Analysis used {FILE_SAMPLE_RATE * 100:.1f}% file sampling with {1.0 / FILE_SAMPLE_RATE:.1f}x scaling for estimation\n\n")
                f.write(f"Estimated Total Records: {total_records}\n")
                f.write(f"Total Unique Fields Found: {len(DATASET_FIELDS[dataset_name])}\n\n")

                # Use threshold based on dataset type
                threshold = AAC_FIELD_PRESENCE_THRESHOLD if dataset_name.startswith(
                    'aac_') else FIELD_PRESENCE_THRESHOLD
                threshold_percent = threshold * 100

                common_fields = get_common_fields(dataset_name, threshold)
                f.write(f"=== Fields present in {threshold_percent}%+ of records ({len(common_fields)} fields) ===\n")

                # Only calculate percentages if total_records > 0
                if total_records > 0:
                    for field, column_name in common_fields:
                        presence = DATASET_FIELDS[dataset_name][field] / total_records * 100
                        f.write(f"- {field} → {column_name}: {presence:.2f}%\n")
                else:
                    # This should not happen for non-MariaDB datasets
                    f.write("- No record count available to calculate percentages\n")

                f.write("\n")

                f.write(f"=== All Fields by Presence Percentage ===\n")
                # Again, only calculate percentages if total_records > 0
                if total_records > 0:
                    sorted_fields = sorted(DATASET_FIELDS[dataset_name].items(), key=lambda x: x[1], reverse=True)
                    for field, count in sorted_fields:
                        percentage = (count / total_records) * 100
                        f.write(f"- {field}: {percentage:.2f}% ({count}/{total_records} records)\n")
                else:
                    # This should not happen for non-MariaDB datasets
                    for field in sorted(DATASET_FIELDS[dataset_name].keys()):
                        f.write(f"- {field}: count unknown (no record count available)\n")

        logging.info(f"Wrote field analysis report for {dataset_name} to {report_path}")

    # Write report of fields common across each dataset group
    if common_across_datasets:
        common_fields_report = os.path.join(reports_dir, f"common_fields_across_{dataset_group}.txt")
        with open(common_fields_report, 'w') as f:
            f.write(f"==== Fields Common Across All {dataset_group} Datasets ====\n\n")
            f.write(
                f"NOTICE: Analysis used {FILE_SAMPLE_RATE * 100:.1f}% file sampling with {1.0 / FILE_SAMPLE_RATE:.1f}x scaling for estimation\n\n")
            f.write(
                f"Found {len(common_across_datasets)} fields that appear in a majority of records in all datasets:\n\n")
            for field in common_across_datasets:
                f.write(f"- {field}\n")

        logging.info(f"Wrote common fields report to {common_fields_report}")

    # Write report of combined fields for each dataset group
    if combined_fields:
        combined_fields_report = os.path.join(reports_dir, f"combined_{dataset_group}_fields.txt")
        with open(combined_fields_report, 'w') as f:
            f.write(f"==== Combined Fields for {dataset_group} Datasets ====\n\n")
            f.write(
                f"NOTICE: Analysis used {FILE_SAMPLE_RATE * 100:.1f}% file sampling with {1.0 / FILE_SAMPLE_RATE:.1f}x scaling for estimation\n\n")
            f.write(f"Total combined fields: {len(combined_fields)}\n\n")

            # Group fields by number of datasets
            by_dataset_count = defaultdict(list)
            for field_info in combined_fields:
                by_dataset_count[len(field_info['datasets'])].append(field_info)

            for count in sorted(by_dataset_count.keys(), reverse=True):
                f.write(f"\n=== Fields present in {count} dataset(s) ===\n")
                for field_info in sorted(by_dataset_count[count], key=lambda x: x['field']):
                    datasets_str = ', '.join(field_info['datasets'])
                    f.write(f"- {field_info['field']} → {field_info['column']} (in: {datasets_str})\n")

        logging.info(f"Wrote combined {dataset_group} fields report to {combined_fields_report}")

    # Save metadata for later processing
    metadata_path = os.path.join(reports_dir, f"{dataset_group}_fields_metadata.json")
    with open(metadata_path, 'w') as f:
        metadata = {
            'common_fields': {
                k: [{"field": f_field, "column": c} for f_field, c in get_common_fields(k)]
                for k in DATASET_FIELDS.keys()
            },
            'record_counts': dict(DATASET_RECORD_COUNTS),
            'analysis_method': f"{FILE_SAMPLE_RATE * 100:.1f}% file sampling by dataset with {1.0 / FILE_SAMPLE_RATE:.1f}x scaling",
            'field_presence_threshold': {
                'elasticsearch': FIELD_PRESENCE_THRESHOLD,
                'aac': AAC_FIELD_PRESENCE_THRESHOLD
            }
        }

        if dataset_group == 'elasticsearchaux':
            metadata['combined_elasticsearchaux_fields'] = combined_fields
            metadata['elasticsearchaux_common_fields'] = [{"field": f} for f in common_across_datasets]
        elif dataset_group == 'aac':
            metadata['combined_aac_fields'] = combined_fields
            metadata['aac_common_fields'] = [{"field": f} for f in common_across_datasets]
        elif dataset_group == 'mariadb':
            metadata['combined_mariadb_fields'] = combined_fields
            metadata['mariadb_common_fields'] = [{"field": f} for f in common_across_datasets]

        json.dump(metadata, f, indent=2)

    logging.info(f"Wrote dataset fields metadata to {metadata_path}")


def convert_csv_to_parquet(split_file, dataset_name, parquet_path, json_fields_to_use=None):
    """
    Converts a CSV/TSV file to Parquet format, first attempting the full file.
    If memory errors occur, falls back to progressively smaller chunks.
    """
    logging.info(f"Converting {os.path.basename(split_file)} to {os.path.basename(parquet_path)}")

    # Determine if it's a MariaDB file from filename
    is_mariadb = 'allthethings' in split_file or dataset_name.startswith('mariadb_')

    # First try: Process the entire file with different strategies
    if try_whole_file_conversion(split_file, parquet_path, is_mariadb):
        return True

    # If whole file processing failed with memory errors, try chunking approach
    logging.info(f"Whole file conversion failed, trying chunked approach for {os.path.basename(split_file)}")
    return try_chunked_conversion(split_file, parquet_path, is_mariadb)


def try_whole_file_conversion(split_file, parquet_path, is_mariadb):
    """Try to convert the entire file at once with various strategies."""
    # DuckDB connection
    con = duckdb.connect()
    try:
        # Configure DuckDB
        con.execute("PRAGMA memory_limit='28GB'")
        con.execute("PRAGMA threads=8")

        # Strategy 1: Auto-detection
        try:
            logging.info(f"Trying binary auto-detection strategy")
            sql = f"""
            COPY (
                SELECT * FROM read_csv_auto(
                    '{split_file}',
                    all_varchar=true,
                    ignore_errors=true,
                    sample_size=2000,
                    max_line_size=50000000,
                    auto_detect=true
                )
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            con.execute(sql)

            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(
                f"Successfully converted with auto-detection: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True
        except Exception as e:
            # Check if it's a memory error
            if "Out of Memory Error" in str(e):
                logging.warning(f"Auto-detection failed due to memory limit: {e}")
                return False  # Return false to trigger chunked processing
            logging.warning(f"Auto-detection strategy failed: {e}")

        # Strategy 2: Tab delimiter
        try:
            logging.info(f"Trying tab delimiter")
            sql = f"""
            COPY (
                SELECT * FROM read_csv(
                    '{split_file}',
                    delim='\t',
                    header=true,
                    quote='',
                    all_varchar=true,
                    ignore_errors=true,
                    null_padding=true,
                    strict_mode=false,
                    max_line_size=50000000
                )
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            con.execute(sql)

            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(
                f"Successfully converted with tab delimiter: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True
        except Exception as e:
            if "Out of Memory Error" in str(e):
                logging.warning(f"Tab delimiter strategy failed due to memory limit: {e}")
                return False  # Return false to trigger chunked processing
            logging.warning(f"Tab delimiter strategy failed: {e}")

        # Strategy 3: Comma delimiter
        try:
            logging.info(f"Trying comma delimiter")
            sql = f"""
            COPY (
                SELECT * FROM read_csv(
                    '{split_file}',
                    delim=',',
                    header=true,
                    quote='"',
                    escape='"',
                    all_varchar=true,
                    ignore_errors=true,
                    null_padding=true,
                    strict_mode=false,
                    max_line_size=50000000
                )
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            con.execute(sql)

            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(
                f"Successfully converted with comma delimiter: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True
        except Exception as e:
            if "Out of Memory Error" in str(e):
                logging.warning(f"Comma delimiter strategy failed due to memory limit: {e}")
                return False  # Return false to trigger chunked processing
            logging.warning(f"Comma delimiter strategy failed: {e}")

        # If we reach here, all strategies failed but not due to memory errors
        con.close()
        return False

    except Exception as e:
        logging.error(f"Critical error during whole file conversion: {e}")
        try:
            con.close()
        except:
            pass
        return False


def try_chunked_conversion(split_file, parquet_path, is_mariadb):
    """Convert file by processing it in chunks to avoid memory issues."""
    # Create a temporary directory for chunks
    temp_dir = os.path.dirname(parquet_path) + "/temp_chunks"
    os.makedirs(temp_dir, exist_ok=True)

    # Try with progressively smaller chunk sizes
    chunk_sizes = [500000, 100000, 50000, 10000]

    # Default delimiter guesses based on file type
    delimiters = ['\t'] if is_mariadb else [',', '\t']

    # Try each chunk size
    for chunk_size in chunk_sizes:
        logging.info(f"Attempting chunked conversion with chunk size: {chunk_size:,} rows")

        # Try each delimiter with current chunk size
        for delimiter in delimiters:
            logging.info(f"Trying delimiter '{delimiter}' with chunk size {chunk_size:,}")

            # Create a unique directory for this attempt's chunks
            chunks_dir = f"{temp_dir}/{delimiter.replace('|', 'pipe')}_{chunk_size}"
            os.makedirs(chunks_dir, exist_ok=True)

            chunk_files = []
            success = True

            try:
                # Connection for chunking operations
                con = duckdb.connect()
                con.execute("PRAGMA memory_limit='10GB'")  # Use lower memory for chunking
                con.execute("PRAGMA threads=4")

                # Get header first
                header_sql = f"""
                SELECT * FROM read_csv(
                    '{split_file}', 
                    delim='{delimiter}', 
                    header=true, 
                    sample_size=1
                ) LIMIT 0;
                """
                header_result = con.execute(header_sql).fetch_arrow_table()
                column_names = header_result.column_names

                # Process in chunks
                offset = 0
                chunk_idx = 0

                while True:
                    chunk_path = f"{chunks_dir}/chunk_{chunk_idx}.parquet"
                    chunk_files.append(chunk_path)

                    # Extract a chunk with OFFSET and LIMIT
                    chunk_sql = f"""
                    COPY (
                        SELECT * FROM read_csv(
                            '{split_file}',
                            delim='{delimiter}',
                            header=true,
                            quote='',
                            all_varchar=true,
                            ignore_errors=true,
                            skip={offset},
                            max_line_size=10000000
                        ) LIMIT {chunk_size}
                    ) TO '{chunk_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
                    """

                    try:
                        con.execute(chunk_sql)
                        # Check if we got any rows in this chunk
                        rows_in_chunk = con.execute(f"SELECT COUNT(*) FROM '{chunk_path}'").fetchone()[0]

                        if rows_in_chunk == 0:
                            # No more rows to process
                            os.remove(chunk_path)  # Remove empty chunk
                            chunk_files.pop()  # Remove from list
                            break

                        logging.info(f"Processed chunk {chunk_idx} with {rows_in_chunk} rows")
                        offset += rows_in_chunk
                        chunk_idx += 1

                    except Exception as e:
                        logging.error(f"Error processing chunk {chunk_idx}: {e}")
                        success = False
                        break

                # Merge all chunks if successful
                if success and chunk_files:
                    logging.info(f"Merging {len(chunk_files)} chunks into final parquet file")

                    # Generate SQL for merging chunks
                    file_list = ", ".join([f"'{f}'" for f in chunk_files])
                    merge_sql = f"""
                    COPY (
                        SELECT * FROM read_parquet([{file_list}])
                    ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
                    """

                    try:
                        con.execute(merge_sql)
                        rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
                        logging.info(
                            f"Successfully merged chunks: {os.path.basename(parquet_path)} ({rows_count} rows)")

                        # Clean up chunks
                        for chunk in chunk_files:
                            if os.path.exists(chunk):
                                os.remove(chunk)

                        # Clean up chunk directory
                        try:
                            os.rmdir(chunks_dir)
                        except:
                            pass

                        con.close()
                        return True

                    except Exception as e:
                        logging.error(f"Error merging chunks: {e}")
                        success = False

                con.close()

                # If this delimiter and chunk size worked, return success
                if success:
                    return True

            except Exception as e:
                logging.error(
                    f"Error in chunked conversion with delimiter '{delimiter}' and chunk size {chunk_size}: {e}")

                # Clean up any partial chunks
                for chunk in chunk_files:
                    if os.path.exists(chunk):
                        os.remove(chunk)

    # If we get here, all chunking strategies failed
    logging.error(f"All chunked conversion strategies failed for {os.path.basename(split_file)}")

    # Try pandas as a last resort (which may handle the file differently)
    try:
        logging.info(f"Trying pandas conversion as last resort")
        import pandas as pd

        # Detect delimiter
        with open(split_file, 'rb') as f:
            # Read first few lines to detect delimiter
            sample = f.read(4096).decode('utf-8', errors='replace')

        if '\t' in sample:
            delimiter = '\t'
        elif ',' in sample:
            delimiter = ','
        else:
            delimiter = '\t' if is_mariadb else ','

        # Process in chunks with pandas
        chunk_files = []
        chunk_idx = 0
        chunks_dir = f"{temp_dir}/pandas_chunks"
        os.makedirs(chunks_dir, exist_ok=True)

        for chunk in pd.read_csv(split_file, sep=delimiter, chunksize=50000,
                                 quoting=3, engine='python', on_bad_lines='skip',
                                 encoding_errors='replace', low_memory=False, dtype=str):
            chunk_path = f"{chunks_dir}/chunk_{chunk_idx}.parquet"
            chunk_files.append(chunk_path)
            chunk.to_parquet(chunk_path, compression='zstd')
            chunk_idx += 1

        # Merge all chunks using DuckDB
        if chunk_files:
            con = duckdb.connect()
            file_list = ", ".join([f"'{f}'" for f in chunk_files])
            merge_sql = f"""
            COPY (
                SELECT * FROM read_parquet([{file_list}])
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """

            con.execute(merge_sql)
            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(
                f"Successfully converted with pandas chunking: {os.path.basename(parquet_path)} ({rows_count} rows)")

            # Clean up temporary files
            for chunk in chunk_files:
                if os.path.exists(chunk):
                    os.remove(chunk)

            con.close()
            return True

    except Exception as e:
        logging.error(f"Pandas conversion failed: {e}")

    return False


def convert_json_to_parquet(split_file, dataset_name, parquet_path, fields_to_use):
    """
    Convert a single JSON split file to Parquet format.
    This function runs in a worker thread.
    """
    logging.info(f"Converting {os.path.basename(split_file)} to {os.path.basename(parquet_path)}")

    con = duckdb.connect()
    con.execute("PRAGMA memory_limit='28GB'")
    con.execute("PRAGMA threads=1")
    try:
        # Increase limits for large objects and memory
        if dataset_name.startswith('aac_'):
            # Para AAC: extraer aacid como campo principal y luego campos comunes de metadata
            field_extractions = ["\"aacid\" AS \"aacid\"",
                                 "CAST(\"metadata\" AS VARCHAR) AS \"metadata\""]

            # Extract fields from metadata (just like we do with _source in elasticsearch)
            for field_info in fields_to_use:
                field_path = field_info["field"]
                column_name = field_info["column"]

                # Skip the main field
                if field_path == 'aacid' or field_path == 'metadata':
                    continue

                # For fields inside metadata
                if '.' in field_path:
                    parts = field_path.split('.')
                    if parts[0] == 'metadata':
                        parts = parts[1:]  # Skip 'metadata' prefix

                    # Build path for JSON extraction
                    json_path = '$'
                    for part in parts:
                        json_path += f'.{part}'

                    field_extractions.append(f"json_extract_string(\"metadata\", '{json_path}') AS \"{column_name}\"")

            # Create SQL statement
            sql_fields = ", ".join(field_extractions)

            # Add ignore_errors=true to handle duplicate fields
            conversion_sql = f"""
            COPY (
                SELECT {sql_fields}
                FROM read_json('{split_file}', ignore_errors=true, maximum_object_size=104857600)
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """

        else:
            # Para Elasticsearch: extraer _index, _id, _score y campos comunes de _source
            field_extractions = ["_index AS \"_index\"", "_id AS \"_id\"", "_score AS \"_score\""]

            # Extract fields from _source
            for field_info in fields_to_use:
                field_path = field_info["field"]
                column_name = field_info["column"]

                # Skip main fields
                if field_path in ['_index', '_id', '_score']:
                    continue

                # For fields inside _source
                if '.' in field_path:
                    parts = field_path.split('.')
                    if parts[0] == '_source':
                        parts = parts[1:]  # Skip '_source' prefix

                    # Build path for JSON extraction
                    json_path = '$'
                    for part in parts:
                        json_path += f'.{part}'

                    field_extractions.append(f"json_extract_string(_source, '{json_path}') AS \"{column_name}\"")

            # Create SQL statement
            sql_fields = ", ".join(field_extractions)

            # Add ignore_errors=true consistently
            conversion_sql = f"""
            COPY (
                SELECT {sql_fields}
                FROM read_json('{split_file}', ignore_errors=true, maximum_object_size=104857600)
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """

        con.execute(conversion_sql)
        rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
        logging.info(f"Saved: {os.path.basename(parquet_path)} ({rows_count} rows)")
        con.close()
        return True

    except Exception as e:
        logging.error(f"Error converting {os.path.basename(split_file)}: {e}")
        try:
            # Si falla, intentar con read_json_auto como último recurso
            auto_sql = f"""
            COPY (
                SELECT *
                FROM read_json_auto('{split_file}', format='auto', ignore_errors=true, maximum_object_size=104857600 )
            ) TO '{parquet_path}' (FORMAT 'parquet', COMPRESSION 'zstd');
            """
            con.execute(auto_sql)
            rows_count = con.execute(f"SELECT COUNT(*) FROM '{parquet_path}'").fetchone()[0]
            logging.info(f"Saved with auto-detection: {os.path.basename(parquet_path)} ({rows_count} rows)")
            con.close()
            return True

        except Exception as final_e:
            logging.error(f"Final attempt failed for {os.path.basename(split_file)}: {final_e}")
            try:
                con.close()
            except:
                pass
            return False


def convert_to_parquet(dataset_groups, target_dir, file_type='json'):
    """Convert all split files to Parquet for all datasets."""
    error_dir = os.path.join(SCRIPT_DIR, 'data', 'error')
    reports_dir = os.path.join(SCRIPT_DIR, 'reports')

    # Determine directory type
    dir_name = os.path.basename(target_dir)
    if 'elasticsearchaux' in dir_name:
        dataset_group = 'elasticsearchaux'
        max_workers = MAX_WORKERS_CONVERSION_ELASTICSEARCHAUX
    elif 'elasticsearch' in dir_name and 'aux' not in dir_name:
        dataset_group = 'elasticsearch'
        max_workers = MAX_WORKERS_CONVERSION_ELASTICSEARCH
    elif 'aac' in dir_name:
        dataset_group = 'aac'
        max_workers = MAX_WORKERS_CONVERSION_AAC
    elif 'mariadb' in dir_name:
        dataset_group = 'mariadb'
        max_workers = MAX_WORKERS_CONVERSION_MARIADB
    else:
        dataset_group = dir_name
        max_workers = 1

    # Get combined fields for dataset group
    combined_elasticsearchaux_fields = []
    combined_aac_fields = []
    combined_mariadb_fields = []

    if dataset_group == 'elasticsearchaux':
        combined_elasticsearchaux_fields = get_combined_fields_for_dataset_group('elasticsearchaux')
    elif dataset_group == 'aac':
        combined_aac_fields = get_combined_fields_for_dataset_group('aac')
    elif dataset_group == 'mariadb':
        combined_mariadb_fields = get_combined_fields_for_dataset_group('mariadb')

    logging.info(f"Converting all files to Parquet for {dataset_group}...")
    os.makedirs(error_dir, exist_ok=True)

    # Process each dataset
    for dataset_name, split_files in dataset_groups.items():
        logging.info(f"Converting {len(split_files)} split files for dataset {dataset_name}")

        # Determine which fields to use
        if dataset_group == 'elasticsearchaux' and dataset_name in ['metadata', 'journals', 'digital_lending']:
            # For elasticsearchaux, use the combined fields from all datasets
            fields_to_use = combined_elasticsearchaux_fields
            logging.info(f"Using {len(fields_to_use)} combined fields for elasticsearchaux")
        elif dataset_group == 'elasticsearch' and dataset_name == 'records':
            # For main elasticsearch (which only has 'records' dataset), use dataset-specific fields
            fields_to_use = [{"field": field, "column": column} for field, column in
                             get_common_fields(dataset_name, FIELD_PRESENCE_THRESHOLD)]
            logging.info(f"Using {len(fields_to_use)} individual common fields for main elasticsearch records")
        elif dataset_group == 'aac' and dataset_name.startswith('aac_'):
            # For AAC, use only dataset-specific fields, not combined fields
            fields_to_use = [{"field": field, "column": column} for field, column in
                             get_common_fields(dataset_name, AAC_FIELD_PRESENCE_THRESHOLD)]
            logging.info(
                f"Using {len(fields_to_use)} individual common fields for {dataset_name} (NOT using combined fields)")
        elif dataset_group == 'mariadb' and dataset_name.startswith('mariadb_'):
            # For MariaDB, use dataset-specific fields
            fields_to_use = [{"field": field, "column": column} for field, column in
                             get_common_fields(dataset_name, FIELD_PRESENCE_THRESHOLD)]
            logging.info(f"Using {len(fields_to_use)} common fields for MariaDB dataset {dataset_name}")
        else:
            # Use threshold based on dataset type
            threshold = AAC_FIELD_PRESENCE_THRESHOLD if dataset_name.startswith('aac_') else FIELD_PRESENCE_THRESHOLD
            fields_to_use = [{"field": field, "column": column} for field, column in
                             get_common_fields(dataset_name, threshold)]
            logging.info(f"Using {len(fields_to_use)} common fields for {dataset_name}")

        # Group split files by original file
        split_groups = defaultdict(list)
        for split_file in split_files:
            base_name = os.path.basename(split_file)
            # Handle nested recursive parts
            if '_part__part_' in base_name:
                # This is a recursively split part, get the original root filename
                original_file = base_name.split('_part_')[0]
            else:
                original_file = base_name.split('_part_')[0]

            split_groups[original_file].append(split_file)

        # Process each group of split files
        with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
            for original_file, group_splits in split_groups.items():
                if not group_splits:
                    continue

                # Convert each split file to Parquet
                futures = []
                created_parquet_files = []

                for i, split_file in enumerate(sorted(group_splits), 1):
                    parquet_name = f"{dataset_name}_{original_file}_{i}.parquet"
                    parquet_path = os.path.join(target_dir, parquet_name)

                    # Submit conversion task based on file type
                    if file_type == 'csv':
                        future = executor.submit(
                            convert_csv_to_parquet,
                            split_file,
                            dataset_name,
                            parquet_path,
                            fields_to_use
                        )
                    else:
                        future = executor.submit(
                            convert_json_to_parquet,
                            split_file,
                            dataset_name,
                            parquet_path,
                            fields_to_use
                        )
                    futures.append((future, split_file, parquet_path))

                # Process results as they complete
                all_successful = True
                for future, split_file, parquet_path in futures:
                    try:
                        success = future.result()
                        if success:
                            created_parquet_files.append(parquet_path)
                        else:
                            all_successful = False
                    except Exception as e:
                        logging.error(f"Error in conversion task for {os.path.basename(split_file)}: {e}")
                        all_successful = False

                # Check if all conversions were successful
                if not all_successful:
                    logging.error(f"Some conversions failed for {original_file}, cleaning up")
                    # Delete all created Parquet files
                    for parquet_path in created_parquet_files:
                        try:
                            if os.path.exists(parquet_path):
                                os.remove(parquet_path)
                                logging.info(f"Deleted parquet file due to errors: {os.path.basename(parquet_path)}")
                        except Exception as e:
                            logging.warning(f"Could not delete parquet file: {e}")

                    # Move split files to error directory
                    for _, split_file, _ in futures:
                        if os.path.exists(split_file):
                            error_path = os.path.join(error_dir, os.path.basename(split_file))
                            try:
                                shutil.move(split_file, error_path)
                                logging.info(f"Moved split file to error directory: {os.path.basename(split_file)}")
                            except Exception as e:
                                logging.warning(f"Could not move split file to error directory: {e}")
                else:
                    # Clean up split files after successful conversion
                    for _, split_file, _ in futures:
                        try:
                            if os.path.exists(split_file):
                                os.remove(split_file)
                                logging.info(f"Deleted split file: {os.path.basename(split_file)}")
                        except Exception as e:
                            logging.warning(f"Could not delete split file: {e}")


def process_mini_batch(batch_files, prefix, mega_batch_idx, mini_batch_idx, temp_dir):
    """Process a mini batch of files and save to temporary Parquet file."""
    try:
        start_time = time.time()

        # Temporary file for this mini-batch
        temp_output = f"{temp_dir}/temp_{mini_batch_idx}.parquet"

        # Connect to DuckDB
        conn = duckdb.connect(database=':memory:')

        # Performance settings for DuckDB
        conn.execute("PRAGMA memory_limit='30GB'")
        conn.execute("PRAGMA threads=16")

        # Process the files in this mini-batch more efficiently
        # Use a single SQL operation with UNION ALL
        sql_parts = []
        for file in batch_files:
            sql_parts.append(
                f"SELECT * FROM '{file}'")  # No need to exclude _source since it's already excluded in conversion

        sql_query = " UNION ALL ".join(sql_parts)

        # Write directly without creating intermediate table
        conn.execute(f"""
        COPY ({sql_query}) TO '{temp_output}' (FORMAT PARQUET, COMPRESSION 'zstd')
        """)

        # Close connection to free memory
        conn.close()

        # Force garbage collection
        gc.collect()

        duration = time.time() - start_time
        logger.info(f"Mini-batch {mini_batch_idx} for prefix {prefix} processed in {duration:.2f} seconds")

        return temp_output
    except Exception as e:
        logger.error(f"Error processing mini-batch {mini_batch_idx}: {str(e)}")
        return None


def get_base_filename(file_path):
    """
    Extract base filename for grouping when merging, preserving the original filename format.
    For example: 'metadata_journals_1.parquet' -> 'metadata_journals'
    """
    filename = os.path.basename(file_path)
    # Remove the extension
    base_name = os.path.splitext(filename)[0]
    # Remove the last part after the last underscore (which is the part number)
    if '_' in base_name:
        # Split by underscore and get all parts except the last one
        parts = base_name.rsplit('_', 1)
        if parts[1].isdigit():  # If last part is a number, remove it
            return parts[0]
    return base_name  # Return as is if no trailing number


def merge_parquet_files_for_directory(config):
    """
    Merge individual Parquet files into consolidated files by prefix group.
    This improves query performance by reducing the number of files.
    """
    source_dir = config['target_dir']  # Use target_dir as source for merge
    file_type = config.get('file_type', 'json')  # Default to JSON

    # Determine the final output directory
    if 'elasticsearchaux' in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'elasticsearchauxF')
        file_pattern = "*.parquet"
    elif 'elasticsearch' in source_dir and 'aux' not in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'elasticsearchF')
        file_pattern = "*.parquet"
    elif 'aac' in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'aacF')
        file_pattern = "*.parquet"
    elif 'mariadb' in source_dir:
        output_dir = os.path.join(SCRIPT_DIR, 'data', 'mariadbF')
        file_pattern = "*.parquet"
    else:
        logging.info(f"Don't know how to merge files for {source_dir}, skipping merge")
        return

    logger.info(f"Starting Parquet file merging process for {source_dir} to {output_dir}")

    # Create output directory if it doesn't exist
    os.makedirs(output_dir, exist_ok=True)

    # Get all Parquet files matching the pattern
    parquet_files = glob.glob(os.path.join(source_dir, file_pattern))
    if not parquet_files:
        # Try with a more general pattern if the specific pattern doesn't match
        parquet_files = glob.glob(os.path.join(source_dir, "*.parquet"))

    logger.info(f"Found {len(parquet_files)} Parquet files in {source_dir} to merge")

    if not parquet_files:
        logger.info(f"No files to process in {source_dir}, skipping merge")
        return

    # Group files by their base filename (excluding part numbers)
    file_groups = defaultdict(list)

    for file in parquet_files:
        base_name = get_base_filename(file)
        file_groups[base_name].append(file)

    logger.info(f"Grouped files into {len(file_groups)} base name groups for {source_dir}")

    # Calculate the total number of files to process
    total_files = sum(len(files) for files in file_groups.values())
    processed_files = 0

    # Number of workers for parallelization
    num_workers = MAX_WORKERS_MERGE  # Process mini-batches in parallel

    # Process each group
    for base_name, files in sorted(file_groups.items()):
        logger.info(f"Processing file group {base_name} with {len(files)} files from {source_dir}")

        total_mega_batches = (len(files) + mega_batch_size - 1) // mega_batch_size

        for mega_batch_idx in range(total_mega_batches):
            mega_start_idx = mega_batch_idx * mega_batch_size
            mega_end_idx = min((mega_batch_idx + 1) * mega_batch_size, len(files))
            mega_batch_files = files[mega_start_idx:mega_end_idx]

            # Directory for temporary files
            temp_dir = f"{output_dir}/temp_{base_name}_{mega_batch_idx}"
            os.makedirs(temp_dir, exist_ok=True)

            # Final output file for this mega-batch - with batch number
            batch_num = mega_batch_idx + 1

            # Keep original name format, just add the batch number
            normalized_base_name = normalize_output_filename(base_name)
            final_output = f"{output_dir}/{normalized_base_name}_{batch_num}.parquet"
            logging.info(f"Using normalized base name: {normalized_base_name} (original: {base_name})")


            logger.info(
                f"Processing mega-batch {batch_num}/{total_mega_batches} for {base_name} ({mega_end_idx - mega_start_idx} files)")

            # Process in mini-batches of 100 files
            mini_batch_size = 100

            # Prepare mini-batches
            mini_batches = []
            for i in range(0, len(mega_batch_files), mini_batch_size):
                end_idx = min(i + mini_batch_size, len(mega_batch_files))
                mini_batches.append(mega_batch_files[i:end_idx])

            # Create a counter for completed mini-batches
            completed_mini_batches = 0
            total_mini_batches = len(mini_batches)

            # Process mini-batches in parallel
            temp_outputs = []
            with concurrent.futures.ProcessPoolExecutor(max_workers=num_workers) as executor:
                futures = {}
                for i, mini_batch in enumerate(mini_batches):
                    future = executor.submit(
                        process_mini_batch,
                        mini_batch,
                        base_name,
                        mega_batch_idx,
                        i,
                        temp_dir
                    )
                    futures[future] = len(mini_batch)

                # Collect results as they complete
                for future in concurrent.futures.as_completed(futures):
                    result = future.result()
                    if result:
                        temp_outputs.append(result)

                    # Update processed count
                    batch_size = futures[future]
                    processed_files += batch_size
                    completed_mini_batches += 1

                    # Show progress percentage
                    completion = processed_files / total_files * 100
                    mega_completion = completed_mini_batches / total_mini_batches * 100
                    logger.info(f"Progress: {processed_files}/{total_files} files ({completion:.2f}%), "
                                f"Mega-batch: {completed_mini_batches}/{total_mini_batches} mini-batches ({mega_completion:.2f}%)")

            # Combine all temporary files into one (sequentially)
            if temp_outputs:
                logger.info(f"Combining {len(temp_outputs)} temporary files into final output: {final_output}")

                conn = duckdb.connect(database=':memory:')
                conn.execute("PRAGMA memory_limit='30GB'")
                conn.execute("PRAGMA threads=16")

                # Create a file list string for SQL
                file_list = ", ".join([f"'{f}'" for f in temp_outputs])

                # Combine and write to final file
                conn.execute(f"""
                COPY (
                    SELECT * FROM read_parquet([{file_list}])
                ) TO '{final_output}' (FORMAT PARQUET, COMPRESSION 'zstd')
                """)

                conn.close()

                # Clean up temporary files if successful
                for temp_file in temp_outputs:
                    if os.path.exists(temp_file):
                        os.remove(temp_file)

                # Clean up original Parquet files that have been merged
                for orig_file in mega_batch_files:
                    try:
                        if os.path.exists(orig_file):
                            os.remove(orig_file)
                            logger.info(f"Deleted original Parquet file: {os.path.basename(orig_file)}")
                    except Exception as e:
                        logger.warning(f"Could not delete original Parquet file: {e}")

                # Remove temporary directory if empty
                try:
                    if not os.listdir(temp_dir):
                        os.rmdir(temp_dir)
                except Exception as e:
                    logger.warning(f"Could not remove temp directory: {str(e)}")

                logger.info(f"Successfully merged mega-batch {batch_num} to {final_output}")
            else:
                logger.error(f"No temporary files were created for mega-batch {batch_num}")

        logger.info(f"Completed processing all batches for {base_name} in {source_dir}")

    logger.info(f"Parquet file merging process completed for {source_dir}")


def cleanup_temp_files(json_dir, parts_dir):
    """Cleanup temporary decompressed and split files"""
    logging.info(f"Cleaning up temporary files in {json_dir} and {parts_dir}")

    # Clean up JSON files and DAT files
    temp_files = glob.glob(os.path.join(json_dir, "*.json")) + \
                 glob.glob(os.path.join(json_dir, "*.jsonl")) + \
                 glob.glob(os.path.join(json_dir, "*.dat"))
    for f in temp_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.warning(f"Failed to delete {f}: {e}")

    # Clean up split files
    split_files = glob.glob(os.path.join(parts_dir, "*_part_*"))
    for f in split_files:
        try:
            os.remove(f)
        except Exception as e:
            logging.warning(f"Failed to delete {f}: {e}")

    logging.info("Temporary files cleanup completed")


def normalize_output_filename(base_name):
    """
    Normalize output filenames based on the pattern.

    The function handles several patterns:
    1. elasticsearch: other_aarecords_* -> aarecords_*
    2. elasticsearchaux: metadata_aarecords_metadata_* -> aarecords_metadata_*
    3. aac: aac_*_annas_archive_meta__aacid__*_* -> aac_*
    4. mariadb: mariadb_*_allthethings.* -> mariadb_*
    """
    # For elasticsearch ('other' prefix removal)
    if base_name.startswith('other_aarecords'):
        return base_name.replace('other_', '')

    # For elasticsearchaux dataset name deduplication
    for prefix in ['metadata_', 'journals_', 'digital_lending_']:
        if base_name.startswith(prefix) and prefix.rstrip('_') in base_name[len(prefix):]:
            return base_name[len(prefix):]

    # For AAC files
    if base_name.startswith('aac_'):
        parts = base_name.split('_', 2)
        if len(parts) >= 2:
            dataset_type = parts[1]
            # If dataset name is followed by redundant information, simplify
            if '__aacid__' in base_name and dataset_type in base_name.split('__aacid__')[1]:
                return f"aac_{dataset_type}"
            # For other AAC files, use a cleaner name
            return f"aac_{dataset_type}"

    # For MariaDB general pattern - extract the table name cleanly
    if base_name.startswith('mariadb_'):
        # Extract table name from allthethings pattern if present
        if 'allthethings.' in base_name:
            table_match = re.search(r'mariadb_(.+?)_allthethings', base_name)
            if table_match:
                table_name = table_match.group(1)
                return f"mariadb_{table_name}"

            # Try alternate pattern extraction
            table_match = re.search(r'mariadb_([^_]+)', base_name)
            if table_match:
                table_name = table_match.group(1)
                return f"mariadb_{table_name}"

        # Handle annas_archive_meta patterns
        if 'annas_archive_meta__aacid__' in base_name:
            # Extract the specific dataset type after '__aacid__'
            meta_parts = base_name.split('__aacid__')
            if len(meta_parts) >= 2:
                dataset_type = meta_parts[1].split('_')[0].split('.')[0]
                return f"mariadb_annas_archive_meta_{dataset_type}"

        # For libgen tables
        if any(libgen_pattern in base_name for libgen_pattern in [
            'libgenli_', 'libgenrs_'
        ]):
            # Extract the specific libgen table type
            libgen_match = re.search(r'mariadb_(libgen(?:li|rs)_[^_\.]+)', base_name)
            if libgen_match:
                return f"mariadb_{libgen_match.group(1)}"

        # For simpler cases, just preserve the first part after mariadb_
        parts = base_name.split('_', 2)
        if len(parts) >= 2:
            return f"mariadb_{parts[1]}"

    # If no pattern matched, return the original
    return base_name

if __name__ == "__main__":
    logging.info("Starting data processing pipeline...")
    create_directories()
    find_and_move_files()

    # Process each main directory completely before moving to the next
    configs = [
        # ElasticsearchAux
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'temp_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearchaux'),
            'compression': 'gzip',
            'file_type': 'json'
        },
        # Elasticsearch
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'temp_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'elasticsearch'),
            'compression': 'gzip',
            'file_type': 'json'
        },
        # AAC
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'temp_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_json'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'aac_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'aac'),
            'compression': 'zstd',
            'file_type': 'json'
        },
        # MariaDB (added)
        {
            'source_dir': os.path.join(SCRIPT_DIR, 'data', 'mariadb'),
            'temp_dir': os.path.join(SCRIPT_DIR, 'data', 'mariadb_dat'),
            'parts_dir': os.path.join(SCRIPT_DIR, 'data', 'mariadb_parts'),
            'target_dir': os.path.join(SCRIPT_DIR, 'data', 'mariadb'),
            'compression': 'gzip',
            'file_type': 'csv'  # MariaDB files are CSV/TSV format
        }
    ]

    # Process each directory sequentially - full pipeline
    for config in configs:
        logging.info(f"===== Starting full processing pipeline for {config['source_dir']} =====")
        process_directory(config)
        logging.info(f"===== Completed processing for {config['source_dir']} =====\n")

    logging.info("Data processing complete! Reports on common fields are available in the reports directory.")
