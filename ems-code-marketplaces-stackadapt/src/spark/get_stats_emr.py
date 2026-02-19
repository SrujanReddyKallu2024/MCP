"""
EMR-compatible version of get_stats.py for generating statistics from data files.
This version is designed to work with S3 paths via EMR Serverless.

Fixed version that properly handles:
1. Creating CSV on first run with file A1
2. Appending new files (B1, C1, etc.) to existing CSV
3. Avoiding duplicates based on filename
4. Proper error handling for missing files
5. JSON email content generation

Usage:
    spark-submit get_stats_emr.py -if <input_file_path> -m <date_mask>

Arguments:
    -if, --input_file: Path to the input file in S3
    -m, --mask: Date for which the process is run (YYYY-MM-DD)
    -sp, --stats_path: S3 path for statistics output
    -l, --islocal: Flag indicating if file is local (always "False" for EMR)
    -et, --email_to: Email recipients (comma-separated)

Outputs:
    - CSV file with statistics in S3
    - JSON file with email content in S3
    - JSON file with alert data in S3 (if issues detected)
"""

import os
import sys
import functools
import logging
import json
import io
import boto3
from datetime import datetime, timedelta
import argparse
from typing import Dict, List, Optional, Union

from pyspark.sql import SparkSession, DataFrame
import pyspark.sql.functions as F
import pandas as pd
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# Default values (can be overridden with arguments)
# STATS_PATH should be passed from DAG based on environment
STATS_PATH = None  # Will be set from command line argument
EMAIL_TO = "emsactivatealerts@experian.com"

def get_s3_file_metadata(path: str) -> Dict[str, str]:
    """
    Gets metadata for an S3 file
    
    Args:
        path: S3 path to the file
    
    Returns:
        Dictionary with metadata info including creation_date and size
    """
    try:
        # Parse S3 path
        if path.startswith("s3://"):
            path = path[5:]
        elif path.startswith("s3a://"):
            path = path[6:]
        bucket, key = path.split("/", 1)
        
        # Get S3 client
        s3 = boto3.client('s3')
        
        # Get object info
        response = s3.head_object(Bucket=bucket, Key=key)
        
        # Extract metadata
        last_modified = response['LastModified']
        size = response['ContentLength']
        
        return {
            "creation_date": last_modified.strftime("%Y-%m-%d %H:%M:%S"),
            "size": size
        }
    except Exception as e:
        logger.error(f"Error getting S3 file metadata: {str(e)}")
        return {
            "creation_date": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "size": 0
        }

def get_creation_date(path: str) -> str:
    """
    Gets the creation date of a file from S3
    
    Args:
        path: S3 path to the file
    
    Returns:
        Creation date as string in YYYY-MM-DD HH:MM:SS format
    """
    metadata = get_s3_file_metadata(path)
    return metadata["creation_date"]

def format_file_size(size_bytes: int) -> str:
    """
    Format file size from bytes to human readable format (KB, MB, GB, etc.)
    
    Args:
        size_bytes: Size in bytes
        
    Returns:
        Formatted file size string (e.g., '1.23 MB')
    """
    if size_bytes == 0:
        return "0 B"
        
    size_units = ("B", "KB", "MB", "GB", "TB", "PB", "EB", "ZB", "YB")
    i = 0
    while size_bytes >= 1024 and i < len(size_units) - 1:
        size_bytes /= 1024
        i += 1
        
    return f"{size_bytes:.2f} {size_units[i]}"

def get_size(path: str, islocal: str = "False") -> str:
    """
    Gets the size of a file from S3 in formatted string
    
    Args:
        path: S3 path to the file
        islocal: Flag indicating if file is local (ignored for S3)
    
    Returns:
        Formatted file size string
    """
    metadata = get_s3_file_metadata(path)
    return format_file_size(metadata["size"])

def check_path_exists(path: str, islocal: str = "False") -> bool:
    """
    Checks if a path exists in S3
    
    Args:
        path: S3 path to check
        islocal: Flag indicating if path is local (ignored for S3)
    
    Returns:
        True if path exists, False otherwise
    """
    try:
        if path.startswith("s3://"):
            path = path[5:]
        elif path.startswith("s3a://"):
            path = path[6:]
        bucket, key = path.split("/", 1)
        
        s3 = boto3.client('s3')
        try:
            s3.head_object(Bucket=bucket, Key=key)
            return True
        except ClientError:
            return False
    except Exception as e:
        logger.error(f"Error checking path existence: {str(e)}")
        return False

def check_and_read_file(spark: SparkSession, filepath: str, file_ext: str) -> Optional[DataFrame]:
    """
    Checks file extension and reads file into DataFrame
    
    Args:
        spark: SparkSession (ctx in original)
        filepath: Path to the file
        file_ext: File extension
    
    Returns:
        DataFrame or None if file cannot be read
    
    Handles multiple file formats:
    - CSV (with/without compression)
    - TSV (with/without compression)
    - Text files
    - Parquet files
    - Standalone .gz files (tries to infer format)
    """
    try:
        # Check for compressed files first
        full_filename = os.path.basename(filepath).lower()
        
        # Handle CSV files (compressed or not)
        if file_ext.lower() in ['csv']:
            return spark.read.csv(filepath, header=True)
        elif full_filename.endswith('.csv.gz'):
            return spark.read.format('csv').option('header', 'true').load(filepath)
            
        # Handle TSV files (compressed or not)
        elif file_ext.lower() in ['tsv']:
            return spark.read.csv(filepath, header=True, sep='\t')
        elif full_filename.endswith('.tsv.gz'):
            return spark.read.format('csv').option('header', 'true').option('sep', '\t').load(filepath)
            
        # Handle text files
        elif file_ext.lower() in ['txt']:
            return spark.read.text(filepath)
            
        # Handle Parquet files
        elif file_ext.lower() in ['parquet']:
            return spark.read.parquet(filepath)
            
        # Handle standalone .gz files (try to infer the format)
        elif file_ext.lower() in ['gz']:
            if filepath.lower().endswith('.csv.gz'):
                return spark.read.format('csv').option('header', 'true').load(filepath)
            elif filepath.lower().endswith('.tsv.gz'):
                return spark.read.format('csv').option('header', 'true').option('sep', '\t').load(filepath)
            else:
                # Default to CSV for unknown .gz files
                logger.info(f"Trying to read {filepath} as compressed CSV")
                return spark.read.format('csv').option('header', 'true').load(filepath)
        else:
            logger.error(f"Unsupported file extension: {file_ext}")
            return None
    except Exception as e:
        logger.error(f"Error reading file: {str(e)}")
        return None

def get_stats(mask: str, week_start_date: str, input_path: str, df: DataFrame, islocal: str) -> pd.DataFrame:
    """
    Gets the stats based on the ingested files
    Returns pandas dataframe
    
    EXACT same logic as on-premise version
    """
    file_download_datetime = get_creation_date(input_path)
    stats_dict = {
        "date": mask,
        "week_start_date": week_start_date,
        "file_name": os.path.split(input_path)[-1],
        "file_creation_time": file_download_datetime,
        "file_size": get_size(input_path, islocal),
        "record_counts": df.count(),
    }
    return pd.DataFrame([stats_dict])

def save_stats_for_email(stats_bucket: str, stats_df: DataFrame, mask: str, week_start_date: str) -> None:
    """
    Saves stats in a format that the DAG can use to send email.
    Creates a weekly JSON file with the stats data.
    
    Args:
        stats_bucket: S3 bucket for stats
        stats_df: DataFrame with stats
        mask: Date mask
        week_start_date: Start date of the week
    """
    try:
        # Convert to pandas for HTML formatting
        stats_pd = stats_df.toPandas()
        
        # Extract bucket from stats_bucket
        if stats_bucket.startswith("s3://"):
            stats_bucket = stats_bucket[5:]
        
        s3 = boto3.client('s3')
        
        # Create HTML table
        stats_html = stats_pd.to_html(index=False, classes='table table-striped', table_id='stats-table')
        
        # Create email JSON data
        email_json_data = {
            "subject": f"StackAdapt Input Statistics - {mask}",
            "html_content": f"""
            <html>
            <body>
                <h2>StackAdapt Input Statistics - {mask}</h2>
                <p><strong>Week starting:</strong> {week_start_date}</p>
                <p><strong>Total files processed:</strong> {len(stats_pd)}</p>
                <br>
                {stats_html}
                <br>
                <p><em>This is an automated message from the StackAdapt processing system.</em></p>
            </body>
            </html>
            """,
            "timestamp": datetime.now().strftime("%Y%m%d%H%M%S")
        }
        
        # Save email JSON - use stackadapt folder to match DAG
        email_json_key = f"stackadapt/{week_start_date}/email_data_weekly_{week_start_date}.json"
        s3.put_object(
            Bucket=stats_bucket,
            Key=email_json_key,
            Body=json.dumps(email_json_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Stats email data stored at s3://{stats_bucket}/{email_json_key}")
        
    except Exception as e:
        logger.error(f"Error saving stats for email: {str(e)}")

def logic_main(spark: SparkSession, mask: str, input_file: str, 
               stats_path: str, islocal: str, email_to: str) -> None:
    """
    Main logic for generating statistics
    
    FIXED version that properly handles:
    1. First run: Create CSV with file A1 data
    2. Subsequent runs: Append new files (B1, C1, etc.) to existing CSV
    3. Avoid duplicates based on filename
    4. Proper error handling
    
    Args:
        spark: SparkSession
        mask: Date mask
        input_file: Direct path to input file
        stats_path: Path for statistics output
        islocal: Flag indicating if input is local
        email_to: Email recipients
    """
    logger.info(f"Running stats process for {mask}")
    mask_date = datetime.strptime(mask, "%Y-%m-%d").date()
    week_start_date = (mask_date - timedelta(days=mask_date.weekday())).strftime("%Y-%m-%d") if mask_date.weekday() != 0 else mask

    # Validate input file exists
    if not input_file or not check_path_exists(input_file, islocal):
        logger.error(f"Input file not found: {input_file}")
        return
    
    logger.info(f"Processing file: {input_file}")
    
    # Get stats bucket from stats_path - fix the path construction
    if stats_path.startswith("s3://"):
        stats_bucket = stats_path.replace("s3://", "").split("/")[0]
        stats_base_path = stats_path.rstrip("/")  # Remove trailing slash
    else:
        # If stats_path is not prefixed with s3://, assume it's a relative path
        if "s3://" in input_file:
            stats_bucket = input_file.replace("s3://", "").split("/")[0]
            stats_base_path = f"s3://{stats_bucket}/{stats_path.strip('/')}"
        elif "s3a://" in input_file:
            stats_bucket = input_file.replace("s3a://", "").split("/")[0]
            stats_base_path = f"s3://{stats_bucket}/{stats_path.strip('/')}"
        else:
            logger.error("Cannot determine stats bucket. Both stats_path and input_file lack s3:// prefix")
            return
    
    # Create full path for stats output with proper directory structure
    stats_output = f"{stats_base_path}/{week_start_date}/pipeline_stats__{week_start_date}.csv"
    logger.info(f"Stats will be output to: {stats_output}")

    # Initialize variables
    alertval = 0
    previous_stats = None
    
    # Check if previous stats file exists
    previous_stats_exists = check_path_exists(stats_output, "False")
    logger.info(f"Previous stats file exists: {previous_stats_exists}")

    # Read previous stats if they exist
    if previous_stats_exists:
        try:
            previous_stats = check_and_read_file(spark, stats_output, "csv")
            if previous_stats is not None:
                logger.info(f"Successfully read previous stats file from {stats_output}")
                logger.info(f"Previous stats count: {previous_stats.count()}")
            else:
                logger.warning("Previous stats file exists but could not be read")
                previous_stats_exists = False
        except Exception as e:
            logger.error(f"Error reading previous stats file: {str(e)}")
            previous_stats = None
            previous_stats_exists = False

    # Process the current input file
    try:
        # Determine file extension
        file_ext = os.path.basename(input_file).split(".")[-1]
        if input_file.lower().endswith('.gz'):
            file_ext = 'gz'
        
        # Read the input file
        input_df = check_and_read_file(spark, input_file, file_ext)
        
        if input_df is None:
            logger.error(f"Could not read input file: {input_file}")
            alertval = 1
            # Create alert and exit
            create_alert(stats_bucket, week_start_date, mask, alertval)
            return
        
        logger.info(f"Successfully read {file_ext} file with {input_df.count()} records")

        # Generate stats for the current file
        stats_pd_df = get_stats(mask, week_start_date, input_file, input_df, islocal)
        current_stats = spark.createDataFrame(stats_pd_df)
        logger.info(f"Created stats data for {input_file}")
        
        # Get current file name for duplicate checking
        current_file_name = os.path.basename(input_file)
        
        # Combine with previous stats if they exist
        if previous_stats_exists and previous_stats is not None:
            # Check if current file already exists in previous stats
            existing_files = previous_stats.select("file_name").rdd.flatMap(lambda x: x).collect()
            
            if current_file_name in existing_files:
                logger.info(f"File {current_file_name} already exists in stats. Skipping to avoid duplicates.")
                # No need to update, file already processed
                final_stats = previous_stats
                stats_updated = False
            else:
                logger.info(f"Adding new file {current_file_name} to existing stats")
                # Union with previous stats
                final_stats = previous_stats.union(current_stats)
                stats_updated = True
        else:
            logger.info(f"Creating new stats file with {current_file_name}")
            # First file or previous stats couldn't be read
            final_stats = current_stats
            stats_updated = True
        
        # Remove any potential duplicates and order by date
        final_stats = final_stats.dropDuplicates(subset=["week_start_date", "file_name", "record_counts"])
        final_stats = final_stats.orderBy("date", ascending=False)
        
        final_count = final_stats.count()
        logger.info(f"Final stats count: {final_count}")
        
        # Save stats only if they were updated
        if stats_updated:
            # Convert to pandas and save as CSV
            pdf = final_stats.toPandas()
            
            # Write to S3 as a single CSV file
            s3 = boto3.client('s3')
            csv_buffer = io.StringIO()
            pdf.to_csv(csv_buffer, index=False)
            csv_content = csv_buffer.getvalue()
            
            # Extract bucket and key from the path
            if stats_output.startswith("s3://"):
                bucket = stats_output.replace("s3://", "").split("/")[0]
                key = "/".join(stats_output.replace("s3://", "").split("/")[1:])
                
                # Put the object
                s3.put_object(
                    Bucket=bucket,
                    Key=key,
                    Body=csv_content,
                    ContentType='text/csv'
                )
            
            logger.info("Stats data written to S3")
            
            # Save stats for email
            save_stats_for_email(stats_bucket, final_stats, mask, week_start_date)
            logger.info("Email data saved for DAG")
        else:
            logger.info("Stats not updated - file already processed")
            
    except Exception as e:
        logger.error(f"Error processing input file: {str(e)}")
        alertval = 2
        create_alert(stats_bucket, week_start_date, mask, alertval)
        raise

def create_alert(stats_bucket: str, week_start_date: str, mask: str, alertval: int) -> None:
    """
    Create alert data and save to S3
    
    Args:
        stats_bucket: S3 bucket for stats
        week_start_date: Start date of the week
        mask: Date mask
        alertval: Alert value indicating the type of issue
    """
    try:
        alert_messages = {
            1: "Could not read or process the input file",
            2: "Error occurred during stats processing"
        }
        
        alert_data = {
            "subject": f"Processing Alert for {mask}",
            "alertval": alertval,
            "alert_message": alert_messages.get(alertval, "Unknown error occurred"),
            "timestamp": datetime.now().strftime("%Y%m%d%H%M%S")
        }
        
        # Save alert data to S3
        s3 = boto3.client('s3')
        alert_key = f"get_stats/{week_start_date}/alert_data_{mask}.json"
        
        s3.put_object(
            Bucket=stats_bucket,
            Key=alert_key,
            Body=json.dumps(alert_data, indent=2),
            ContentType='application/json'
        )
        
        logger.info(f"Alert data stored at s3://{stats_bucket}/{alert_key}")
        
    except Exception as e:
        logger.error(f"Error creating alert: {str(e)}")

def main():
    """
    Main entry point for the script
    
    Handles command-line arguments and initializes the SparkSession.
    Calls the main logic function and handles exceptions.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-if", "--input_file", type=str, required=True,
        help="direct path to input file in S3"
    )
    parser.add_argument(
        "-sp", "--stats_path", type=str, required=True,
        help="stats output folder path (e.g., s3://bucket-name/get_stats)"
    )
    parser.add_argument(
        "-m", "--mask", type=str,
        help="date for which process is run", default=datetime.now().strftime("%Y-%m-%d")
    )
    parser.add_argument(
        "-l", "--islocal", type=str,
        help="flag to check if input file is in local fs or S3 (always False for EMR)", default="False"
    )
    parser.add_argument(
        "-et", "--email_to", type=str,
        help="email recipients", default=EMAIL_TO
    )
    args = parser.parse_args()

    # Initialize SparkSession
    spark = SparkSession.builder.appName("GetStats_EMR").getOrCreate()
    
    # Process starts here
    try:
        logic_main(
            spark=spark,
            mask=args.mask,
            input_file=args.input_file,
            stats_path=args.stats_path,
            islocal=args.islocal,
            email_to=args.email_to
        )
        
        logger.info("Stats collection completed successfully")
    except Exception as e:
        logger.error(f"Error in stats collection: {str(e)}", exc_info=True)
        sys.exit(1)
    finally:
        spark.stop()
        logger.info("Spark session stopped")

if __name__ == "__main__":
    main()