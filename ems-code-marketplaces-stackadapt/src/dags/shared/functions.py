import json
import os
from urllib.parse import urlparse

from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import os
import smtplib

import boto3
import logging
import datetime
from typing import Dict, Tuple
from timeit import default_timer as timer

# this is a convenient way to alter the protocol used in the S3 URLs
# defauls to "s3" but can be changed to "s3a" or "s3n" if needed, for instance, in pyspark shell
# import shared.functions as sf
# sf.protocol = "s3a"
protocol = "s3"

# Dictionary of emojis
emoji = dict()
emoji['pass']   = '✔'
emoji['fail']   = '❌'
emoji['green']  = '🟢'
emoji['amber']  = '🟡'
emoji['red']    = '🔴'
emoji['clock']  = '🕒'

STATUS_EMOJIS = {
    'green': '\U00002705',  # ✅
    'amber': '\U000026A0',  # ⚠️
    'red': '\U0000274C'     # ❌
}

# Process health and quick stata headers
headers= dict()
headers['files']    = '--- FILES ------------------------------------------------------------------------------------------'
headers['health']   = '--- PROCESS HEALTH ----------------------------------------------------------------------------'
headers['qstats']   = '--- QUICK STATS ---------------------------------------------------------------------------------'
headers['time']     = '--- TOTAL TIME TAKEN ---------------------------------------------------------------------------'

# Function to apply triffic lights to stats
def traffic_lights(x):
    if x > 0:
        y = emoji['green']
    elif x < 0:
        y = emoji['red']
    else:
        y = emoji['amber']
    return y

def result_files(address_dict, header):
    result= header
    for key, item in address_dict.items():
        result= f"{result}\n\n {key}: {item}"
    return result

def get_s3_client(miniohost="minio-hello"):
    """Create an S3 client.
    This function creates an S3 client using boto3. If the code is running in a local Docker environment,
    it uses MinIO as the S3 client. Otherwise, it uses AWS S3.
    The MinIO client is configured using environment variables for the access key and secret key.

    Returns:
        s3client: the s3 client
    """
    import boto3

    if platform.is_local and platform.is_docker:
        # Create a session with MinIO
        import os
        import dotenv
        dotenv.load_dotenv()
        session = boto3.Session(
            aws_access_key_id=os.environ["MINIO_ROOT_USER"],
            aws_secret_access_key=os.environ["MINIO_ROOT_PASSWORD"],
        )
        s3 = session.client("s3", endpoint_url=f"http://{miniohost}:9000")
        try:
            import s3fs
            s3fs.S3FileSystem(
                key=os.environ["MINIO_ROOT_USER"],
                secret=os.environ["MINIO_ROOT_PASSWORD"],
                client_kwargs={"endpoint_url": f"http://{miniohost}:9000"}
            )
        except ImportError:
            logging.error("s3fs is not installed. Please install it to use MinIO S3 client.")

        logging.info(f"Using MinIO S3 client with endpoint {os.environ['MINIO_ROOT_USER']}")
    else:
        # Create a session with AWS
        s3 = boto3.client("s3", region_name='eu-west-2')

    return s3

def get_py_files(bucket_name, prefix="repos/ems-code-shared", pattern=""):
    """Get a list of Python files in an S3 bucket with a given prefix and pattern.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix to filter the files.
        pattern (str, optional): The pattern to match in the file names. Defaults to ''.

    Returns:
        list: A list of Python file paths that match the given prefix and pattern.
    """
    import shared.bucket_list
    import boto3
    import os
   
    s3 = get_s3_client()

    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix
    )
    
    py_files = []
    for response in response_iterator:
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                logging.info(f"Checking key: {key}")
                if key.endswith('.py') and pattern in key:
                    py_files.append(key)
    
    if not py_files:
        raise ValueError(f"No Python files found with pattern '{pattern}' in bucket '{bucket_name}' and prefix '{prefix}'")
    
    return [f"{protocol}://{bucket_name}/{file}" for file in py_files]

def list_files(bucket_name, prefix, pattern="", ends_with="", is_pattern_parent_directory_name=True):
    """List files in an S3 bucket with a given prefix and pattern.

    Args:
        bucket_name (str): The name of the S3 bucket.
        prefix (str): The prefix to filter the files.
        pattern (str, optional): The pattern to match in the file names. Defaults to ''.
        ends_with (str, optional): The suffix that the file names should end with. Defaults to ''.

    Returns:
        list: A list of file paths that match the given prefix and pattern.
    """
    import shared.bucket_list
    import boto3
    import os
   
    s3 = get_s3_client()

    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix
    )
    
    files = []
    for response in response_iterator:
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                logging.info(f"Checking key: {key}")
                if pattern in key:
                    if is_pattern_parent_directory_name:
                        # if key contains the pattern as a directory name second to last in the path
                        keys_to_parent = "/".join(key.split('/')[:-1])
                        parentkey= key.split('/')[-2]
                        if pattern in parentkey  and parentkey.endswith(ends_with):
                            if keys_to_parent not in files: files.append(keys_to_parent)
                    else:   
                        # if key contains the pattern anywhere in the path
                        if pattern in key and key.endswith(ends_with):
                            if key not in files: files.append(key)



    
    if not files:
        raise ValueError(f"No files found with pattern '{pattern}' and ends with '{ends_with}' in bucket '{bucket_name}' and prefix '{prefix}'")
    
    return [f"{protocol}://{bucket_name}/{file}" for file in files]

def latest_file(bucket_name, prefix, pattern=''):
    '''
    Finds latest file or directory.
    pattern is the file name before the data.
    Beware there the boto3 paginator will return the files under the key prefix so this will return any part file that matches the pattern.
    '''
    # import subprocess
    # import re
    # files = subprocess.check_output(f'hdfs dfs -ls {path}', shell=True)
    # x= [re.search(' (/.+)', i).group(1) for i in str(files).split("\\n") if re.search(' (/.+)', i)]
    # lenx= len(pattern)
    # y= [x1 for x1 in x if os.path.split(x1)[1][0:lenx]==pattern]
    # y= sorted(y)
    # dirx= y[-1]
    # return dirx
    import shared.bucket_list
    import shared.functions
    import boto3
    import os
   
    s3 = shared.functions.get_s3_client()

    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix
    )
    files = []
    for response in response_iterator:
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                logging.info(f"Checking key: {key}")
                if pattern in key:
                    files.append(key)
    if not files:
        raise ValueError(f"No files found with pattern '{pattern}' in bucket '{bucket_name}' and prefix '{prefix}'")
    
    files.sort()
    return f"{protocol}://{bucket_name}/{files[-1]}"

def latest_path(bucket_name, prefix, pattern='', is_pattern_parent_directory_name=True):
    """From a bucket and a given prefix use the pattern to find the latests path, the default is to find the latest path where the pattern is the parent directory name.
    If the pattern is not found in the parent directory name, it will return the latest path where the pattern is anywhere in the path.
    If no files are found with the pattern, it will raise a ValueError.
    If the pattern is empty, it will return the latest path in the bucket with the given prefix.

    Args:
        bucket_name (str): bucket name to search in
        prefix (str): the part fo the filename key that is not the bucket name, e.g. 'match2/process/2023-10-01/'
        pattern (str, optional): part of the full key name to look for. Defaults to ''.
        is_pattern_parent_directory_name (bool, optional): Defaults to True and means that the pattern is the parent directory name, this is for when there are multiple parts within the directory. If False, it means that the pattern can be anywhere in the path. 

    Raises:
        ValueError: no files found

    Returns:
        str: the name of the lastest path found in the bucket with the given prefix and pattern with {protocol}:// at the start.
    """
    import shared.bucket_list
    import shared.functions
    import boto3
    import os
   
    s3 = shared.functions.get_s3_client()
    
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=prefix
    )
    paths = []
    for response in response_iterator:
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                logging.info(f"Checking key: {key}")
                if is_pattern_parent_directory_name:
                    # if key contains the pattern as a directory name second to last in the path
                    keys_to_parent = "/".join(key.split('/')[:-1])
                    parentkey= key.split('/')[-2]
                    if pattern in parentkey:
                        paths.append(keys_to_parent)
                else:   
                    # if key contains the pattern anywhere in the path
                    if pattern in key:
                        paths.append(key)
    if not paths:
        raise ValueError(f"No files found with pattern '{pattern}' in bucket '{bucket_name}' and prefix '{prefix}'")
    
    paths = list(set(paths))  # Remove duplicates
    logging.info(f"Found paths: {paths}")
    paths.sort()
    return f"{protocol}://{bucket_name}/{paths[-1]}"

def find_latest_folder_by_date(bucket_name, root_prefix, pattern=""):
    """
    Finds the latest folder name in the given S3 root path based on the most recent object modification date.

    Args:
        bucket_name (str): The name of the S3 bucket.
        root_prefix (str): The root prefix (e.g., "root/") where folders are located.

    Returns:
        str: The full S3 path of the latest folder.
    """
    import shared.bucket_list
    import shared.functions
    import boto3
    import os
    import logging
    from datetime import datetime

    s3 = shared.functions.get_s3_client()
    
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=root_prefix,
        Delimiter="/"  # Ensures we only get folder names
    )
    
    latest_year = None
    latest_prefix = None
    
    for response in response_iterator:
    
        common_prefixes = response.get("CommonPrefixes", [])
        logging.info(f"common_prefixes: {common_prefixes}")
        
        # Step 2: Filter prefixes using the pattern
        filtered_prefixes = [
            prefix["Prefix"] for prefix in common_prefixes if pattern in prefix["Prefix"]
        ]
        
        logging.info(f"Filtered Prefixes: {filtered_prefixes}")
        
        # Step 3: Extract the year and find the latest prefix
        for prefix in filtered_prefixes:
            # Extract the year (assumes the year is the last part of the prefix before the trailing slash)
            parts = prefix.rstrip("/").split("/")
            year = parts[-1]
            logging.info(f"Year: {year}")
            if year.isdigit():  # Ensure it's a valid year
                logging.info("Inside the IF Condition")
                logging.info(f"Val Year: {year}")
                year = int(year)
                if latest_year is None or year > latest_year:
                    latest_year = year
                    latest_prefix = prefix
        
        if latest_prefix is None:
            raise ValueError("No valid prefixes with years found in the filtered prefixes.")

    return f"{protocol}://{bucket_name}/{latest_prefix}"

def find_latest_folder(bucket_name, root_prefix, pattern=""):
    """
    Finds the latest folder name in the given S3 root path by sorting the prefixes.
    Handles both year-only paths (e.g., "2025/") and full-date paths (e.g., "2025-05-26/").

    Args:
        bucket_name (str): The name of the S3 bucket.
        root_prefix (str): The root prefix (e.g., "root/") where folders are located.
        pattern (str): Optional pattern to filter prefixes (e.g., "202").

    Returns:
        str: The full S3 path of the latest folder.
    """
    import boto3
    import logging

    s3 = get_s3_client()

    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=bucket_name,
        Prefix=root_prefix,
        Delimiter="/"  # Ensures we only get folder names
    )

    # Collect all prefixes
    prefixes = []
    for response in response_iterator:
        common_prefixes = response.get("CommonPrefixes", [])
        for prefix in common_prefixes:
            if pattern in prefix["Prefix"]:  # Filter using the pattern
                prefixes.append(prefix["Prefix"])

    if not prefixes:
        raise ValueError(f"No prefixes found in bucket '{bucket_name}' with prefix '{root_prefix}' and pattern '{pattern}'")

    # Sort the prefixes lexicographically and return the latest one
    prefixes.sort()
    latest_prefix = prefixes[-1]  # The last item is the latest

    return f"{protocol}://{bucket_name}/{latest_prefix}"

def folder_exists_and_not_empty(bucket_name, folder_prefix):
    """
    Check if a folder exists in an S3 bucket and is not empty.

    Args:
        bucket_name (str): The name of the S3 bucket.
        folder_prefix (str): The prefix of the folder to check (e.g., "path/to/folder/").

    Returns:
        bool: True if the folder exists and is not empty, False otherwise.
    """
    import logging
    s3 = get_s3_client()

    try:
        # Use the paginator to check for objects under the folder prefix
        paginator = s3.get_paginator('list_objects_v2')
        response_iterator = paginator.paginate(
            Bucket=bucket_name,
            Prefix=folder_prefix
        )

        for response in response_iterator:
            # Check if there are any objects under the prefix
            if 'Contents' in response:
                for obj in response['Contents']:
                    # If there is an object other than the placeholder, the folder is not empty
                    if obj['Key'] != folder_prefix:
                        logging.info(f"Folder '{folder_prefix}' exists and is not empty in bucket '{bucket_name}'")
                        return True

        # If no objects other than the placeholder are found, return False
        logging.info(f"Folder '{folder_prefix}' exists but is empty in bucket '{bucket_name}'")
        return False

    except Exception as e:
        logging.error(f"Error checking if folder exists: {e}")
        return False
    
def delete_file(bucket_name, key):
    """
    Delete a file from an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key of the file to delete.
    """
    import shared.functions
    
    s3 = shared.functions.get_s3_client()
    logging.info(f"Deleting file from S3: bucket={bucket_name}, key={key}")
    try:
        s3.delete_object(Bucket=bucket_name, Key=key)
        logging.info("File deleted successfully.")
        return True
    except Exception as e:
        logging.error(f"Error deleting file: {e}")
        return False
    
def get_s3_file_object(bucket_name: str, key: str):
    """
    Retrieve a file from S3 and return its body as a file-like object for pandas.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key (path) of the file in the S3 bucket.

    Returns:
        StreamingBody: A file-like object containing the file's content, suitable for pandas read functions.

    Raises:
        Exception: If the file cannot be retrieved from S3.
    """
    import logging
    import shared.functions

    logger = logging.getLogger(__name__)
    logger.info(f"Attempting to read file from S3: bucket='{bucket_name}', key='{key}'")

    try:
        s3 = shared.functions.get_s3_client()
        response = s3.get_object(Bucket=bucket_name, Key=key)
        body = response.get('Body')
        logger.info(f"Successfully retrieved file from S3: bucket='{bucket_name}', key='{key}'")
        return body
    except Exception as e:
        logger.error(f"Failed to retrieve file from S3: bucket='{bucket_name}', key='{key}'. Error: {e}")
        raise
    
def get_bucket_and_prefix(s3path):
    """
    Extracts the S3 bucket and key prefix from a given S3 path.

    Args:
        s3path (str): The S3 path, e.g., "s3a://my-bucket/folder/file.csv" or "s3://my-bucket/folder/file.csv".

    Returns:
        tuple: (bucket, key) where bucket is the S3 bucket name and key is the object key prefix.

    Raises:
        ValueError: If the input path is not a valid S3 path.
    """
    if not isinstance(s3path, str):
        raise ValueError("Input path must be a string.")

    if s3path.startswith("s3a://"):
        s3path = s3path[6:]
    elif s3path.startswith("s3://"):
        s3path = s3path[5:]
    else:
        raise ValueError(f"Invalid S3 path: '{s3path}'. Path must start with 's3a://' or 's3://'.")

    parts = s3path.split("/", 1)
    if not parts[0]:
        raise ValueError(f"Invalid S3 path: '{s3path}'. Bucket name is missing.")

    bucket = parts[0]
    key = parts[1] if len(parts) > 1 else ""
    return bucket, key

def zip_s3_folder(s3_bucket, s3_folder, zip_s3_bucket, zip_s3_key, local_tmp_dir='/tmp/create_zip'):
    import zipfile
    import shared.functions
    s3 = shared.functions.get_s3_client()
    
    # Step 1: Read Parquet folder with Spark (optional, for validation)
    # parquet_path = f's3a://{s3_bucket}/{s3_folder}/'
    # df = spark.read.parquet(parquet_path)
    # print(f"Read {df.count()} rows from {parquet_path}")
    # Step 2: List all part files in the S3 folder
    
    response = s3.list_objects_v2(Bucket=s3_bucket, Prefix=s3_folder)
    part_files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.parquet') or 'part' in obj['Key']]
    # Step 3: Download files locally
    os.makedirs(local_tmp_dir, exist_ok=True)
    local_files = []
    for key in part_files:
        local_path = os.path.join(local_tmp_dir, os.path.basename(key))
        s3.download_file(s3_bucket, key, local_path)
        local_files.append(local_path)
    # Step 4: Zip the files
    zip_file_name = os.path.basename(zip_s3_key)
    zip_path = os.path.join(local_tmp_dir, zip_file_name)
    with zipfile.ZipFile(zip_path, 'w') as zipf:
        for file in local_files:
            zipf.write(file, arcname=os.path.basename(file))
    # Step 5: Upload zip back to S3
    s3.upload_file(zip_path, zip_s3_bucket, zip_s3_key)
    print(f"Uploaded zip to s3://{zip_s3_bucket}/{zip_s3_key}")
    # Cleanup (optional)
    for file in local_files:
        os.remove(file)
    os.remove(zip_path)
    os.rmdir(local_tmp_dir)

def delete_from_s3(logger, bucket_name, prefix, extensions, exclude_filename="notapplicable"):
    """
    Deletes files in S3 bucket under the given prefix that match extensions and do not contain exclude_filename.
    Returns: True if files were deleted, else False
    """
    import shared.functions
    s3 = shared.functions.get_s3_client()
    
    paginator = s3.get_paginator('list_objects_v2')
    deleted = False
    for page in paginator.paginate(Bucket=bucket_name, Prefix=prefix):
        if 'Contents' in page:
            for obj in page['Contents']:
                key = obj['Key']
                if any(key.endswith(ext) for ext in extensions) and exclude_filename not in key:
                    s3.delete_object(Bucket=bucket_name, Key=key)
                    logger.info(f"deleted file from S3: {key}")
                    deleted = True
        else:
            logger.info("no files to delete in S3")
    return deleted

def rename_files(
    mask,
    all_files,
    graph_type,
    new_name_template=f"UK__experianuk_{{graph_type}}_UK_{{mask}}_{{partname}}.csv.gz",
    part_offset=0,
):
    s3 = get_s3_client()
    """
    Renames part files
    """
    logging.info(
        f"renaming files for {graph_type} and {mask} with template {new_name_template}"
    )
    logging.info(
        f"preview: {new_name_template.format(graph_type=graph_type, mask=mask, partname='partname')}"
    )
    
    import time
    from urllib.parse import urlparse

    try:
        for i, s3_uri in enumerate(all_files):
            parsed = urlparse(s3_uri)
            bucket = parsed.netloc
            key = parsed.path.lstrip("/")
            prefix = "/".join(key.split("/")[:-1])

            if graph_type == "meta":
                partname = str(int(round(1000 * time.time(), 0)))
            else:
                partname = str(i + 1 + part_offset).zfill(3)

            new_filename = new_name_template.format(
                graph_type=graph_type, mask=mask, partname=partname
            )
            new_key = f"{prefix}/{new_filename}"

            logging.info(f"Copying {key} → {new_key}")
            s3.copy_object(
                Bucket=bucket, CopySource={"Bucket": bucket, "Key": key}, Key=new_key
            )
            s3.delete_object(Bucket=bucket, Key=key)
            logging.info(f"Renamed {s3_uri} to s3://{bucket}/{new_key}")

    except Exception as e:
        logging.exception(f"Failed to rename S3 files for graph_type='{graph_type}'")
        raise

def get_greeting():
    """
    gets the greeting based on time of day
    """
    from datetime import datetime
    
    now_time = datetime.now()
    if (now_time.hour >= 12) & (now_time.hour < 17):
        return "Good afternoon,"
    elif now_time.hour >= 17:
        return "Good evening,"
    else:
        return "Good morning,"


def get_status_email_fields(email_subject, body, s3_file_path=None):
    """
    email format for simple text body
    """

    # email variables
    greeting = get_greeting()
    EMAIL_FROM = "emsactivate@experian.com"
    MESSAGE_BODY = (
        f"{greeting} \n\n{body} \n\nThanks and Regards\nExperian Match Team".replace(
            "\n", "<br>"
        )
    )
    email_fields = {
        "subject": email_subject,
        "body": MESSAGE_BODY,
        "from": EMAIL_FROM,  # currently not used
    }
    if s3_file_path:
        email_fields["s3_file_path"] = s3_file_path
    return email_fields


def get_teams_email_fields(mask, email_subject, body, swap_newlines=True):
    """
    email formatted for both stats and alerts
    """
    EMAIL_FROM = "emsactivate@experian.com"
    if not body:
        body = "No stats generated, please check logs."
    if swap_newlines:
        body = body.replace("\\n", "<br>")

    msg_body = f"""\
        <!DOCTYPE html>
        <html>
          <head></head>
          <body>
            <div style="text-align: center;"><b>{email_subject}</b></div>
            <br>
            <br>
            {body}
            <br>
            <p style='margin-bottom:0;'>Thanks and regards</p>
            <p style="margin : 0; padding-top:0;">Experian Match Team</p>
          </body>
        </html>
    """
    email_fields = {
        "subject": mask + ": " + email_subject,
        "body": msg_body,
        "from": EMAIL_FROM,  # currently not used
    }
    return email_fields


def save_email_data(
    success_flag, teams_email_fields, status_email_fields, email_data_path
):
    email_data = {"success_flag": success_flag}
    if teams_email_fields:
        email_data["teams_email"] = teams_email_fields
    if status_email_fields:
        email_data["status_email"] = status_email_fields

    email_data = json.dumps(email_data)
    parsed = urlparse(email_data_path)
    bucket = parsed.netloc
    key = parsed.path.lstrip("/")
    s3 = get_s3_client()
    s3.put_object(
        Bucket=bucket, Key=key, Body=email_data, ContentType="application/json"
    )


def file_exists(bucket_name, key, is_pattern_parent_directory_name=True, exclude=None):
    """Check if a file exists in an S3 bucket.

    Args:
        bucket_name (str): The name of the S3 bucket.
        key (str): The key of the file to check.

    Returns:
        bool: True if the file exists, False otherwise.
    """
    logging.info(f"Checking if file exists in bucket '{bucket_name}' with key '{key}'")
    s3 = get_s3_client()
    try:
        # If is_pattern_parent_directory_name is True, check if the bucket contains the pattern as a directory name among the keys.
        if is_pattern_parent_directory_name:
            paginator = s3.get_paginator('list_objects_v2')
            response_iterator = paginator.paginate(
                Bucket=bucket_name,
                Prefix=key
            )
            for response in response_iterator:
                if 'Contents' in response:
                    for obj in response['Contents']:
                        logging.info(f"Checking object key: {obj['Key']}")
                        if key in obj['Key']:
                            if not exclude or exclude and exclude not in obj['Key']:
                                return True
            return False            
        else:
            logging.info(f"Checking if file exists with head_object for key: {key}")
            s3.head_object(Bucket=bucket_name, Key=key)
            return True
    except Exception as e:
        logging.error(f"Error checking if file exists: {e}")
        return False

def copy_files(source_bucket, source_key, destination_bucket, destination_key):
    """Copy a file from one S3 bucket to another.

    Args:
        source_bucket (str): The name of the source S3 bucket.
        source_key (str): The key of the file to copy in the source bucket.
        destination_bucket (str): The name of the destination S3 bucket.
        destination_key (str): The key for the copied file in the destination bucket.
    """
    s3 = get_s3_client()
    # get list of keys under the source_key
    files = []
    paginator = s3.get_paginator('list_objects_v2')
    response_iterator = paginator.paginate(
        Bucket=source_bucket,
        Prefix=source_key
    )
    for response in response_iterator:
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                logging.info(f"Found key: {key}")
                files.append(key)

    if not files:
        raise ValueError(f"No files found in bucket '{source_bucket}' with prefix '{source_key}'")
    if not destination_key.endswith('/'):
        destination_key += '/'
    
    for file in files:
    # Copy each file to the destination bucket
        source_key = file
        destination_file_key = os.path.join(destination_key, os.path.basename(source_key))
        logging.info(f"Copying {source_key} to {destination_file_key} in {destination_bucket}")
        copy_source = {'Bucket': source_bucket, 'Key': source_key}
        s3.copy_object(CopySource=copy_source, Bucket=destination_bucket, Key=destination_file_key)
        logging.info(f"Copied {source_key} to {destination_file_key} in {destination_bucket}")
        
def update_stats(stats, milestone, status="success", message=""):
    """
    Update the stats dictionary with the milestone, status and message
    """
    from datetime import datetime
    
    stats.append({
        "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "Milestone": milestone,
        "Message": message,
        "Status": STATUS_EMOJIS['green'] if status.lower()=="success" else STATUS_EMOJIS['red']
    })
        


def delete_batch(bucket_name: str, keys: list) -> None:
    """
    Delete a batch of objects from an S3 bucket.

    :param bucket_name: The name of the S3 bucket
    :param keys: A list of objects to delete, eg: ['path/to/object1', 'path/to/object2']
    :returns True if successfully deleted else False
    """
    s3 = get_s3_client()
    try:
        objs_to_del = [{"Key": k} for k in keys]        
        response = s3.delete_objects(
            Bucket=bucket_name,
            Delete={'Objects': objs_to_del, 'Quiet': False}
        )
        # Handling successful deletions
        if 'Deleted' in response:
            for deleted in response['Deleted']:
                logging.info(f"Deleted: {deleted['Key']}")
        # Handling errors
        if 'Errors' in response:
            for error in response['Errors']:
                logging.error(f"Error deleting {error['Key']}: {error['Message']}")
        return True
    except Exception as e:
        logging.error(f"Unable to delete: {e}")


class Platform:
    """
    Class to determine the platform the code is running on.
    """
    def __init__(self):
        import os
        import subprocess
        import sys
        self.is_docker = ".dockerenv" in os.listdir("/") or "entrypoint.sh" in os.listdir("/opt")
        if os.environ.get('AWS_EXECUTION_ENV') == 'CloudShell':
            self.is_docker = False # CloudShell is a Docker environment, but this enables us to run the code in CloudShell without Docker checks.

        self.is_linux =  "linux" in sys.platform 
        self.is_windows =  "win" in sys.platform 
        if self.is_docker:
            self.platform = "docker"
        elif self.is_linux:
            self.platform = "linux"
        elif self.is_windows:
            self.platform = "windows"
        else:
            self.platform = "unknown"

        try:
            subprocess.check_call(["hdfs", "dfs", "-ls"])
            self.is_hdfs = True
        except:
            self.is_hdfs = False

        self.is_local = True if self.is_windows or self.is_docker else False
        if self.is_local:
            logging.info("Running in local environment")
            global protocol
            protocol = "s3a"  # Use s3a for local development with MinIO or similar
            try:
                import dotenv
                dotenv.load_dotenv()
                logging.info("Loaded environment variables from .env file")
            except ImportError:
                logging.warning("dotenv module not found, environment variables may not be loaded")

platform = Platform()

def get_env():
    '''
    Get the environment from the environment variable if it exists, otherwise from boto3 airflow variable.
    '''
    env = os.getenv('ENV')
    logging.info(f"Environment vars: {os.environ}")
    if platform.is_local and platform.is_docker:
        # If running in local Docker, default to 'local' environment
        env = 'local'
        logging.info("Running in local Docker environment, setting ENV to 'local'")

    if env is None:
        # Get the environment from boto3
        try:
            ssm = boto3.client('ssm', region_name='eu-west-2')
            #logging.info(f"parameters {[p['Name'] for p in ssm.describe_parameters()['Parameters']]}")
            env = ssm.get_parameter(Name='/environment/env')['Parameter']['Value']
        except Exception as e:
            logging.error(f"Failed to retrieve environment from boto3 SSM: {e}")
            logging.warning("Environment variable 'ENV' not set and unable to retrieve from boto3 SSM.")
            env = None

    if env is None:
        raise ValueError("Environment variable 'ENV' not set")
    return env

def stats_prev_load(path: str, code: str, week: str, spark) -> Dict:
    """
    Load previous statistics from S3 text file using Spark.
    
    This function retrieves statistics from the previous processing run
    to enable week-over-week comparisons. Statistics are stored as
    key-value pairs in a text file.
    
    Args:
        path: S3 path to the statistics file
        code: Script code identifier
        week: Week identifier
        spark: Active SparkSession
        
    Returns:
        Dictionary of statistics keys and values
    """
    stats_dict = {}
    
    try:
        # Check if path exists
        logging.info(f"Loading previous stats from {path}")
        
        # Use Spark to read the text file
        prev_stats = spark.read.text(path)
        
        # Process each line to extract key-value pairs
        for row in prev_stats.collect():
            try:
                # Expected format: key=value
                key_value = row.value.split('=')
                if len(key_value) == 2:
                    stats_dict[key_value[0].strip()] = float(key_value[1].strip())
            except Exception as e:
                logging.warning(f"Error reading stats file: {str(e)}")
    except Exception as e:
        logging.warning(f"Error loading previous stats: {str(e)}")
    
    return stats_dict


def quick_stats_backup(path: str, code: str, week: str, stats: Dict, spark) -> None:
    """
    Save current statistics to S3 text file using boto3 for a clean single file.
    
    Args:
        path: S3 path where statistics will be saved
        code: Script code identifier
        week: Week identifier (format: YYYY-MM-DD)
        stats: Dictionary of statistics to save
        spark: Active SparkSession
    """
    import boto3
    import tempfile
    
    try:
        # Use standardized naming 
        full_path = path
        logging.info(f"Saving stats to {full_path}")
        
        # Create content as a simple string
        content = "\n".join([f"{k}={v}" for k, v in stats.items()])
        
        # Parse S3 path using the utility function
        bucket_name, key = parse_s3_path(full_path)
        
        # Use boto3 to write directly to S3 as a single file
        s3_client = get_s3_client()
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=content
        )
        
        logging.info(f"Stats saved to s3://{bucket_name}/{key}")
    except Exception as e:
        logging.warning(f"Error saving stats: {str(e)}")

def quick_stats_perc(stats: Dict, stats_prev: Dict) -> Dict:
    """
    Calculate percentage change between current and previous stats.
    
    Args:
        stats: Current statistics dictionary
        stats_prev: Previous run's statistics (or empty string if none)
        
    Returns:
        Dictionary of percentage changes for each stat
    """
    stats_perc = dict()
    
    # Handle case where no previous stats exist
    if stats_prev == '':
        stats_prev = dict()
        
    for key, value in stats.items():
        if key in stats_prev and stats_prev[key] != 0:
            # Calculate percentage change 
            stats_perc[key] = round(100 * (stats[key] / stats_prev[key] - 1), 2)
        else:
            # Use '-.--' string for missing previous data
            stats_perc[key] = '-.--'
            
    return stats_perc

def convert_dict_to_df(ps, stats_dict):
    """
    Converts a stats dictionary containing scalar values to a dataframe
    """
    for k in stats_dict.keys():
        stats_dict[k] = [stats_dict[k]]
    return ps.DataFrame.from_dict(stats_dict, orient = 'columns')

def get_formatter(var):
    if type(var) is int:
        return ',d'
    else:
        return ''
    
def create_flag_file(bucket_name, key):
    """
    Create a flag file in the specified S3 bucket and key.
    """
    import logging
    
    s3 = get_s3_client()
    
    try:
        # Create an empty file in the specified S3 location
        s3.put_object(Bucket=bucket_name, Key=key, Body='')
        logging.info(f"Flag file created at s3://{bucket_name}/{key}")
        return True
    except Exception as e:
        logging.error(f"Error creating flag file: {e}")
        return False

def quick_stats_result(stats, stats_name, stats_perc, stats_tl, result):
    for key, value in stats.items():
        result= f"{result}\n\n{stats_tl[key]} {stats_name[key]} [{stats[key]:{get_formatter(stats[key])}}] ({stats_perc[key]}%)"
    return result

# Update the quick_stats function to use the exact quick_stats_perc function
def quick_stats(stats: Dict, stats_name: Dict, stats_prev: Dict, result: str) -> Tuple[Dict, Dict, str]:
    """
    Calculate statistics and format for display.
    
    Args:
        stats: Current statistics dictionary
        stats_name: Dictionary mapping stat keys to display names
        stats_prev: Previous run's statistics (or empty string if none)
        result: Current result string to append to
        
    Returns:
        Tuple of (percentage stats, trend indicators, formatted result string)
    """
    # Calculate percentage changes
    stats_perc = quick_stats_perc(stats, stats_prev)
    
    # Calculate traffic light indicators
    stats_tl = dict()
    for key, value in stats_perc.items():
        if stats_perc[key] == '-.--':
            stats_tl[key] = "⚪"  # Neutral when no previous data
        elif isinstance(value, (int, float)):
            if value > 0:
                stats_tl[key] = "🟢"  # Positive change
            elif value < 0:
                stats_tl[key] = "🔴"  # Negative change
            else:
                stats_tl[key] = "⚪"  # No change

    logging.info("=== QUICK STATS ===")
    for key, value in stats.items():
        #logging.info(f"{key}: {value}")
        #logging.info(f"{stats_name[key]}: {value:,} ({stats_perc[key]}%) {stats_tl[key]}")
        if key in stats_name:
            #logging.info(f"{stats_name[key]}: {value}")
            perc_display = f" ({stats_perc[key]}%)" if stats_perc[key] != '-.--' else " (-.--%)"
            #logging.info(f"{perc_display}")
            stat_line = f"{stats_tl[key]} {stats_name[key]} [{stats[key]:}]{perc_display}"
            logging.info(stat_line)
            
    result = quick_stats_result(stats, stats_name, stats_perc, stats_tl, result)
    
    return stats_perc, stats_tl, result

# ----------------------------------------------------------------------
# PROCESS TRACKING CONTEXT MANAGER
# ----------------------------------------------------------------------
class ProcessTracker:
    """
    Context manager to track process steps with timing.
    """
    def __init__(self, process_num: int, process_labels: dict):
        self.process_num = process_num
        self.process_labels = process_labels
        self.start_time = None

    def __enter__(self):
        """Called when entering the 'with' block - logs the start and records time"""
        self.start_time = timer()
        logging.info(f"Starting process {self.process_num}: {self.process_labels.get(self.process_num, 'Unknown')}")
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Called when exiting the 'with' block - logs completion time and handles errors
        """
        end_time = timer()
        duration = datetime.timedelta(seconds=round(end_time - self.start_time, 0))

        if exc_type:
            logging.error(f"Error in process {self.process_num}: {exc_val}")
            # Store the process number for error handling higher up
            global process_num2
            process_num2 = self.process_num
            return False  # Propagate the exception
        else:
            logging.info(f"Completed process {self.process_num} in {duration}")
            return True
        
def nmr_apply(df_in, nmr_path=None, spark=None):
    """
    Apply Network Mobile Removal filter to the dataframe.
    
    Excludes households found in the NMR dataset to remove mobile connections.
    
    Args:
        df_in: Input DataFrame with household data
        nmr_path: Path to NMR dataset file (CSV)
        spark: SparkSession (optional - will use active session if None)
        
    Returns:
        DataFrame with mobile network households removed
    """
    logger = logging.getLogger(__name__)
    
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()

    
    try:
        logger.info(f"Running NMR filter using data from: {nmr_path}")
        
        # Read the NMR data
        nmr01 = spark.read.csv(nmr_path, header=True)
        logger.info(f"Loaded NMR data: {nmr01.count():,} rows")
        
        # Get distinct household ID's to filter
        nmr02 = nmr01.groupby('cb_key_household').count().drop('count')
        logger.info(f"NMR filter will exclude {nmr02.count():,} households")
        
        # Apply NMR filter by anti-joining with the NMR household ID's - exactly as in production
        temp01 = df_in.join(nmr02, 'cb_key_household', 'left_anti')
        logger.info(f"After NMR filter: {temp01.count():,} rows remaining from {df_in.count():,} original rows")
        
        return temp01
    
    except Exception as e:
        logger.error(f"Error applying NMR filter: {str(e)}", exc_info=True)
        raise


def cip_apply(df_in, cip_path=None, spark=None):
    """
    Apply Commercial IP filtering to the dataframe.
    
    Removes known commercial IPs from the dataset to improve household targeting accuracy.
    
    Args:
        df_in: Input DataFrame with IP data
        cip_path: Path to Commercial IP dataset (parquet)
        spark: SparkSession (optional - will use active session if None)

    Returns:
        DataFrame with commercial IPs removed
    """
    logger = logging.getLogger(__name__)
    
    if spark is None:
        from pyspark.sql import SparkSession
        spark = SparkSession.builder.getOrCreate()
    
    
    try:
        logger.info(f"Running CIP filter using data from: {cip_path}")
        
        # Read the CIP data
        cip01 = spark.read.parquet(cip_path)
        logger.info(f"Loaded CIP data: {cip01.count():,} rows")
        
        # Prepare CIP data - exactly as in production
        cip02 = cip01.withColumnRenamed('host', 'ip')
        cip03 = cip02.groupby('ip').count().drop('count')
        logger.info(f"CIP filter will exclude {cip03.count():,} IPs")
        
        # Apply CIP filter by anti-joining with the CIP IPs
        temp01 = df_in.join(cip03, 'ip', 'left_anti')
        logger.info(f"After CIP filter: {temp01.count():,} rows remaining from {df_in.count():,} original rows")
        
        return temp01
    
    except Exception as e:
        logger.error(f"Error applying CIP filter: {str(e)}", exc_info=True)
        raise

def save_formatted_report(path: str, result_str: str):
    """
    Save a formatted report to S3 for email sending.
    
    Args:
        path: S3 path where the report will be saved
        result_str: Formatted result string with sections
    """
    import boto3
    
    try:
        # Parse S3 path using the utility function
        bucket_name, key = parse_s3_path(path)
        
        # Use boto3 to write directly to S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=result_str
        )
        
        logging.info(f"Formatted report saved to s3://{bucket_name}/{key}")
    except Exception as e:
        logging.warning(f"Error saving formatted report: {str(e)}")
        
        # Use boto3 to write directly to S3
        s3_client = boto3.client('s3')
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=result_str
        )
        
        logging.info(f"Formatted report saved to s3://{bucket_name}/{key}")
    except Exception as e:
        logging.warning(f"Error saving formatted report: {str(e)}")

def parse_s3_path(path: str) -> Tuple[str, str]:
    """
    Parse an S3 path to extract bucket name and key.
    
    Handles both s3:// and s3a:// protocols
    
    Args:
        path: S3 path in format s3://bucket/key or s3a://bucket/key
        
    Returns:
        Tuple of (bucket_name, key)
        
    Raises:
        ValueError: If the path format is invalid
    """
    if path.startswith('s3://'):
        s3_parts = path.replace('s3://', '').split('/', 1)
    elif path.startswith('s3a://'):
        s3_parts = path.replace('s3a://', '').split('/', 1)
    else:
        raise ValueError(f"Invalid S3 path format: {path}")
    
    if len(s3_parts) != 2:
        raise ValueError(f"Invalid S3 path structure: {path}")
        
    return s3_parts[0], s3_parts[1]

def send_status_email(email_subject, body, email_to):
    """
    email format for simple text body
    """
    # email variables
    greeting = get_greeting()
    EMAIL_FROM = 'marketplace_monitor@experian.com'
    MESSAGE_BODY = f"{greeting} \n\n{body} \n\nThanks and Regards\nExperian Match Team"
    SMTP_SERVER = "relay.uk.experian.local"
    msg = MIMEMultipart()
    body_part = MIMEText(MESSAGE_BODY, 'plain')
    msg['Subject'] = email_subject
    msg['From'] = EMAIL_FROM
    msg['To'] = email_to
    # Add body to email
    msg.attach(body_part)
    smtp = smtplib.SMTP(SMTP_SERVER)
    logging.info(f"Sending email to: {email_to}")
    smtp.sendmail(EMAIL_FROM, email_to.split(","), msg.as_string())
    smtp.quit()


def send_teams_email(mask, email_subject, body, email_to, swap_newlines=True):
    """
    email formatted for both stats and alerts
    """
    # email variables
    EMAIL_FROM = 'mkp_monitor@experian.com'
    EMAIL_CC = ''
    SMTP_SERVER = "relay.uk.experian.local"
    msg = MIMEMultipart('alternative')
    if body==False:
        body = "No stats generated, please check logs."
    if swap_newlines:
        body = body.replace("\\n","<br>")

    msg_body = f"""\
        <!DOCTYPE html>
        <html>
          <head></head>
          <body>
            <div style="text-align: center;"><b>{email_subject}</b></div>
            <br>
            <br>
            {body}
            <br>
            <p style='margin-bottom:0;'>Thanks and regards</p>
            <p style="margin : 0; padding-top:0;">Experian Match Team</p>
          </body>
        </html>
    """
    body_part = MIMEText(msg_body, 'html')
    msg['Subject'] = mask + ": " + email_subject
    msg['From'] = EMAIL_FROM
    msg['To'] = email_to
    msg.attach(body_part)
    smtp = smtplib.SMTP(SMTP_SERVER)
    logging.info(f"Sending teams email to: {email_to}")
    smtp.sendmail(EMAIL_FROM, email_to.split(","), msg.as_string())
    smtp.quit()


def email_on_failure(context):

    from airflow.utils.email import send_email

    exception_html = str(context["exception"]).replace("\n", "<br>")
    html_content = (
        f"Try {context['ti'].try_number} out of {context['ti'].max_tries+1}<br>"
        f"Task {context['ti'].task_id} failed with error: {exception_html}<br>"
        f"DAG Run ID: {context['run_id']}<br>"
        f"Execution date: {context['ti'].execution_date}<br>"
        f'Logs: <a href="{context["ti"].log_url}">Airflow UI</a><br>'
    )

    send_email(
        to=context["params"].get("email_to", "emsactivatealerts@experian.com"),
        subject=f"DAG Failure Notification - {context['ti'].dag_id}",
        html_content=html_content,
    )