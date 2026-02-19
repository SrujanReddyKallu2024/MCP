"""Functions to create the graph extracts, dedupe by id and date, and join with taxonomy data.

Local testing:

pyspark --master spark://spark-master:7077 --conf spark.hadoop.fs.s3a.endpoint=http://minio-hello:9000 --conf spark.hadoop.fs.s3a.access.key=robin --conf spark.hadoop.fs.s3a.secret.key=robinrobin --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem --conf spark.hadoop.fs.s3a.path.style.access=True --jars /jars/aws-java-sdk-bundle-1.12.264.jar,/jars/3.3.4/hadoop-common-3.3.4.jar,/jars/3.3.4/hadoop-aws-3.3.4.jar --py-files=code/ems-code-shared.zip

Usage: See https://pages.experian.local/display/ACTIVATE/Graph+Extraction+and+Dedupe+Functions

"""

# script to identify segments -->
# select segments from experian taxonomy file as extract
# obtain graph -->
# filter by lookback window -->
# dedupe and take top N -->
# as graph 
# join graph and extract

from datetime import datetime
import logging
from pyspark.sql import functions as F
from pyspark.sql import types as T
from pyspark.sql import Window
from pyspark.sql import SparkSession
import os
from pathlib import Path
import stackadapt_shared.functions as sf
import stackadapt_shared.bucket_list as bl
import hashlib
import json

spark = SparkSession.builder.getOrCreate() # late import may guarantee SparkSession is available

def get_hash(selection_dict):
    return hashlib.sha256(json.dumps(selection_dict, sort_keys=True).encode()).hexdigest()

def check_for_existing(selection_dict, file_type, temp_bucket):
    """
    Check if the taxonomy selection file already exists in the specified bucket.
    
    Args:
        selection_dict (dict): Dictionary containing the parameters for the selection.
        file_type (str): Type of file to check for existence.
        bucket (str): Name of the S3 bucket to check.
        
    Returns:
        bool: True if the file exists, False otherwise.
    """

    hash = get_hash(selection_dict)
    output_filename = f"{sf.protocol}://{temp_bucket}/{file_type}/{file_type}_{hash}.parquet"

    logging.info(f"{file_type} hash: {hash}")
    logging.info(f"output_filename: {output_filename}")
    logging.info(f"check if {file_type} file exists")

    try:
        # check if previously this data is created
        file_list = sf.list_files(
            bucket_name=temp_bucket,
            prefix=file_type,
            pattern=f"{file_type}_",
        )
        # keep parquet files, in case someone fixes the list_files soon
        if file_list:
            file_list = [
                file for file in file_list if file.endswith(".parquet")
            ]
        logging.info(f"file_list: {file_list}")        
    except:
        logging.warning(f"Error checking for existing {file_type} file, creating new file")
        file_list = None

    if file_list:
        if len(file_list)>0:
            if output_filename in file_list:
                logging.info(f"{file_type} file already exists {output_filename}")
                return output_filename


def format_s_code_list(s_codes_list, s_code_format="SU{s_code:07}"):
    """
    Format a list of S_Codes into the specified format.
    
    Args:
        s_codes_list (list): List of S_Codes to format.
        s_code_format (str): Format string for S_Codes, default is "SU{s_code:07}".
        
    Returns:
        list: Formatted list of S_Codes.
    """
    return [s_code_format.format(s_code=s_code) for s_code in s_codes_list]


def taxonomy_selection(experian_taxonomy_file, match_id_lookup_experian_file, s_codes_list, temp_bucket):
    """
    Select and format S_Codes from the Experian taxonomy file and join with match_id_hh_key file.
    Args:
        df_experian_taxonomy (str): Experian taxonomy parquet file dataframe.
        match_id_hh_key_file (Dataframe): match_id_hh_key parquet file dataframe.
        s_codes_list (list): List of numeric S_Codes to filter from the taxonomy.
    Returns:
        string: A DataFrame parquet file name containing the household key and S_Code, joined with match_id_hh_key.
    """

    logging.info("read taxonomy data")

    import json
    import hashlib

    taxonomy_selection_dict = {
        "experian_taxonomy_file": experian_taxonomy_file, 
        "match_id_lookup_experian_file": match_id_lookup_experian_file,
        "s_codes_list": s_codes_list
    }

    #temp_bucket = bl.get_bucket_name("temp")
    file_type = "taxonomy_selection"

    existing = check_for_existing(taxonomy_selection_dict, file_type, temp_bucket)
    if existing:
        logging.info(f"{file_type} file already exists, reading from file")
        return existing
    
    logging.info("taxonomy_selection file does not exist, creating new file")
    
    hash = get_hash(taxonomy_selection_dict)
    output_filename = f"{sf.protocol}://{temp_bucket}/{file_type}/{file_type}_{hash}.parquet"

    df_match_id_lookup_experian = (
        spark.read.parquet(match_id_lookup_experian_file)
        .withColumn("match_num", F.col("match_id").cast("bigint"))
        .drop("match_id")
        .withColumnRenamed("match_num", "match_id")
    )

    logging.info("read match id data and swapped match_id for numeric")

    df_experian_taxonomy = (
        spark.read.parquet(experian_taxonomy_file)
    )

    # extract hh key and s code from experian taxonomy
    df_hh_scode_extract = (
        df_experian_taxonomy
        .withColumn("S_Code2", F.col("S_Code").substr(2, 7).cast("int"))
        .drop("S_Code")
        .filter(F.col("S_Code2").isin([str(s_code) for s_code in s_codes_list]))
        .withColumnRenamed("S_Code2", "S_Code")
        .select("cb_key_household", "S_Code")
    )

    logging.info("extracted hh key and s_code from taxonomy")

    # join the household key with match_id_hh_key
    df_matchid_scode_extract = df_hh_scode_extract.join(
        df_match_id_lookup_experian, on="cb_key_household", how="left"
    ).drop("cb_key_household")

    logging.info("joined hh key with match_id_hh_key")
    # save the df in the temp/taxonomy_selection folder
    df_matchid_scode_extract.repartition("S_Code", "match_id").write.parquet(output_filename)

    # save some metadata as json
    sf.get_s3_client().put_object(
        Bucket=temp_bucket, 
        Key=f"{file_type}/{file_type}_{hash}.json", 
        Body=json.dumps(taxonomy_selection_dict, indent=4)
    )

    return output_filename


def get_date_column(id_column):
    """Determine the date column based on the id_column type.
    Args:
        id_column (str): The identifier column type, e.g., "ip", "hems", "udprn", etc.
    Returns:
        str: The name of the date column to use for filtering.
    """

    return "last_seen" if id_column == "ip" else "" if (id_column=="hems" or id_column=="udprn") else "date"


def extract_graph(graph_file, id_column, temp_bucket, days_to_filter=90):
    """Extract graph data from a parquet file, filter by number of days, and repartition by match_id and id_column."""
    logging.info(f"Extracting graph data from {graph_file} with id_column {id_column} and filtering for the last {days_to_filter} days.")

    extraction_dict = {
        "graph_file": graph_file,
        "id_column": id_column,
        "days_to_filter": days_to_filter
    }

    # temp_bucket = bl.get_bucket_name("temp")
    file_type = "graph_extraction"

    existing = check_for_existing(extraction_dict, file_type, temp_bucket)
    if existing:
        logging.info(f"{file_type} file already exists: {existing}")
        #graph_data = spark.read.parquet(existing)
        return existing
    
    logging.info(f"{file_type} file does not exist, creating new file")


    date_column = get_date_column(id_column)
    logging.info(f"Using date column: {date_column}, because graph is {id_column}")
    graph_data = (
        spark.read.parquet(graph_file)
        .withColumn(date_column, F.to_date(F.col(date_column).cast(T.DateType())))
        .filter((F.col(date_column) > F.date_sub(F.current_date(), days_to_filter)))
        .withColumn("match_num", F.col("match_id").cast("bigint"))
        .drop("match_id")
        .withColumnRenamed("match_num", "match_id")
        .repartition(id_column, "match_id")
    )

    hash = get_hash(extraction_dict)
    output_filename = f"{sf.protocol}://{temp_bucket}/{file_type}/{file_type}_{hash}.parquet"
    
    graph_data.write.parquet(output_filename)

    # save some metadata as json
    sf.get_s3_client().put_object(
        Bucket=temp_bucket, 
        Key=f"{file_type}/{file_type}_{hash}.json", 
        Body=json.dumps(extraction_dict, indent=4)
    )

    logging.info(f"extracted: {graph_data.count()} rows")
    return output_filename


def dedupe_graph_with_source(graph_extract_file, id_column, temp_bucket, top=5):
    """Dedupe graph data frame by id_column and date_column, keeping the top N entries per id."""

    logging.info(f"Dedupe graph data {graph_extract_file} by {id_column}, keeping top {top} entries per id, sorted by date.")
    dedupe_dict = {
        "graph_extract_file": graph_extract_file,
        "id_column": id_column,
        "top": top
    }
    # temp_bucket = bl.get_bucket_name("temp")
    file_type = "graph_deduplication"
    existing = check_for_existing(dedupe_dict, file_type, temp_bucket)
    if existing:
        logging.info(f"{file_type} file already exists: {existing}")
        return existing
    

    df_graph = spark.read.parquet(graph_extract_file)
    date_column = get_date_column(id_column)
    has_source = "source" in df_graph.columns

    logging.info(f"Dedupe graph data frame by {id_column} and {date_column}, keeping top {top} entries per id, "
                 "sorted by date and the number of sources specified.")
    
    if has_source:
        window_spec = Window.partitionBy(id_column).orderBy(
            F.desc(date_column), F.desc("source_count")
        )
        df_graph = df_graph.withColumn("source_count", F.size(F.split("source", r",")))
    else:
        window_spec = Window.partitionBy(id_column).orderBy(F.desc(date_column))

    df_with_row_num = df_graph.withColumn("row_num", F.row_number().over(window_spec))

    df_deduped = df_with_row_num.filter(F.col("row_num") <= top).drop("row_num")

    hash = get_hash(dedupe_dict)
    output_filename = f"{sf.protocol}://{temp_bucket}/{file_type}/{file_type}_{hash}.parquet"
    
    df_deduped.write.parquet(output_filename)

    # save some metadata as json
    sf.get_s3_client().put_object(
        Bucket=temp_bucket, 
        Key=f"{file_type}/{file_type}_{hash}.json", 
        Body=json.dumps(dedupe_dict, indent=4)
    )

    df_deduped = spark.read.parquet(output_filename)

    logging.info(f"extracted: {df_deduped.count()} rows")
    return output_filename



