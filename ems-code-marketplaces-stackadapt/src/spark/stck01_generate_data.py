import argparse
from datetime import datetime, timedelta
import json
import logging
from typing import Any, Dict, List

import boto3
from pyspark.sql import DataFrame, SparkSession, Window
import pyspark.sql.functions as F

# Configure logger
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)

logger = logging.getLogger(__name__)


REQUIRED_COLUMNS = {
    "mobile": ["hhk", "hashed_mobile"],
    "hems": ["hhk", "hems"],
    "maids": ["hhk", "maids", "date"],
    "uid": ["hhk", "uid", "date"],
    "ip": ["hhk", "ip", "first_seen", "last_seen", "conf_score"],
}


class Stats:
    """Class to store and add stats"""

    STATUS_EMOJIS = {
        "green": "\U00002705",  # ✅
        "amber": "\U000026a0",  # ⚠️
        "red": "\U0000274c",  # ❌
    }

    def __init__(self):
        self._stats = []

    def add_stats(self, milestone: str, status: str = "success", message: str = ""):
        """
        Add a stats dictionary with the milestone, status and message

        Args:
            milestone (str): Milestone name
            status (str): Status of the milestone ("success" or "failed"), default is "success"
            message (str): Additional message
        """
        self._stats.append(
            {
                "Time": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "Milestone": milestone,
                "Message": message,
                "Status": (
                    self.STATUS_EMOJIS["green"]
                    if status.lower() == "success"
                    else self.STATUS_EMOJIS["red"]
                ),
            }
        )

    def get_stats(self) -> List[Dict[str, Any]]:
        """
        Get the stats list

        Returns:
            List[Dict[str, Any]]: List of stats dictionaries
        """
        return self._stats


def save_json_to_s3(s3: boto3.client, data: Dict, path: str):
    """Save a dictionary as a JSON file to S3.

    Args:
        data (Dict): The dictionary to save.
        path (str): The S3 path where the JSON file will be saved.
    """
    bucket, key = path.replace("s3://", "").split("/", 1)
    json_data = json.dumps(data)
    s3.put_object(Bucket=bucket, Key=key, Body=json_data.encode("utf-8"))


def format_graph(idg_df: DataFrame, idg_date_col: str, cut_off: int) -> DataFrame:
    """
    format the given graph

    Args:
        idg_df (DataFrame): Input DataFrame to format
        idg_date_col (str): Date column name for the identifier
        cut_off (int): Cut-off days for filtering

    Returns:
        DataFrame: Formatted DataFrame
    """
    if cut_off != -1:
        latest_date = idg_df.agg(F.max(F.col(idg_date_col))).collect()[0][0]
        start_date = (
            datetime.strptime(latest_date, "%Y-%m-%d") - timedelta(days=cut_off)
        ).strftime("%Y-%m-%d")
        logger.info(f"filtering graph data between {start_date} and {latest_date}")
        idg_df = idg_df.where(F.col(idg_date_col).between(start_date, latest_date))
    else:
        logger.info("no date filtering applied to graph data")
    return (
        idg_df.withColumn("source_array", F.split(F.col("source"), ","))
        .withColumn("source_count", F.size("source_array"))
        .withColumn(
            "chv",
            F.when(F.array_contains(F.col("source_array"), "chv"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "de",
            F.when(F.array_contains(F.col("source_array"), "de"), F.lit(1)).otherwise(F.lit(0)),
        )
        .withColumn(
            "uc",
            F.when(F.array_contains(F.col("source_array"), "uc"), F.lit(1)).otherwise(F.lit(0)),
        )
    )


def apply_rank(idg_df: DataFrame, idg: str, idg_date_col: str) -> DataFrame:
    """
    apply rank to the graph based on the given identifier

    Args:
        idg_df (DataFrame): Input DataFrame to rank
        idg (str): Identifier type (e.g., "ip")
        idg_date_col (str): Date column name for the identifier

    Returns:
        DataFrame: Ranked DataFrame
    """
    if idg == "ip":
        order_by_columns = [F.col("ip").desc()]
    else:
        order_by_columns = [
            F.col("diff"),
            F.col("source_count").desc(),
            F.col("de").desc(),  ## ask andy as earlier chv had higher priority
            F.col("chv").desc(),
            F.col("uc").desc(),
            F.col(idg_date_col).desc(),
        ]

    winSpec1 = Window.partitionBy("match_id").orderBy(*order_by_columns)
    df = idg_df.withColumn("rnum", F.row_number().over(winSpec1))
    if idg == "ip":
        return df.where(F.col("rnum") <= 5)
    return df.where(F.col("rnum") <= 10)


def join_with_lookup(
    idg_df: DataFrame,
    lookup_df: DataFrame,
    required_columns: List[str],
) -> DataFrame:
    """
    Join the identifier DataFrame with the lookup DataFrame.

    Args:
        idg_df (DataFrame): Identifier DataFrame
        lookup_df (DataFrame): Lookup DataFrame
        required_columns (List[str]): List of required columns for the final output

    Returns:
        DataFrame: Joined DataFrame with required columns
    """
    joined_df = (
        idg_df.join(lookup_df, on="match_id", how="inner")
        .withColumnRenamed(f"match_id_cid00025", "hhk")
        .select(required_columns)
        .dropDuplicates()
        .cache()
    )
    return joined_df


def calculate_num_partitions(df_count: int, req_cols: List[str]) -> int:
    """
    Calculate the number of partitions needed for a DataFrame based on its size and the
    required columns.

    Args:
        df_count (int): The number of rows in the DataFrame.
        req_cols (List[str]): The list of required columns.

    Returns:
        int: The calculated number of partitions.
    """
    avg_bytes_per_row = len(req_cols) * 50  # Estimate ~50 bytes per column
    estimated_size_bytes = df_count * avg_bytes_per_row
    estimated_size_gb = estimated_size_bytes / (1024**3)
    target_size_gb = 0.9  # Target 900MB to stay safely under 1GB
    num_parts = max(1, int(estimated_size_gb / target_size_gb) + 1)
    return num_parts


def logic_main(
    stck_lookup: DataFrame,
    id_df: DataFrame,
    stck_id: str,
    quick_stats: Stats,
) -> DataFrame:
    """
    Main logic to process stackadapt data

    Args:
        stck_lookup (DataFrame): Stackadapt lookup DataFrame
        id_df (DataFrame): ID DataFrame
        stck_id (str): Stackadapt identifier
        quick_stats (Stats): Stats object to log processing stats

    Returns:
        DataFrame: Processed DataFrame with match IDs
    """
    try:
        startime = datetime.now()
        stck_id_cap = stck_id.upper()

        ## Read Data
        try:
            id_datecol = (
                "last_seen"
                if stck_id == "ip"
                else "" if (stck_id == "hems" or stck_id == "udprn") else "date"
            )
            logger.info(f"date column to be used for {stck_id}: {id_datecol}")

            id_cutoff = 30 if stck_id == "ip" else -1
            logger.info(f"lookup window to be used for {stck_id}: {id_cutoff} days")

            quick_stats.add_stats(
                f"{stck_id_cap} Data Load",
                "Success",
                f"Latest {stck_id} data loaded with {id_df.count():,} records, date column: "
                f"{id_datecol}, id cutoff: {id_cutoff} days",
            )
        except Exception as e:
            quick_stats.add_stats(f"{stck_id_cap} Data Load", "Failed", str(e))
            raise

        ## Format Data
        try:
            if stck_id not in ["hems", "mobile"]:
                idg_fmt = format_graph(id_df, id_datecol, id_cutoff)
                logger.info(
                    f"formatted and filtered out id graph data older than {id_cutoff} days, "
                    f"updated count: {idg_fmt.count()}"
                )

                id_deduped = apply_rank(idg_fmt, stck_id, id_datecol).cache()
                logger.info(f"ranked id graph data, updated count: {id_deduped.count()}")
            else:
                id_deduped = id_df.cache()
                logger.info(
                    f"no formatting required for {stck_id}, updated count: {id_deduped.count()}"
                )

            quick_stats.add_stats(
                f"{stck_id_cap} Data Format",
                "Success",
                f"Formatted {stck_id} data, updated count: {id_deduped.count()}",
            )
        except Exception as e:
            quick_stats.add_stats(f"{stck_id_cap} Data Format", "Failed", str(e))
            raise

        ## Join with lookup
        try:
            columns_required = REQUIRED_COLUMNS[stck_id]
            logger.info(f"columns required for final output: {columns_required}")

            id_with_lookup = join_with_lookup(id_deduped, stck_lookup, columns_required)
            id_lookup_count = id_with_lookup.count()
            logger.info(f"joined {stck_id} data with lookup, updated count: {id_lookup_count}")
            id_deduped.unpersist()

            quick_stats.add_stats(
                f"{stck_id_cap} Data with MatchId",
                "Success",
                f"Joined {stck_id} data with lookup, updated count: {id_lookup_count}",
            )
        except Exception as e:
            quick_stats.add_stats(f"{stck_id_cap} Data with MatchId", "Failed", str(e))
            raise
    except Exception as e:
        logger.exception(e)
        raise
    finally:
        endtime = datetime.now()
        logger.info(f"process completed in {str(endtime - startime).split('.')[0]}")
    return id_with_lookup


def setup_spark_session(app_name: str) -> SparkSession:
    """Setup Spark session.

    Args:
        app_name (str): Name of the Spark application.

    Returns:
        SparkSession: Configured Spark session.
    """
    return SparkSession.builder.appName(app_name).getOrCreate()


def parse_arguments():
    """Parse command line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-m",
        "--mask",
        help="date on which process is run in format YYYY-MM-DD",
        required=True,
    )
    parser.add_argument(
        "-ilp",
        "--idg_lookup_path",
        help="idgraph parent lookup file path",
        required=True,
    )
    parser.add_argument(
        "-ip",
        "--stck_id_path",
        help="stack id name and path to id file as pairs, accepts multiple stack ids",
        action="append",
        nargs=2,
        required=True,
    )
    parser.add_argument(
        "-op",
        "--out_path_template",
        help="template path for cleaned stackadapt data, replaces '[stck_id]' with actual id",
        required=True,
    )
    parser.add_argument(
        "-os",
        "--out_stats_path",
        help="stackadapt output stats is written out to this hdfs json file path",
        required=True,
    )
    return parser.parse_args()


def setup_s3_client() -> boto3.client:
    """Setup and return a boto3 S3 client.

    Returns:
        boto3.client: Configured S3 client.
    """
    return boto3.client("s3")


def main():
    """Main entry point for the script."""
    # Parse arguments
    args = parse_arguments()

    # Setup Spark session
    spark = setup_spark_session(f"stackadapt_generate_data_{args.mask}")

    # Setup S3 client
    s3 = setup_s3_client()

    # Initialize stats object
    quick_stats = Stats()

    try:
        # Read latest ID Graph Lookup Data
        idg_lookup_df = spark.read.parquet(args.idg_lookup_path, header=True)
        idg_lookup_count = idg_lookup_df.count()
        logger.info(
            f"read latest IP graph file from path: {args.idg_lookup_path}, "
            f"total latest ID graph record count: {idg_lookup_count}"
        )
        quick_stats.add_stats(
            "Loaded Stackadapt MatchIds",
            "Success",
            f"Latest Stackadapt lookup count of stackadapt matchids: {idg_lookup_count}",
        )
        # Get required columns
        for stck_id, id_path in args.stck_id_path:
            if stck_id not in REQUIRED_COLUMNS:
                logger.error(f"stackadapt id {stck_id} not recognized, skipping...")
                continue
            logger.info(f"processing stackadapt id: {stck_id}")
            columns_required = REQUIRED_COLUMNS[stck_id]
            # Read latest ID Data
            id_df = spark.read.parquet(id_path, header=True)
            logger.info(
                f"read latest {stck_id} graph file from path: {id_path}, "
                f"total latest {stck_id} graph record count: {id_df.count()}"
            )
            # Dedupe Graphs and Get Dominant Identifier-HHK combinations
            output_df = logic_main(
                idg_lookup_df,
                id_df,
                stck_id,
                quick_stats,
            )
            # Write out final data
            id_lookup_count = output_df.count()
            num_parts = calculate_num_partitions(id_lookup_count, columns_required)
            output_path = args.out_path_template.replace("[stck_id]", stck_id)
            logger.info(f"calculated number of partitions for writing {stck_id} data: {num_parts}")
            output_df.repartition(num_parts).write.option("header", True).mode(
                "overwrite"
            ).parquet(output_path)
            output_df.unpersist()
            quick_stats.add_stats(
                f"{stck_id.upper()} Data Write",
                "Success",
                f"{stck_id} data written",
            )
            logger.info(f"written out the {stck_id} data {output_path}")
            logger.info(f"final record count for {stck_id}: {id_lookup_count}")
        # Write out stats to S3
        save_json_to_s3(s3, quick_stats.get_stats(), args.out_stats_path)
        logger.info(f"written out stats to {args.out_stats_path}")
    except Exception as e:
        logger.exception(f"Error in main processing: {e}")
        raise
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
