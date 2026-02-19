import json
import logging
import time
import boto3
import fnmatch
from datetime import datetime, timedelta

from airflow import DAG
from airflow.models.param import Param
from airflow.operators.python import PythonOperator, get_current_context
from airflow.operators.dummy import DummyOperator
from airflow.operators.email import EmailOperator
from airflow.exceptions import AirflowSkipException

import pendulum
import sys
from pathlib import Path

sys.path.append(str(Path(__file__).resolve().parent))

logging.getLogger(__name__).setLevel(logging.INFO)

import shared.functions
import shared.utils.utils
import shared.bucket_list

from shared.functions import get_env, parse_s3_path, email_on_failure
from shared.bucket_list import get_bucket_name
from shared.utils.emr_application_factory import create_emr_application, delete_emr_application, start_emr_job


STCK_IDS = ["mobile", "hems", "maids", "uid", "ip"]

S3_PREFIX_MAP = {
    "hems": "hem",
    "maids": "device",
    "uid": "id5",
    "mobile": "hashed_mobile",
    "ip": "ip"
}


def resolve_s3_pattern(s3_path_pattern):
    """
    Resolve S3 pattern to actual path.
    Handles both single files and parquet directories.
    """
    if s3_path_pattern.startswith("s3://"):
        s3_path_pattern = s3_path_pattern[5:]
    elif s3_path_pattern.startswith("s3a://"):
        s3_path_pattern = s3_path_pattern[6:]

    parts = s3_path_pattern.split("/", 1)
    bucket_name = parts[0]
    key_pattern = parts[1] if len(parts) > 1 else ""

    try:
        s3 = boto3.client("s3")

        if "*" not in key_pattern:
            return f"s3://{bucket_name}/{key_pattern}"

        # Get the prefix before the wildcard
        prefix = key_pattern.split("*")[0]
        # Get the parent directory
        parent_prefix = "/".join(prefix.rstrip("/").rsplit("/", 1)[:-1])
        if parent_prefix:
            parent_prefix += "/"

        # List with delimiter to get "folders"
        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=parent_prefix, Delimiter="/")

        matched_items = []

        for page in page_iterator:
            # Check for folders (CommonPrefixes)
            if "CommonPrefixes" in page:
                for common_prefix in page["CommonPrefixes"]:
                    folder_path = common_prefix["Prefix"].rstrip("/")
                    if fnmatch.fnmatch(folder_path, key_pattern.rstrip("/")):
                        matched_items.append(("folder", folder_path))

            # Check for single files (Contents)
            if "Contents" in page:
                for obj in page["Contents"]:
                    obj_key = obj["Key"]
                    if fnmatch.fnmatch(obj_key, key_pattern):
                        matched_items.append(("file", obj_key))

        if not matched_items:
            return None

        # Sort to get latest (by name, descending)
        matched_items.sort(key=lambda x: x[1], reverse=True)
        item_type, item_path = matched_items[0]
        
        logging.info(f"Resolved pattern '{key_pattern}' to {item_type}: {item_path}")
        return f"s3://{bucket_name}/{item_path}"

    except Exception as e:
        logging.error(f"Error resolving S3 pattern: {e}")
        return None


def file_exists_with_pattern(bucket_name, key_pattern):
    try:
        s3 = boto3.client("s3")

        if "*" in key_pattern:
            prefix = key_pattern.split("*")[0]
        else:
            try:
                s3.head_object(Bucket=bucket_name, Key=key_pattern)
                return True
            except Exception:
                return False

        paginator = s3.get_paginator("list_objects_v2")
        page_iterator = paginator.paginate(Bucket=bucket_name, Prefix=prefix)

        for page in page_iterator:
            if "Contents" in page:
                for obj in page["Contents"]:
                    if fnmatch.fnmatch(obj["Key"], key_pattern):
                        return True

        return False

    except Exception:
        return False


with DAG(
    "stackadapt_processing",
    description="Generate StackAdapt marketplace data",
    catchup=False,
    start_date=pendulum.datetime(2023, 1, 1, tz="UTC"),
    schedule_interval="0 12 * * 1",
    tags=["stackadapt", "marketplace"],
    params={
        "email_to": Param("kallusrujan.reddy@experian.com", type="string"),
    },
    default_args={
        "owner": "airflow",
        "depends_on_past": False,
        "email_on_failure": False,
        "email_on_retry": False,
        "retries": 0,
        "on_failure_callback": email_on_failure,
    },
) as dag:

    def create_script_arguments(**context):
        workflow_start_time = time.time()
        context["task_instance"].xcom_push(key="workflow_start_time", value=workflow_start_time)

        env = get_env()
        execution_date = context["execution_date"]
        run_date = execution_date.strftime("%Y-%m-%d")

        context["task_instance"].xcom_push(key="env", value=env)
        context["task_instance"].xcom_push(key="run_date", value=run_date)

        source_bucket = get_bucket_name("source")
        idgraph_bucket = get_bucket_name("idgraph")
        output_bucket = get_bucket_name("output")
        code_bucket = get_bucket_name("code")
        log_bucket = get_bucket_name("logs")
        stats_bucket = get_bucket_name("stats")

        context["task_instance"].xcom_push(key="source_bucket", value=source_bucket)
        context["task_instance"].xcom_push(key="idgraph_bucket", value=idgraph_bucket)
        context["task_instance"].xcom_push(key="output_bucket", value=output_bucket)
        context["task_instance"].xcom_push(key="code_bucket", value=code_bucket)
        context["task_instance"].xcom_push(key="log_bucket", value=log_bucket)
        context["task_instance"].xcom_push(key="stats_bucket", value=stats_bucket)

        input_files = {
            "lookup": f"s3://{idgraph_bucket}/lookup/cid00025/match_id_lookup_cid00025_*.parquet",
            "mobile": f"s3://{idgraph_bucket}/mobile/mobile_*.parquet",
            "hems": f"s3://{idgraph_bucket}/hems/hems_*.parquet",
            "maids": f"s3://{idgraph_bucket}/maids/maids_*.parquet",
            "uid": f"s3://{idgraph_bucket}/uid/uid_*.parquet",
            "ip": f"s3://{idgraph_bucket}/ip/ip_*.parquet",
        }

        output_base = f"s3://{output_bucket}/marketplaces/stackadapt/{run_date}"
        output_template = f"{output_base}/[stck_id]_graph.parquet"

        context["task_instance"].xcom_push(key="input_files", value=input_files)
        context["task_instance"].xcom_push(key="output_base", value=output_base)
        context["task_instance"].xcom_push(key="output_template", value=output_template)

        # EMR setup
        execution_role_arn = shared.utils.utils.get_emr_serverless_role(env)
        log_uri_prefix = f"s3://{log_bucket}/spark-logs"
        shared_code = [f"s3://{code_bucket}/zipped/ems-code-shared.zip"]
        stck_code_prefix = f"s3://{code_bucket}/repos/ems-code-marketplaces-stackadapt/spark"
        stats_script = f"{stck_code_prefix}/get_stats_emr.py"

        context["task_instance"].xcom_push(key="execution_role_arn", value=execution_role_arn)
        context["task_instance"].xcom_push(key="log_uri_prefix", value=log_uri_prefix)
        context["task_instance"].xcom_push(key="shared_code", value=shared_code)
        context["task_instance"].xcom_push(key="stck_code_prefix", value=stck_code_prefix)
        context["task_instance"].xcom_push(key="stats_script", value=stats_script)
        context["task_instance"].xcom_push(key="stats_path", value=f"s3://{stats_bucket}/stackadapt")

        return True

    def check_inputs_exist(**context):
        """Check all required input files exist. If any missing, skip entire DAG."""
        input_files = context["ti"].xcom_pull(key="input_files", task_ids="create_script_arguments")
        run_date = context["ti"].xcom_pull(key="run_date", task_ids="create_script_arguments")
        recipients = [context["params"].get("email_to", "kallusrujan.reddy@experian.com")]

        missing_files = []
        for file_key, file_path in input_files.items():
            try:
                bucket, key = parse_s3_path(file_path)
                if not file_exists_with_pattern(bucket, key):
                    missing_files.append(file_key)
                    logging.warning(f"Missing: {file_key} at {file_path}")
                else:
                    logging.info(f"Found: {file_key}")
            except Exception as e:
                missing_files.append(file_key)
                logging.error(f"Error checking {file_key}: {e}")

        context["task_instance"].xcom_push(key="missing_files", value=missing_files)

        if missing_files:
            # Send email about missing files
            html_content = f"""
            <h2>StackAdapt - Missing Input Files</h2>
            <p><b>Date:</b> {run_date}</p>
            <p><b>Missing Files:</b> {', '.join(missing_files)}</p>
            <p>Processing skipped. No EMR resources created.</p>
            """
            email_op = EmailOperator(
                task_id="missing_files_email",
                to=recipients,
                subject=f"StackAdapt SKIPPED - Missing Files - {run_date}",
                html_content=html_content,
                dag=dag,
            )
            email_op.execute(context=context)

            raise AirflowSkipException(f"Missing input files: {missing_files}")

        logging.info("All required input files exist")
        return True

    def initialise(**context):
        date = datetime.now().strftime("%Y%m%d%H%M%S")
        emr_application_name = f"stackadapt-{date}"

        app_id = create_emr_application(context, emr_application_name, None, None)

        context["task_instance"].xcom_push(key="application_id", value=app_id)
        context["task_instance"].xcom_push(key="emr_application_name", value=emr_application_name)

        return f"Created EMR application: {app_id}"

    def collect_file_stats(context, file_name: str, file_key: str):
        app_id = context["task_instance"].xcom_pull(key="application_id", task_ids="initialise")
        execution_role_arn = context["task_instance"].xcom_pull(key="execution_role_arn", task_ids="create_script_arguments")
        log_uri_prefix = context["task_instance"].xcom_pull(key="log_uri_prefix", task_ids="create_script_arguments")
        shared_code = context["task_instance"].xcom_pull(key="shared_code", task_ids="create_script_arguments")
        stats_script = context["task_instance"].xcom_pull(key="stats_script", task_ids="create_script_arguments")
        input_files = context["task_instance"].xcom_pull(key="input_files", task_ids="create_script_arguments")
        run_date = context["task_instance"].xcom_pull(key="run_date", task_ids="create_script_arguments")
        stats_path = context["task_instance"].xcom_pull(key="stats_path", task_ids="create_script_arguments")

        input_file_path = resolve_s3_pattern(input_files[file_key])
        if not input_file_path:
            raise ValueError(f"No file found for {file_key}")

        job = start_emr_job(
            task_id=f"collect_{file_name.lower()}_stats",
            application_id=app_id,
            execution_role_arn=execution_role_arn,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": stats_script,
                    "entryPointArguments": ["-m", run_date, "-if", input_file_path, "-sp", stats_path],
                    "sparkSubmitParameters": f"--conf spark.submit.pyFiles={','.join(shared_code)}",
                }
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {
                        "logUri": f"{log_uri_prefix}/stackadapt_{file_name.lower()}_stats",
                    }
                }
            },
        )

        job_id = job.execute(get_current_context())
        return f"Collected {file_name} stats: {job_id}"

    def collect_lookup_stats(**context):
        return collect_file_stats(context, "Lookup", "lookup")

    def collect_mobile_stats(**context):
        return collect_file_stats(context, "Mobile", "mobile")

    def collect_hems_stats(**context):
        return collect_file_stats(context, "HEMS", "hems")

    def collect_maids_stats(**context):
        return collect_file_stats(context, "MAIDS", "maids")

    def collect_uid_stats(**context):
        return collect_file_stats(context, "UID", "uid")

    def collect_ip_stats(**context):
        return collect_file_stats(context, "IP", "ip")

    def send_input_stats_email(**context):
        """Send consolidated input stats email after all stats collected - matches digital_taxonomy"""
        import tempfile
        import os
        
        run_date = context["task_instance"].xcom_pull(key="run_date", task_ids="create_script_arguments")
        stats_bucket = context["task_instance"].xcom_pull(key="stats_bucket", task_ids="create_script_arguments")
        recipients = [context["params"].get("email_to", "kallusrujan.reddy@experian.com")]

        # Calculate week start date
        current_date = datetime.strptime(run_date, "%Y-%m-%d")
        week_start_date = (current_date - timedelta(days=current_date.weekday())).strftime("%Y-%m-%d")
        if current_date.weekday() == 0:
            week_start_date = run_date

        s3 = boto3.client("s3")

        # First try JSON email data (contains HTML table, created by get_stats_emr.py)
        email_json_key = f"stackadapt/{week_start_date}/email_data_weekly_{week_start_date}.json"
        try:
            logging.info(f"Looking for email JSON at s3://{stats_bucket}/{email_json_key}")
            email_response = s3.get_object(Bucket=stats_bucket, Key=email_json_key)
            email_data = json.loads(email_response["Body"].read().decode("utf-8"))

            email_content = email_data.get("html_content", f"<p>No detailed statistics available for {run_date}</p>")

            stats_email = EmailOperator(
                task_id="send_input_stats_email_json",
                to=recipients,
                subject=f"StackAdapt Input Statistics - {run_date}",
                html_content=email_content,
                dag=dag,
            )
            stats_email.execute(context=context)
            logging.info(f"Sent input stats email using JSON data")
            return f"Sent input stats email for {run_date} using JSON data"

        except Exception as json_err:
            logging.warning(f"No email JSON found, trying CSV file: {json_err}")

            # Fallback: Try CSV attachment
            stats_file_key = f"stackadapt/{week_start_date}/pipeline_stats__{week_start_date}.csv"
            try:
                logging.info(f"Looking for stats CSV at s3://{stats_bucket}/{stats_file_key}")
                
                with tempfile.NamedTemporaryFile(delete=False, suffix=".csv") as temp_file:
                    s3.download_fileobj(stats_bucket, stats_file_key, temp_file)
                    temp_file_path = temp_file.name

                html_content = f"""
                <h2>StackAdapt Input Statistics - {run_date}</h2>
                <p>Please find attached the input statistics report for:</p>
                <ul>
                    <li>Lookup - cid00025 match IDs</li>
                    <li>Mobile - hashed mobile graph</li>
                    <li>HEMS - hashed email graph</li>
                    <li>MAIDS - mobile advertising IDs</li>
                    <li>UID - UID2.0 graph</li>
                    <li>IP - IP address graph</li>
                </ul>
                <p><em>This is an automated message from the StackAdapt processing system.</em></p>
                """

                email_op = EmailOperator(
                    task_id="input_stats_email_csv",
                    to=recipients,
                    subject=f"StackAdapt Input Statistics - {run_date}",
                    html_content=html_content,
                    files=[temp_file_path],
                    dag=dag,
                )
                email_op.execute(context=context)
                
                os.unlink(temp_file_path)
                logging.info(f"Sent input stats email with CSV attachment")
                return f"Sent input stats email for {run_date} with CSV attachment"

            except Exception as csv_err:
                logging.warning(f"Could not get stats CSV: {csv_err}")
                # Last resort: basic email
                html_content = f"""
                <h2>StackAdapt Input Statistics - {run_date}</h2>
                <p>Stats collected for: Lookup, Mobile, HEMS, MAIDS, UID, IP</p>
                <p><em>Detailed stats file not available.</em></p>
                """
                email_op = EmailOperator(
                    task_id="input_stats_email_basic",
                    to=recipients,
                    subject=f"StackAdapt Input Statistics - {run_date}",
                    html_content=html_content,
                    dag=dag,
                )
                email_op.execute(context=context)
                return f"Sent basic input stats email for {run_date}"

    def run_stackadapt_processing(**context):
        app_id = context["task_instance"].xcom_pull(key="application_id", task_ids="initialise")
        execution_role_arn = context["task_instance"].xcom_pull(key="execution_role_arn", task_ids="create_script_arguments")
        log_uri_prefix = context["task_instance"].xcom_pull(key="log_uri_prefix", task_ids="create_script_arguments")
        shared_code = context["task_instance"].xcom_pull(key="shared_code", task_ids="create_script_arguments")
        stck_code_prefix = context["task_instance"].xcom_pull(key="stck_code_prefix", task_ids="create_script_arguments")
        input_files = context["task_instance"].xcom_pull(key="input_files", task_ids="create_script_arguments")
        output_template = context["task_instance"].xcom_pull(key="output_template", task_ids="create_script_arguments")
        stats_bucket = context["task_instance"].xcom_pull(key="stats_bucket", task_ids="create_script_arguments")
        run_date = context["task_instance"].xcom_pull(key="run_date", task_ids="create_script_arguments")

        stats_path = f"s3://{stats_bucket}/stackadapt/stackadapt_processing_{run_date}.json"

        lookup_path = resolve_s3_pattern(input_files["lookup"])
        if not lookup_path:
            raise ValueError("No lookup file found")

        job_args = ["-m", run_date, "-ilp", lookup_path, "-op", output_template, "-os", stats_path]

        for stck_id in STCK_IDS:
            graph_path = resolve_s3_pattern(input_files[stck_id])
            if graph_path:
                job_args.extend(["-ip", stck_id, graph_path])

        script_path = f"{stck_code_prefix}/stck01_generate_data.py"

        from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

        job = EmrServerlessStartJobOperator(
            task_id="run_stackadapt_emr",
            application_id=app_id,
            execution_role_arn=execution_role_arn,
            job_driver={
                "sparkSubmit": {
                    "entryPoint": script_path,
                    "entryPointArguments": job_args,
                    "sparkSubmitParameters": f"--conf spark.submit.pyFiles={','.join(shared_code)}",
                },
            },
            configuration_overrides={
                "monitoringConfiguration": {
                    "s3MonitoringConfiguration": {"logUri": f"{log_uri_prefix}/stackadapt_main"}
                }
            },
            wait_for_completion=True,
            waiter_delay=60,
            waiter_max_attempts=120,
            aws_conn_id="aws_default",
            dag=dag,
        )

        job_id = job.execute(get_current_context())
        return f"StackAdapt processing complete: {job_id}"

    def rename_output_files(**context):
        s3 = boto3.client("s3")
        output_base = context["task_instance"].xcom_pull(key="output_base", task_ids="create_script_arguments")
        run_date = context["task_instance"].xcom_pull(key="run_date", task_ids="create_script_arguments")

        path_without_prefix = output_base[5:] if output_base.startswith("s3://") else output_base
        bucket = path_without_prefix.split("/")[0]
        base_prefix = "/".join(path_without_prefix.split("/")[1:])

        renamed_count = 0
        for stck_id in STCK_IDS:
            s3_prefix = S3_PREFIX_MAP[stck_id]
            folder_prefix = f"{base_prefix}/{stck_id}_graph.parquet/"

            paginator = s3.get_paginator("list_objects_v2")
            page_iterator = paginator.paginate(Bucket=bucket, Prefix=folder_prefix)

            part_num = 1
            for page in page_iterator:
                if "Contents" in page:
                    for obj in page["Contents"]:
                        obj_key = obj["Key"]
                        if "part-" in obj_key and obj_key.endswith(".parquet"):
                            new_key = f"{base_prefix}/{s3_prefix}__{stck_id}_{run_date}-part{str(part_num).zfill(3)}.parquet"
                            s3.copy_object(Bucket=bucket, CopySource={"Bucket": bucket, "Key": obj_key}, Key=new_key)
                            s3.delete_object(Bucket=bucket, Key=obj_key)
                            renamed_count += 1
                            part_num += 1

        return f"Renamed {renamed_count} files"

    def send_success_email(**context):
        run_date = context["task_instance"].xcom_pull(key="run_date", task_ids="create_script_arguments")
        output_base = context["task_instance"].xcom_pull(key="output_base", task_ids="create_script_arguments")
        recipients = [context["params"].get("email_to", "kallusrujan.reddy@experian.com")]

        html_content = f"""
        <h2>StackAdapt Processing Complete</h2>
        <p><b>Date:</b> {run_date}</p>
        <p><b>Output:</b> {output_base}</p>
        <p><b>Identifiers:</b> mobile, hems, maids, uid, ip</p>
        <p>Processing completed successfully.</p>
        """

        email_op = EmailOperator(
            task_id="success_email_task",
            to=recipients,
            subject=f"StackAdapt Complete - {run_date}",
            html_content=html_content,
            dag=dag,
        )
        email_op.execute(context=context)
        return "Success email sent"

    def send_failure_email(**context):
        run_date = context["task_instance"].xcom_pull(key="run_date", task_ids="create_script_arguments")
        recipients = [context["params"].get("email_to", "kallusrujan.reddy@experian.com")]

        failed_tasks = [t.task_id for t in context["dag_run"].get_task_instances() if t.state == "failed"]

        html_content = f"""
        <h2 style="color:red;">StackAdapt Processing FAILED</h2>
        <p><b>Date:</b> {run_date}</p>
        <p><b>Failed Tasks:</b> {', '.join(failed_tasks) if failed_tasks else 'Unknown'}</p>
        <p>Please check Airflow logs.</p>
        """

        email_op = EmailOperator(
            task_id="failure_email_task",
            to=recipients,
            subject=f"StackAdapt FAILED - {run_date}",
            html_content=html_content,
            dag=dag,
        )
        email_op.execute(context=context)
        return "Failure email sent"

    def finalise(**context):
        app_id = context["task_instance"].xcom_pull(key="application_id", task_ids="initialise")
        emr_application_name = context["task_instance"].xcom_pull(key="emr_application_name", task_ids="initialise")
        
        if app_id:
            delete_emr_application(context, emr_application_name, app_id)
            logging.info(f"Deleted EMR application: {app_id}")
        return "EMR application deleted"

    # Task definitions
    start = DummyOperator(task_id="start", dag=dag)

    task_create_arguments = PythonOperator(
        task_id="create_script_arguments",
        python_callable=create_script_arguments,
        dag=dag,
    )

    task_check_inputs = PythonOperator(
        task_id="check_inputs_exist",
        python_callable=check_inputs_exist,
        dag=dag,
    )

    task_initialise = PythonOperator(
        task_id="initialise",
        python_callable=initialise,
        dag=dag,
    )

    task_collect_lookup_stats = PythonOperator(
        task_id="collect_lookup_stats",
        python_callable=collect_lookup_stats,
        dag=dag,
    )

    task_collect_mobile_stats = PythonOperator(
        task_id="collect_mobile_stats",
        python_callable=collect_mobile_stats,
        dag=dag,
    )

    task_collect_hems_stats = PythonOperator(
        task_id="collect_hems_stats",
        python_callable=collect_hems_stats,
        dag=dag,
    )

    task_collect_maids_stats = PythonOperator(
        task_id="collect_maids_stats",
        python_callable=collect_maids_stats,
        dag=dag,
    )

    task_collect_uid_stats = PythonOperator(
        task_id="collect_uid_stats",
        python_callable=collect_uid_stats,
        dag=dag,
    )

    task_collect_ip_stats = PythonOperator(
        task_id="collect_ip_stats",
        python_callable=collect_ip_stats,
        dag=dag,
    )

    task_send_input_stats_email = PythonOperator(
        task_id="send_input_stats_email",
        python_callable=send_input_stats_email,
        dag=dag,
    )

    task_run_processing = PythonOperator(
        task_id="run_stackadapt_processing",
        python_callable=run_stackadapt_processing,
        execution_timeout=timedelta(hours=4),
        dag=dag,
    )

    task_rename_files = PythonOperator(
        task_id="rename_output_files",
        python_callable=rename_output_files,
        dag=dag,
    )

    task_send_success_email = PythonOperator(
        task_id="send_success_email",
        python_callable=send_success_email,
        dag=dag,
    )

    task_send_failure_email = PythonOperator(
        task_id="send_failure_email",
        python_callable=send_failure_email,
        trigger_rule="one_failed",
        dag=dag,
    )

    task_finalise = PythonOperator(
        task_id="finalise",
        python_callable=finalise,
        trigger_rule="all_done",
        dag=dag,
    )

    # DAG Flow:
    # 1. create_script_arguments - setup paths
    # 2. check_inputs_exist - if missing, skip all and send email (no EMR created)
    # 3. initialise - create EMR only after files confirmed
    # 4. stats collection → send input stats email
    # 5. main processing → rename → success email
    # 6. finalise - always runs to cleanup EMR

    start >> task_create_arguments >> task_check_inputs >> task_initialise

    (
        task_initialise
        >> task_collect_lookup_stats
        >> task_collect_mobile_stats
        >> task_collect_hems_stats
        >> task_collect_maids_stats
        >> task_collect_uid_stats
        >> task_collect_ip_stats
        >> task_send_input_stats_email
        >> task_run_processing
        >> task_rename_files
        >> task_send_success_email
        >> task_finalise
    )

    # Failure handling - tasks after EMR created need cleanup
    [
        task_initialise,
        task_collect_lookup_stats,
        task_collect_mobile_stats,
        task_collect_hems_stats,
        task_collect_maids_stats,
        task_collect_uid_stats,
        task_collect_ip_stats,
        task_send_input_stats_email,
        task_run_processing,
        task_rename_files,
    ] >> task_send_failure_email >> task_finalise

