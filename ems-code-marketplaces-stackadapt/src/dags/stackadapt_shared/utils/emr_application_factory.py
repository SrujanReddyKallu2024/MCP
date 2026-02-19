"""Creates and deletes EMR applications.
"""
import logging
import re
import subprocess
import sys

from airflow.models import BaseOperator
from airflow.utils.context import Context

from stackadapt_shared.functions import platform
from stackadapt_shared.tags import tags

logging.getLogger().setLevel(logging.INFO)

class DummyEmrServerlessCreateApplicationOperator(BaseOperator):
    """Dummy class for mocking the creation of an emr serverless app and
    returning a dummy application id

    Parameters
    ----------
    BaseOperator : class
        Abstract class for all airflow operators

    Returns
    -------
    str
        dummy application id
    """

    DUMMY_APP_ID = "0a1b2c3d4e5f6g7h"

    def __init__(self, task_id, *args, **kwargs):
        logging.debug("Creating dummy app %s args: %s, kwargs: %s", task_id, args, kwargs)
        super().__init__(task_id=task_id)

    def execute(self, context: Context) -> str:
        print("bombs away")
        return self.DUMMY_APP_ID


def run_me(cmd: list) -> bool:
    """Run command

    Parameters
    ----------
    cmd : list
        Command to run

    Returns
    -------
    bool
        True if processes succeeded
    """
    logging.info("command: %s", cmd)
    logging.info("command line: %s", " ".join(cmd))
    process = subprocess.Popen(" ".join(cmd), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    out, err = process.communicate()
    data = f"info: {str(out)}"
    logging.info(data.replace("\\n", "\n"))
    data = f"output: {str(err)}"
    logging.info(data.replace("\\n", "\n"))
    return process.returncode == 0


class DummyEmrServerlessStartJobOperator(BaseOperator):
    """Dummy class for starting an EMR job as a local spark job

    Parameters
    ----------
    BaseOperator : class
        Abstract class for all airflow operators
    """

    def __init__(self, task_id, *args, **kwargs):
        logging.info("Starting dummy job %s, args: %s, kwargs: %s", task_id, args, kwargs)
        super().__init__(task_id=task_id)
        self._kwargs = kwargs
        print(kwargs)

    def execute(self, context: Context):
        logging.info("Starting dummy job")
        logging.info("kwargs %s", self._kwargs)

        spark_submit = self._kwargs["job_driver"]["sparkSubmit"]
        logging.info("spark_submit %s", spark_submit)

        ep_args = spark_submit["entryPointArguments"]

        for n,ep in enumerate(ep_args):
            while ep.startswith("s3://"):
                ep_args[n] = ep.replace("s3://", "s3a://")
                ep = ep_args[n]

            if ep.startswith("{"):
                import json
                try:
                    json.loads(ep)
                    ep_args[n] = f"\'{ep}\'"
                except json.JSONDecodeError:
                    logging.error(f"probly fine")

        logging.info(f"ep_args {ep_args}")
        args = " ".join(ep_args)
        
        sub_args = spark_submit["sparkSubmitParameters"]
        main = spark_submit["entryPoint"]
        logging.info(f"sub_args {sub_args}")
        logging.info(f"args {args}")
        logging.info(f"main {main}")

        import re
        # main is s3://eec-aws-uk-ms-consumersync-local-code-bucket/repos/ems-code-id-graph/spark/idg_keys_p30_update.py
        # needs to be ./code/ems-code-id-graph/src/spark/idg_keys_p30_update.py
        main = re.sub(r"s3://[^/]+/repos/([^/]+)/", r"./code/\1/src/", main)

        logging.info(f"main {main}")



        zips = None
        if sub_args:
            zipcode = re.findall("--conf spark.submit.pyFiles=([^ ]*)", sub_args)[0]
            logging.info(f"zipcode {zipcode}")
            # zipcode is s3://eec-aws-uk-ms-consumersync-local-code-bucket/zipped/ems-code-shared.zip,s3://eec-aws-uk-ms-consumersync-local-code-bucket/zipped/ems-code-id-graph.zip
            # but needs to be ./code/ems-code-shared.zip,./code/ems-code-id-graph.zip

            
            zipcode = re.sub(
                r"s3://[^/]+/zipped/([a-z_-]*).zip",
                r"./code/\1.zip",
                zipcode,
            )


            logging.info(f"zipcode {zipcode}")

            #zips = ",".join(zipcode.split(":"))
        assert run_me(
            cmd=[
                "/home/airflow/.local/bin/spark-submit",
                "--master",
                "spark://spark-master:7077",
                "--conf",
                "spark.hadoop.fs.s3a.endpoint=http://minio-hello:9000",
                "--conf",
                "spark.hadoop.fs.s3a.access.key=robin",
                "--conf",
                "spark.hadoop.fs.s3a.secret.key=robinrobin",
                "--conf",
                "spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem",
                "--conf",
                "spark.hadoop.fs.s3a.path.style.access=True",
                "--jars",
                "/jars/aws-java-sdk-bundle-1.12.264.jar,/jars/3.3.4/hadoop-common-3.3.4.jar,/jars/3.3.4/hadoop-aws-3.3.4.jar",
                "--py-files",
                f"{main}" + f",{zipcode}" if zipcode else "",
               # "--files",
               # f"{main}" + f",{zips}" if zips else "",
                f"{main}",
                f"{args}",
            ]
        )

        return True


class DummyEmrServerlessDeleteApplicationOperator(BaseOperator):
    """Dummy class for mocking the deletion of an emr serverless app

    Parameters
    ----------
    BaseOperator : class
        Abstract class for all airflow operators
    """

    def __init__(self, task_id, *args, **kwargs):
        logging.debug(
            "Deleting dummy app task_id: %s, args: %s, kwargs: %s", task_id, args, kwargs
        )
        super().__init__(task_id=task_id)

    def execute(self, context: Context) -> bool:
        print("mischief managed")
        return True


def create_emr_application(
    context: Context,
    application_name: str,
    emr_subnet_ids: list = None,
    emr_security_group_ids: list = None,
):
    """Creates an EMR application.

    Parameters
    ----------
    context : Context
        DAG context.
    application_name : str
        The name of the EMR application.
    emr_subnet_ids : list
        The subnet identifiers for the EMR application.
    emr_security_group_ids : list
        The security group identifiers for the EMR application.

    Returns
    -------
    str
        EMR application identifier.
    """
    logging.info("Creating EMR application %s", application_name)
    from airflow.providers.amazon.aws.operators.emr import (
        EmrServerlessCreateApplicationOperator,
    )

    if platform.is_local:
        EmrServerlessCreateApplicationOperator = DummyEmrServerlessCreateApplicationOperator

    network_config = {}
    if emr_subnet_ids:
        network_config["subnetIds"] = emr_subnet_ids
    if emr_security_group_ids:
        network_config["securityGroupIds"] = emr_security_group_ids
    config = {"name": application_name}
    if network_config:
        config["networkConfiguration"] = network_config

    if tags:
        config["tags"] = tags

    application_id = EmrServerlessCreateApplicationOperator(
        task_id=application_name,
        job_type="SPARK",
        release_label="emr-6.12.0",
        config=config,
    ).execute(context)

    logging.info("Created EMR application %s identifier %s", application_name, application_id)
    return application_id


def start_emr_job(
    task_id: str,
    application_id: str,
    execution_role_arn: str,
    job_driver: dict,
    configuration_overrides: dict = None,
):
    """_summary_

    Parameters
    ----------
    task_id : str
        The name of the task to be run.
    application_id : str
        EMR application identifier.
    execution_role_arn : str
        The IAM role used to execute the EMR job
    job_driver : dict
        The spark_submit information required to run the EMR job
        such as "entryPoint", "entryPointArguments" and "sparkSubmitParameters".
    configuration_overrides : dict, optional
        Configuration settings to use in place of the default settings,
        by default None

    Returns
    -------
    _type_
        _description_
    """
    from airflow.providers.amazon.aws.operators.emr import EmrServerlessStartJobOperator

    if platform.is_local:
        EmrServerlessStartJobOperator = DummyEmrServerlessStartJobOperator
        
    configuration_overrides = {} if not configuration_overrides else configuration_overrides
    return EmrServerlessStartJobOperator(
        task_id=task_id,
        application_id=application_id,
        execution_role_arn=execution_role_arn,
        job_driver=job_driver,
        configuration_overrides=configuration_overrides,
    )


def delete_emr_application(
    context: Context, application_name: str, application_id: str,
):
    """Deletes an EMR application.

    Parameters
    ----------
    context : Context
        DAG context.
    application_name : str
        The name of the EMR application.
    application_id : str
        EMR application identifier.
    """
    #logging.info("Deleting EMR application %s identifier %s", application_name, application_id)
    from airflow.providers.amazon.aws.operators.emr import (
        EmrServerlessDeleteApplicationOperator,
    )

    if platform.is_local:
        EmrServerlessDeleteApplicationOperator = DummyEmrServerlessDeleteApplicationOperator

    EmrServerlessDeleteApplicationOperator(
        task_id=application_name, application_id=f"{application_id}", trigger_rule="all_done"
    ).execute(context)

    #logging.info("Deleted EMR application %s identifier %s", application_name, application_id)
