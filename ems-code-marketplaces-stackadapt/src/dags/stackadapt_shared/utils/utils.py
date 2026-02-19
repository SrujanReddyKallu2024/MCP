"""utility functions for use in other modules
"""

import http
import json
import logging

import boto3
from botocore.exceptions import ClientError

from stackadapt_shared.functions import platform

logging.getLogger(__name__).setLevel(logging.INFO)

# account ids for different environments

accounts = {
    "dev": "298884149518",
    "test": "853660505950",
    "prod": "639298159173",
    "uat": "712395066954",
    "local": "000000000000"
}

def get_emr_serverless_role(env):
    account = accounts.get(env, "639298159173")
    return f"arn:aws:iam::{account}:role/BURoleForEMRServerlessDefault"

def get_aws_parameter(name: str, region: str) -> any:
    """Retrieve a parameter value from the AWS parameter store

    Parameters
    ----------
    name : str
        name of the parameter to retrieve
    region : str
        The aws region

    Returns
    -------
    any
        the parameter value
    """
    ssm = boto3.client("ssm", region_name=region)
    param_json = ssm.get_parameters(Names=[name])
    return param_json["Parameters"][0]["Value"]


def get_aws_secret(secret_id: str, region: str) -> any:
    """Retrieve a secret from the AWS secrets manager

    Parameters
    ----------
    secret_id : str
        the id for the secret to retrieve
    region : str
        The aws region

    Returns
    -------
    any
        the value of the secret

    Raises
    ------
    ClientError
        _description_
    """
    secretsmanager = boto3.client("secretsmanager", region_name=region)
    try:
        response = secretsmanager.get_secret_value(SecretId=secret_id)
    except ClientError as error:
        if error.response["Error"]["Code"] == "ResourceNotFoundException":
            logging.error("The requested secret %s was not found", secret_id)
        elif error.response["Error"]["Code"] == "InvalidRequestException":
            logging.error("The request was invalid due to: %s", error)
        elif error.response["Error"]["Code"] == "InvalidParameterException":
            logging.error("The request had invalid params: %s", error)
        elif error.response["Error"]["Code"] == "DecryptionFailure":
            logging.error(
                "The requested secret can't be decrypted using the provided KMS key: %s", error
            )
        elif error.response["Error"]["Code"] == "InternalServiceError":
            logging.error("An error occurred on service side: %s", error)
            raise ClientError(error.response, error.operation_name) from error
    else:
        if "SecretString" in response:
            return response["SecretString"]
        return response["SecretBinary"]
    return None


# //def get_user_info(region: str) -> dict:
# //    """Retrieve the user info for the current user
# //
# //    Parameters
# //    ----------
# //    region : str
# //        The aws region
# //
# //    Returns
# //    -------
# //    dict
# //        The user info
# //
# //    Raises
# //    ------
# //    ClientError
# //        If the client fails to retrieve details
# //    """
# //    client = boto3.client("sts", region_name=region)
# //    try:
# //        return client.get_caller_identity()
# //    except ClientError as error:
# //        logging.error(error)
# //        raise ClientError(error.response, error.operation_name) from error


def get_default_vpc() -> dict:
    """Get details for default VPC

    Returns
    -------
    dict
        VPC info
    """
    if platform.is_local:
        return None
    
    client = boto3.client("ec2")
    vpcs = client.describe_vpcs()["Vpcs"]
    logging.debug("Found vpcs: %s", vpcs)
    if len(vpcs) > 1:
        vpcs = [x for x in vpcs if x["IsDefault"]]
    default_vpc = vpcs[0]
    vpc_tag_name_list = [x["Value"] for x in default_vpc["Tags"] if x["Key"] == "Name"]
    vpc_name = None if not vpc_tag_name_list else vpc_tag_name_list[0]
    return {
        "name": vpc_name,
        "id": default_vpc["VpcId"],
        "cidrBlock": default_vpc["CidrBlock"],
    }


def vpc_filter(vpc_id: str = None) -> dict:
    """Set dict to filter for  VPC in other functions

    Parameters
    ----------
    vpc_id : str
        The VPC id to filter for

    Returns
    -------
    dict
        Filter dict
    """
    if not vpc_id:
        default_vpc = get_default_vpc()
        vpc_id = default_vpc["id"]
    return {"Name": "vpc-id", "Values": [vpc_id]}


def get_subnets(vpc_id: str = None) -> list:
    """Get list of subnets in VPC

    Parameters
    ----------
    vpc_id : str
        The id for the VPC containing the required subnets

    Returns
    -------
    list
        List of subnets
    """

    
    if platform.is_local:
        return []
    

    client = boto3.client("ec2")
    return client.describe_subnets(Filters=[vpc_filter(vpc_id)])["Subnets"]


def get_security_groups(vpc_id: str = None, group_names: list = None) -> list:
    """Get list of security groups in default VPC

    Parameters
    ----------
    vpc_id : str
        The id for the VPC containing the required security groups

    Returns
    -------
    list
        List of security groups
    """

    
    if platform.is_local:
        return []
    
    client = boto3.client("ec2")
    filters = [vpc_filter(vpc_id)]
    if group_names:
        filters.append({"Name": "group-name", "Values": group_names})
    return client.describe_security_groups(Filters=filters)["SecurityGroups"]


def invoke_lambda(function_name: str, region: str, payload: dict = None) -> dict:
    """Invoke a Lambda function and return the body of the response

    Parameters
    ----------
    client : boto3
        boto3 client for invoking AWS lambda functions
    function_name : str
        The name of the lambda function to invoke
    payload : dict, optional
        The data_date (vintage) of the input data, by default None

    Returns
    -------
    dict
        Body of the lambda response

    Raises
    ------
    AirflowException
        If lambda doesn't return a success status code
    """
    client = boto3.client("lambda", region_name=region)
    payload = {} if not payload else payload
    logging.info("Running lambda %s with payload %s", function_name, payload)
    response = client.invoke(FunctionName=function_name, Payload=json.dumps(payload))
    result = json.loads(response["Payload"].read().decode("utf-8"))
    if result["statusCode"] != http.HTTPStatus.OK:
        raise RuntimeError(f"function {function_name} returned error: {result}")
    return result["body"]
