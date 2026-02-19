from pathlib import Path

import boto3
import pytest

from moto import mock_aws


@pytest.fixture(scope="session")
def s3_mock():
    """Creates a mock boto3 s3 client

    Yields
    ------
    boto3.client("s3")
    """
    with mock_aws():
        yield boto3.client("s3", region_name="us-east-1")


@pytest.fixture(scope="session")
def tmp_root_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    """The temporary path used for the root value and to generate file paths

    Parameters
    ----------
    tmp_path_factory : pytest.TempPathFactory
        Built-in fixture to setup and teardown temp file paths

    Returns
    -------
    pathlib.Path
        Temporary file path
    """
    return tmp_path_factory.mktemp("root")
