import json
import logging
from argparse import ArgumentParser
from typing import Dict

import boto3
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from sql_metadata import Parser

logger = logging.getLogger(__name__)

_SECRETS = "ibm-ccf-secrets"
_LOG_LEVEL = "ALL"


def get_aws_client(service: str, region: str = "us-east-1"):
    """
    Method to get an AWS client for given service
    """
    return boto3.client(
        service,
        region_name=region,
    )


def get_s3_object(bucket: str, key: str) -> Dict:
    """
    Method to get an S3 object
     for given bucket and key
    """
    aws_client = get_aws_client("s3")
    return aws_client.get_object(Bucket=bucket, Key=key)


def get_secret_value(secret_name: str) -> dict:
    """
    Method to get a secret value for secret name
    """
    aws_client = get_aws_client("secretsmanager")
    return aws_client.get_secret_value(SecretId=secret_name)


# noinspection PyShadowingNames
def _get_table_mapping(query: str) -> Dict:
    catalog_details = json.loads(
        get_secret_value(secret_name=_SECRETS).get("SecretString")
    )
    catalog_object = get_s3_object(
        bucket=catalog_details.get("CATALOG_BUCKET"),
        key=catalog_details.get("CATALOG_KEY"),
    ).get("Body")
    catalog = json.loads(catalog_object.read())

    mapping = dict()
    for table_name in Parser(query).tables:
        file = f"{table_name}.parquet"
        file_path = catalog.get(file).replace("s3", "s3a")
        logger.info(
            f"{job_id}-> Setting mapping for name:{table_name}, with path={file_path}"
        )
        mapping[table_name] = spark.read.parquet(f"{file_path}/*.parquet")
    return mapping


# noinspection PyShadowingNames
def _create_view(dataframe: DataFrame, name: str) -> None:
    dataframe.createOrReplaceTempView(name)


if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument(
        "query",
        type=str,
        help="Required SQL query to be performed.",
    )
    parser.add_argument(
        "destination",
        type=str,
        help="Required destination path for generated output.",
    )
    parser.add_argument(
        "id",
        type=str,
        help="Required ID for generated output.",
    )

    args = vars(parser.parse_args())
    query = args.get("query")
    destination = args.get("destination").replace("s3", "s3a")
    job_id = args.get("id")

    aws_credentials = json.loads(
        get_secret_value(secret_name=_SECRETS).get("SecretString")
    )

    conf = SparkConf()
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
    )
    conf.set("spark.hadoop.fs.s3a.access.key", aws_credentials.get("ACCESS_KEY"))
    conf.set("spark.hadoop.fs.s3a.secret.key", aws_credentials.get("SECRET_ACCESS_KEY"))

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel(_LOG_LEVEL)

    mapping = _get_table_mapping(query)

    for file_name, dataframe in mapping.items():
        _create_view(dataframe, file_name)

    subset = spark.sql(query)
    subset.write.parquet(f"{destination}/{job_id}")

    logger.info(
        f"{job_id}-> Successfully written output to destination: {destination}/{job_id}"
    )
