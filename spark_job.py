import json
import logging
from argparse import ArgumentParser
from typing import Dict

import boto3
from datetime import datetime
from pyspark import SparkConf
from pyspark.sql import DataFrame, SparkSession
from sql_metadata import Parser

logger = logging.getLogger(__name__)

_LOG_LEVEL = "ALL"
_ROLE_SESSION_NAME = "AssumeRoleSession"


def get_aws_client(service: str, region: str):
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
    aws_client = get_aws_client("s3", region=region)
    return aws_client.get_object(Bucket=bucket, Key=key)


def get_secret_value(secret_name: str):
    """
    Method to get a secret value for secret name
    """
    aws_client = get_aws_client("secretsmanager", region=region)
    return aws_client.get_secret_value(SecretId=secret_name)


def get_assumed_role_object(role_arn: str) -> Dict:
    """
    Method to get role object
    """
    aws_client = get_aws_client("sts", region=region)
    return aws_client.assume_role(
        RoleArn=role_arn,
        RoleSessionName=_ROLE_SESSION_NAME,
    )


# noinspection PyShadowingNames
def _get_table_mapping(query: str) -> Dict:
    catalog_details = json.loads(
        get_secret_value(secret_name=secrets).get("SecretString")
    )
    catalog_object = get_s3_object(
        bucket=catalog_details.get("CATALOG_BUCKET_NAME"),
        key=catalog_details.get("CATALOG_KEY"),
    ).get("Body")
    catalog = json.loads(catalog_object.read())

    mapping = dict()
    for table_name in Parser(query).tables:
        file_path = catalog.get(table_name).replace("s3", "s3a")
        print(
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
        "id",
        type=str,
        help="Required ID for generated output.",
    )
    parser.add_argument(
        "secrets",
        type=str,
        help="Required secrets for credentials.",
    )
    parser.add_argument(
        "region",
        type=str,
        help="AWS region.",
    )

    args = vars(parser.parse_args())

    query = args.get("query")
    job_id = args.get("id")
    secrets = args.get("secrets")
    region = args.get("region")

    role_arn = json.loads(
        get_secret_value(secret_name=secrets).get("SecretString")
    ).get("ASSUMED_ROLE_ARN")

    aws_credentials = \
        get_assumed_role_object(role_arn=role_arn).get("Credentials")

    destination = json.loads(
        get_secret_value(secret_name=secrets).get("SecretString")
    ).get("INTERNAL_OUTPUT_DESTINATION")
    destination = destination.replace("s3", "s3a")

    conf = SparkConf()
    conf.set("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.3.4")
    conf.set(
        "spark.hadoop.fs.s3a.aws.credentials.provider",
        "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    )
    conf.set(
        "spark.hadoop.fs.s3a.access.key",
        aws_credentials.get("AccessKeyId"),
    )
    conf.set(
        "spark.hadoop.fs.s3a.secret.key",
        aws_credentials.get("SecretAccessKey"),
    )
    conf.set(
        "spark.hadoop.fs.s3a.session.token",
        aws_credentials.get("SessionToken"),
    )

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel(_LOG_LEVEL)

    mapping = _get_table_mapping(query)

    for file_name, dataframe in mapping.items():
        _create_view(dataframe, file_name)

    start_time = datetime.now()
    subset = spark.sql(query)
    end_time = datetime.now()
    print(f"{job_id}-> Starting query: {query} execution on time: {start_time}")
    print(
        f"{job_id}-> Successfully executed query: {query} execution on time:"
        f" {end_time}"
    )

    print(
        f"{job_id}-> Total time taken in query: {query} execution:"
        f" {end_time - start_time}"
    )

    subset.write.parquet(f"{destination}/{job_id}")

    print(
        f"{job_id}-> Successfully written output to destination:"
        f" {destination}/{job_id}"
    )
