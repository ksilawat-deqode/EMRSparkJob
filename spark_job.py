import json
import boto3
import logging
from argparse import ArgumentParser
from typing import Dict

from pyspark import SparkConf
from pyspark.sql import DataFrame
from pyspark.sql import SparkSession
from sql_metadata import Parser

logger = logging.getLogger(__name__)


def get_aws_client(service: str):
    """
    Method to get an AWS client for given service
    """
    return boto3.client(
        service,
        region_name='us-east-1',
    )

def get_s3_object(bucket: str, key: str) -> Dict:
    """
    Method to get an S3 object
     for given bucket and key
    """
    aws_client = get_aws_client("s3")
    return aws_client.get_object(Bucket=bucket, Key=key)


# noinspection PyShadowingNames
def _get_table_mapping(query: str) -> Dict:
    obj = get_s3_object(bucket='', key='').get('Body')
    catalog = json.loads(obj.read())

    mapping = dict()
    for table_name in Parser(query).tables:
        file = f'{table_name}.parquet'
        file_path = catalog.get(file).replace('s3', 's3a')
        logger.info(
            f'{job_id}-> Setting mapping for name:{table_name}, with path={file_path}'
        )
        mapping[table_name] = spark.read.parquet(f"{file_path}/*.parquet")
    return mapping


# noinspection PyShadowingNames
def _create_view(dataframe: DataFrame, name: str) -> None:
    dataframe.createOrReplaceTempView(name)


if __name__ == '__main__':
    parser = ArgumentParser()
    parser.add_argument(
        'query',
        type=str,
        help='Required SQL query to be performed.',
    )
    parser.add_argument(
        'destination',
        type=str,
        help='Required destination path for generated output.',
    )
    parser.add_argument(
        'id',
        type=str,
        help='Required ID for generated output.',
    )

    args = vars(parser.parse_args())
    query = args.get('query')
    destination = args.get('destination').replace('s3', 's3a')
    job_id = args.get('id')

    conf = SparkConf()
    conf.set('spark.jars.packages', 'org.apache.hadoop:hadoop-aws:3.3.4')
    conf.set(
        'spark.hadoop.fs.s3a.aws.credentials.provider',
        'org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider',
    )
    conf.set('spark.hadoop.fs.s3a.access.key', '')
    conf.set('spark.hadoop.fs.s3a.secret.key', '')

    spark = SparkSession.builder.config(conf=conf).getOrCreate()
    spark.sparkContext.setLogLevel('ALL')
    
    mapping = _get_table_mapping(query)

    for file_name, dataframe in mapping.items():
        _create_view(dataframe, file_name)

    subset = spark.sql(query)
    subset.write.parquet(f'{destination}/{job_id}')

    logger.info(f'{job_id}-> Successfully written output to destination: {destination}/{job_id}')
