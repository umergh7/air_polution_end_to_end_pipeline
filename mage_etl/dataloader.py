from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.s3 import S3
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
import pandas as pd
import requests
import boto3
from fastavro import reader
from io import BytesIO
import json
import pprint


@data_loader
def load_from_s3_bucket(*args, **kwargs):
    """
    Template for loading data from a S3 bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#s3
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = 'test-airbyte-ug1'
    s3_prefix = 's3:/test-airbyte-ug1/data/weather_stream/'
    s3_client = boto3.client('s3')
    objects = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=s3_prefix)

    all_data = []

    
    for obj in objects.get('Contents', []):
        response = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
        jsonl_data = response['Body'].read().decode('utf-8').splitlines()

        for json_string in jsonl_data:
            try:
                record = json.loads(json_string)
                coord = record['_airbyte_data']['coord']
                components = record['_airbyte_data']['list'][0]['components']
                dt = record['_airbyte_data']['list'][0]['dt']
                aqi = record['_airbyte_data']['list'][0]['main']['aqi']

                flattened_data = {
                    'lat': coord['lat'],
                    'lon': coord['lon'],
                    'co': components['co'],
                    'nh3': components['nh3'],
                    'no': components['no'],
                    'no2': components['no2'],
                    'o3': components['o3'],
                    'pm10': components['pm10'],
                    'pm2_5': components['pm2_5'],
                    'so2': components['so2'],
                    'aqi': aqi,
                    'dt': dt
                }

                all_data.append(flattened_data)
            except json.JSONDecodeError as e:
                print(f"Error decoding JSON line: {e}")

    df = pd.DataFrame(all_data)

    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
