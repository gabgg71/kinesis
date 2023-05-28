import boto3
import datetime
import numpy as np
import requests
import os
from dotenv import load_dotenv

load_dotenv('./env_vars.sh')

kinesis_client = boto3.client('kinesis',   
                      region_name='us-east-1'  ,
                      aws_access_key_id=os.environ.get('aws_access_key_id'),
                      aws_secret_access_key=os.environ.get('aws_secret_access_key'),
                      aws_session_token = os.environ.get('aws_session_token')
                      )

sns_client = boto3.client('sns',
                          region_name='us-east-1'  ,
                          aws_access_key_id=os.environ.get('aws_access_key_id'),
                          aws_secret_access_key=os.environ.get('aws_secret_access_key'),
                          aws_session_token = os.environ.get('aws_session_token'))


response = kinesis_client.get_shard_iterator(
    StreamName='DemoStream',
    ShardId='shardId-000000000000',
    ShardIteratorType='TRIM_HORIZON'
)

iterator = response['ShardIterator']
response = kinesis_client.get_records(
    ShardIterator=iterator
)
records = response["Records"]
if len(records) >0:
  data = [float(i['Data'].decode('utf-8')) for i in records]
  moving_average =sum(data[-20:])/len(data[-20:])
  std = np.std(data)
  upper_band = moving_average+2*std
  last = data[-1]
  if last > upper_band:
    response = sns_client.publish(
        TopicArn=os.environ.get('arn'),  
        Subject="Bitcoin above the upper Bollinger band!",  
        Message=f"The price of Bitcoin has exceeded the upper Bollinger band by {last-upper_band}.\nCurrent Bitcoin price: {last} \nUpper band: {upper_band}."
    )

