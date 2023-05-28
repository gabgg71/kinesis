import boto3
import datetime
import requests
import os
from dotenv import load_dotenv

load_dotenv('./env_vars.sh')

client = boto3.client('kinesis',   
                      region_name='us-east-1'  ,
                      aws_access_key_id=os.environ.get('aws_access_key_id'),
                      aws_secret_access_key=os.environ.get('aws_secret_access_key'),
                      aws_session_token = os.environ.get('aws_session_token')
                      )

def current_bitcoin():
  url = os.environ.get('data_bitcoin')
  headers = {
      'Content-Type': 'application/json',
      'Connection': 'keep-alive',
      'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/113.0.0.0 Safari/537.36',
  }
  response = requests.get(url, headers=headers)
  return response.json()

bit = current_bitcoin()
bitcoin = bit[0]['current_price']
response = client.put_record(
    StreamName='DemoStream',
    Data=f'{bitcoin}'.encode('utf-8'),
    PartitionKey='market'
)
print(f'New data: {datetime.datetime.now().strftime("%I:%M")}')

