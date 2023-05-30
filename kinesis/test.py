import boto3
import os
from dotenv import load_dotenv

load_dotenv('./env_vars.sh')


def test():
    kinesis_client = boto3.client('kinesis',
                      region_name='us-east-1'  ,
                      aws_access_key_id=os.environ.get('aws_access_key_id'),
                      aws_secret_access_key=os.environ.get('aws_secret_access_key'),
                      aws_session_token = os.environ.get('aws_session_token')
                      )
    response = kinesis_client.get_shard_iterator(
    StreamName='DemoStream',
    ShardId='shardId-000000000000',
    ShardIteratorType='TRIM_HORIZON'
    )

    status = response['ResponseMetadata']['HTTPStatusCode']
    assert status == 200, "Cant get an Iterator, please check shard name and id"

    iterator = response['ShardIterator']
    response = kinesis_client.get_records(
        ShardIterator=iterator
    )
    size = len(response)
    assert size > 0, "Check crontab events"
    
test()

