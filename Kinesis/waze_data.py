import requests
import time
import json
import boto3
# import datetime


def waze_firehose(waze_data):
    """
    Get the tweets from twitter stream api and use kinesis to store tweets into
    S3 storage
    """
    print(waze_data)
    response = firehose_client.put_record(
                DeliveryStreamName='waze_firehose',
                Record={
                        'Data': json.dumps(waze_data) + '\n'
                        }
                )
    print(response)


if __name__=='__main__':
	response = requests.get("http://localhost:8080/waze/traffic-notifications?latBottom=37.715739&latTop=37.787601&lonLeft=-122.495216&lonRight=-122.396643")
	waze_data = response.json()
	waze_data['time_stamp'] = time.time()

	firehose_client = boto3.client('firehose', region_name="us-east-1")
	waze_firehose(waze_data)
