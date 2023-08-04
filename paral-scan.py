import argparse
import sys
import boto3
import threading
from botocore.exceptions import ClientError
from boto3.dynamodb.types import TypeDeserializer
from boto3.dynamodb.conditions import Attr

# Parse script args // accepting dev...
parser = argparse.ArgumentParser()
parser.add_argument('--environment', '-e',
                    help="the environment", type=str, required=True)

args = parser.parse_args(sys.argv[1:])

# DynamoDB Table names
table_name = f"{args.environment}_mytable"

# Creating DynamoDB Client
session = boto3.Session(profile_name='imy-profile')
dynamoDBClient = session.client('dynamodb')
deserializer = TypeDeserializer()

def scan_table(segment, total_segments):
    print('Looking at segment ' + str(segment))
    last_evaluated_key = None

    while True:
        if last_evaluated_key:
            response = dynamoDBClient.scan(
                TableName = table_name,
                ExclusiveStartKey = last_evaluated_key,
                Segment = segment,
                TotalSegments = total_segments
            )
        else:
            response = dynamoDBClient.scan(
                TableName = table_name,
                Segment = segment,
                TotalSegments = total_segments
            )

        last_evaluated_key = response.get('LastEvaluatedKey')
        for item in response['Items']:
            item_data = { k: deserializer.deserialize(v) for k,v in item.items() }
            if item_data.get('status') == None:
                do_process(item)

        if not last_evaluated_key:
            break


def do_process(item):
    try:
        dynamoDBClient.update_item (
            TableName = table_name,
            Key = { 'id' : item.get('id')},
            ExpressionAttributeNames = {
                '#s': 'status',
            },
            ExpressionAttributeValues = {
                ':s': {
                    'S': 'OK'
                }
            },
            UpdateExpression = 'SET #s = :s'
        )

        print(f'Updated {item.get("id")}')

    except ClientError as e:
        print(item.get("id"))
        print(e)

def create_threads():
    thread_list = []
    total_threads = 100

    for i in range(total_threads):
        # Instantiate and store the thread
        thread = threading.Thread(target=scan_table, args=(i, total_threads))
        thread_list.append(thread)
    # Start threads
    for thread in thread_list:
        thread.start()
    # Block main thread until all threads are finished
    for thread in thread_list:
        thread.join()

def main():
    create_threads()

if __name__ == "__main__" :
    main()