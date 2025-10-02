import json
import boto3
import os
import re
import uuid

sqs = boto3.client('sqs')
QUEUE_URL = os.environ['QUEUE_URL']

def lambda_handler(event, context):
	payload = json.loads(event['body'])
	# sanitize
	prompt = payload['prompt']
	prompt = re.sub(r'[^a-zA-Z0-9\s]', '', prompt)
	payload['prompt'] = prompt
	# send to SQS
	sqs.send_message(
		QueueUrl=QUEUE_URL,
		MessageBody=json.dumps(payload)
	)
	# get queue length
	attrs = sqs.get_queue_attributes(
		QueueUrl=QUEUE_URL,
		AttributeNames=['ApproximateNumberOfMessages']
	)
	queue_length = int(attrs['Attributes']['ApproximateNumberOfMessages'])
	return {
		'statusCode': 200,
		'headers': {
            'Access-Control-Allow-Origin': 'https://symbolfigures.io',
            'Access-Control-Allow-Methods': 'OPTIONS,POST',
            'Access-Control-Allow-Headers': 'Content-Type'
        },
		'body': json.dumps({
			'status': 'queued',
			'queue_length': queue_length
		})
	}