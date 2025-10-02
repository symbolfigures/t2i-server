import argparse
import asyncio
import boto3
from botocore.client import Config
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from stablediffusion35.inference import generate
import json
import os
import requests
import shutil
import sys
import time

load_dotenv()
session = boto3.Session(profile_name='t2i-server')
log_filepath = os.getenv('LOG_FILEPATH')
sys.stdout = open(log_filepath, 'a', buffering=1)
sys.stderr = sys.stdout

def upload_s3(order_id):
	s3 = session.client('s3', config=Config(signature_version='s3v4'))
	bucket_name = os.getenv('BUCKETNAME')

	# details upload
	json_filepath = f'orders/{order_id}/details.json'
	s3_key = f't2i/details/{order_id}.json'
	try:
		s3.upload_file(json_filepath, bucket_name, s3_key)
		print(f'Details uploaded to S3.')
	except Exception as e:
		print(f'Error uploading details: {e}')

	# init multiplart upload
	zip_filepath = f'orders/{order_id}.zip'
	s3_key = f't2i/{zip_filepath}'
	part_size = 5 * 1024 * 1024 # 5 MB part size
	response = s3.create_multipart_upload(Bucket=bucket_name, Key=s3_key)
	upload_id = response['UploadId']
	parts = []
	try:
		with open(zip_filepath, 'rb') as f:
		    part_number = 1
		    while True:
		        chunk = f.read(part_size)
		        if not chunk:
		            break
		        # upload parts
		        part_response = s3.upload_part(
		            Bucket=bucket_name,
		            Key=s3_key,
		            UploadId=upload_id,
		            PartNumber=part_number,
		            Body=chunk
		        )
		        parts.append({'PartNumber': part_number, 'ETag': part_response['ETag']})
		        part_number += 1
		# complete upload
		s3.complete_multipart_upload(
		    Bucket=bucket_name,
		    Key=s3_key,
		    UploadId=upload_id,
		    MultipartUpload={'Parts': parts}
		)
		print(f'Images uploaded to S3.')
	except Exception as e:
		print(f'Error during multipart upload: {e}')
		s3.abort_multipart_upload(Bucket=bucket_name, Key=s3_key, UploadId=upload_id)
	# pre-signed url
	try:
		url = s3.generate_presigned_url(
		    'get_object',
		    Params={'Bucket': bucket_name, 'Key': s3_key},
		    ExpiresIn=86400 # valid for 24 hours
		)
		print('Generated pre-signed URL.')
	except ClientError as e:
		print(f'Error generating pre-signed URL: {e}')
	return url


def process_order(order):
	t1 = time.time()
	order_id = int(t1)
	dir_out = f'orders/{order_id}'
	os.makedirs(dir_out, exist_ok=True)

	# take order
	prompt = order['prompt']
	quantity = int(order['quantity'])
	email = order['email']
	print('~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~')
	print(f'\nReceived order:\n- Order ID: {order_id}\n- Prompt: {prompt}\n- Quantity: {quantity}\n- Email: {email}')

	# create details
	order['order_id'] = order_id
	json_filepath = f'{dir_out}/details.json'
	with open(json_filepath, 'w') as f:
		json.dump(order, f, indent=4)
	print(f'Details created and saved.\n')

	# generate
	t2 = time.time()
	generate(dir_out, order_id, prompt, quantity)
	print(f'\nImages generated and saved.')
	t3 = time.time()

	# zip
	shutil.make_archive(dir_out, 'zip', dir_out)
	print(f'Images zipped.')

	# upload to s3 and generate pre-signed URL
	presigned_url = upload_s3(order_id)

	# email
	invoke_url = f'{os.getenv('INVOKE_URL')}?order_id={order_id}'
	try:
		response = requests.get(invoke_url)
		response.raise_for_status()
		print(f'Email sent. Status code: {response.status_code}')
	except requests.exceptions.RequestException as e:
			print(f'Email failed: {e}')

	t4 = time.time()
	total_time = t4 - t1
	images_time = t3 - t2
	per_image_time = images_time / quantity
	non_images_time = total_time - images_time
	time_stats = (
		f'\nTotal time: {int(total_time / 60)} minutes {int(total_time % 60)} seconds\n'
		f'Images time: {int(images_time / 60)} minutes {int(images_time % 60)} seconds\n'
		f'Per image time: {int(per_image_time / 60)} minutes {int(per_image_time % 60)} seconds\n'
		f'Other time: {int(non_images_time / 60)} minutes {int(non_images_time % 60)} seconds\n'
	)
	print(time_stats)


def poll_sqs():
	sqs = session.client('sqs', config=Config(signature_version='s3v4'))
	QUEUE_URL = os.getenv('QUEUE_URL')
	print('\nPolling for messages...\n')
	while True:
		try:
			response = sqs.receive_message(
				QueueUrl=QUEUE_URL,
				MaxNumberOfMessages=1,
				WaitTimeSeconds=20
			)
			messages = response.get('Messages', [])
			if not messages:
				continue

			for msg in messages:
				payload = json.loads(msg['Body'])
				try:
					process_order(payload)
					sqs.delete_message(
						QueueUrl=QUEUE_URL,
						ReceiptHandle=msg['ReceiptHandle']
					)
				except Exception as e:
					print('Job failed:', e)
					# message left in queue, retried after visibility timeout
		except Exception as e:
			print('Polling error:', e)
			time.sleep(5) # backoff


if __name__ == '__main__':
	poll_sqs()





































