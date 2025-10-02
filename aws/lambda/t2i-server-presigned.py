import boto3
import json
import os

bucketname = os.environ['BUCKETNAME']
key_prefix = 't2i/orders'


def get_details(order_id):
	s3_client = boto3.client('s3')
	response = s3_client.get_object(Bucket=bucketname, Key=f't2i/details/{order_id}.json')
	details = json.loads(response['Body'].read().decode('utf-8'))
	prompt = details['prompt']
	quantity = details['quantity']
	email = details['email']
	return prompt, quantity, email


def get_presigned_url(order_id):
	s3_client = boto3.client('s3')
	response = s3_client.generate_presigned_url(
		'get_object',
		Params={'Bucket': bucketname, 'Key': f't2i/orders/{order_id}.zip'},
		ExpiresIn=86400)
	return response


def send_email(order_id, recipient_email, presigned_url):
	sender_email = os.environ['SENDER_EMAIL']
	invoke_url = f'{os.environ['INVOKE_URL']}?order_id={order_id}'

	ses_client = boto3.client('ses')
	subject = f'Text-2-Image Order Completed'
	body_text = f'Order ID: {order_id}\n\n'
	body_text += f'This link will expire in 24 hours.\n\n{presigned_url}\n\n'
	body_text += f'If it expires, click the link below to make a new one.\n\n{invoke_url}\n\n'
	body_text += 'Images are deleted after 30 days.\n\nsymbolfigures'
	try:
		response = ses_client.send_email(
			Source=sender_email,
			Destination={
				'ToAddresses': [recipient_email]
			},
			Message={
				'Subject': {'Data': subject},
				'Body': {
					'Text': {'Data': body_text}
				}
			}
		)
		print('Email sent. Message ID:', response['MessageId'])
	except Exception as e:
		print('Error sending email:', e)


def lambda_handler(event, context):
	query_params = event.get('queryStringParameters', {})
	order_id = query_params.get('order_id', '')
	#order_id = event.get('order_id', '') # test

	prompt, quantity, email = get_details(order_id)
	presigned_url = get_presigned_url(order_id)
	send_email(order_id, email, presigned_url)

	return {
		'statusCode': 200,
		'body': json.dumps('An email has been sent with a new link.')
	}
