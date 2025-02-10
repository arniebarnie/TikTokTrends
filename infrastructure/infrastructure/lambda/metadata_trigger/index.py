import json
import os
import boto3
import logging
import re
import hashlib
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch = boto3.client('batch')

def create_valid_job_name(key: str) -> str:
    """Create a valid job name from S3 key."""
    # Decode URL-encoded key for name creation
    decoded_key = unquote(key)
    
    try:
        # Extract profile name from the path
        parts = decoded_key.split('/')
        for part in parts:
            if part.startswith('PROFILE='):
                profile = part.split('=')[1]
                break
        else:
            # If no profile found, use a hash of the key
            profile = hashlib.md5(key.encode()).hexdigest()[:8]
    except:
        # Fallback to using hash if any error occurs
        profile = hashlib.md5(key.encode()).hexdigest()[:8]
    
    # Remove invalid characters and truncate
    clean_name = re.sub(r'[^a-zA-Z0-9-_]', '-', profile)[:30]
    # Create unique suffix
    hash_suffix = hashlib.md5(key.encode()).hexdigest()[:8]
    
    return f"transcribe-{clean_name}-{hash_suffix}"

def handler(event, context):
    try:
        # Get the S3 bucket and key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Decode the key for logging
        decoded_key = unquote(key)
        logger.info(f"Processing new metadata upload - Bucket: {bucket}, Key: {decoded_key}")
        
        # Submit the transcription batch job
        response = batch.submit_job(
            jobName=create_valid_job_name(key),
            jobQueue=os.environ['GPU_JOB_QUEUE'],
            jobDefinition=os.environ['TRANSCRIBER_JOB_DEFINITION'],
            containerOverrides={
                'environment': [
                    {
                        'name': 'METADATA_S3_KEY',
                        'value': decoded_key  # Pass decoded key to container
                    },
                    {
                        'name': 'S3_BUCKET',
                        'value': bucket
                    }
                ]
            }
        )
        
        logger.info(f"Submitted transcription job {response['jobName']} with ID {response['jobId']}")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'jobId': response['jobId'],
                'jobName': response['jobName']
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing S3 event: {str(e)}")
        raise 