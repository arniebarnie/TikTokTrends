import json
import os
import boto3
import logging
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

batch = boto3.client('batch')

def create_valid_job_name(key: str) -> str:
    """Create a valid job name from transcript file path."""
    # Extract profile name from the path
    decoded_key = unquote(key)
    parts = decoded_key.split('/')
    
    try:
        # Find the profile part
        for part in parts:
            if part.startswith('PROFILE='):
                profile = part.split('=')[1]
                break
        else:
            profile = 'unknown'
    except:
        profile = 'unknown'
    
    # Create a clean job name
    return f"text-analysis-{profile}"

def handler(event, context):
    try:
        # Get the S3 bucket and key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Decode the key for logging
        decoded_key = unquote(key)
        logger.info(f"Processing new transcript upload - Bucket: {bucket}, Key: {decoded_key}")
        
        # Submit the text analysis batch job
        response = batch.submit_job(
            jobName=create_valid_job_name(key),
            jobQueue=os.environ['FARGATE_JOB_QUEUE'],
            jobDefinition=os.environ['TEXT_ANALYSIS_JOB_DEFINITION'],
            containerOverrides={
                'environment': [
                    {
                        'name': 'TRANSCRIPTS_S3_KEY',
                        'value': decoded_key
                    },
                    {
                        'name': 'S3_BUCKET',
                        'value': bucket
                    }
                ]
            }
        )
        
        logger.info(f"Submitted text analysis job {response['jobName']} with ID {response['jobId']}")
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