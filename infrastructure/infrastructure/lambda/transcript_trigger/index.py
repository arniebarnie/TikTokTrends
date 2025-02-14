import json
import boto3
import logging
import os
from urllib.parse import unquote

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

batch = boto3.client('batch')

def create_valid_job_name(key: str) -> str:
    """Create a valid job name from the S3 key."""
    # Extract profile name from the key
    parts = key.split('/')
    profile = next((p.split('=')[1] for p in parts if p.startswith('profile=')), 'unknown')
    return f"text-{profile}-{hash(key) % 1000000:06d}"

def handler(event, context):
    try:
        # Process each record in the event
        for record in event['Records']:
            # Get the S3 event from the SNS message
            sns_message = record['Sns']['Message']
            s3_event = json.loads(sns_message)
            
            # Process each S3 record
            for s3_record in s3_event['Records']:
                bucket = s3_record['s3']['bucket']['name']
                key = s3_record['s3']['object']['key']
                
                # Decode the key for logging
                decoded_key = unquote(key)
                logger.info(f"Processing new transcript upload - Bucket: {bucket}, Key: {decoded_key}")
                
                # Submit the text analysis batch job
                response = batch.submit_job(
                    jobName = create_valid_job_name(key),
                    jobQueue = os.environ['FARGATE_JOB_QUEUE'],
                    jobDefinition = os.environ['TEXT_ANALYSIS_JOB_DEFINITION'],
                    containerOverrides = {
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
            'body': 'Successfully processed all transcript files'
        }
        
    except Exception as e:
        logger.error(f"Error processing SNS event: {str(e)}")
        raise 