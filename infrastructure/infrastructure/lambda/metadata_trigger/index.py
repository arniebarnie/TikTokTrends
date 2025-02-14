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
athena = boto3.client('athena')

def create_valid_job_name(key: str) -> str:
    """Create a valid job name from S3 key."""
    # Decode URL-encoded key for name creation
    decoded_key = unquote(key)
    
    try:
        # Extract profile name from the path
        parts = decoded_key.split('/')
        for part in parts:
            if part.startswith('profile='):
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

def add_partition(bucket: str, key: str) -> None:
    """Add partition to Glue table for the uploaded file."""
    try:
        # Extract partition values from the key
        partition_pattern = re.compile(r'profile=([^/]+)/processed_at=([^/]+)/')
        match = partition_pattern.search(key)
        
        if not match:
            logger.warning(f"Could not extract partition info from key: {key}")
            return
            
        profile = match.group(1)
        processed_at = match.group(2)
        location = f"s3://{bucket}/videos/metadata/profile={profile}/processed_at={processed_at}"
        
        # Add partition using Athena
        query = f"""
        ALTER TABLE tiktok_analytics.metadata
        ADD IF NOT EXISTS PARTITION (
            profile = '{profile}',
            processed_at = '{processed_at}'
        )
        LOCATION '{location}'
        """
        
        logger.info(f"Adding partition: profile={profile}, processed_at={processed_at}")
        
        response = athena.start_query_execution(
            QueryString = query,
            QueryExecutionContext = {'Database': 'tiktok_analytics'},
            ResultConfiguration={
                'OutputLocation': f's3://{os.environ["ATHENA_RESULTS_BUCKET"]}/athena-results/'
            }
        )
        
        # Wait for query to complete
        query_execution_id = response['QueryExecutionId']
        while True:
            query_status = athena.get_query_execution(
                QueryExecutionId=query_execution_id
            )['QueryExecution']['Status']['State']
            
            if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                break
                
        if query_status != 'SUCCEEDED':
            logger.error(f"Failed to add partition: {query_status}")
            
    except Exception as e:
        logger.error(f"Error adding partition: {str(e)}")

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
                logger.info(f"Processing new metadata upload - Bucket: {bucket}, Key: {decoded_key}")
                
                # Submit the transcription batch job
                response = batch.submit_job(
                    jobName = create_valid_job_name(key),
                    jobQueue = os.environ['GPU_JOB_QUEUE'],
                    jobDefinition = os.environ['TRANSCRIBER_JOB_DEFINITION'],
                    containerOverrides = {
                        'environment': [
                            {
                                'name': 'METADATA_S3_KEY',
                                'value': decoded_key
                            },
                            {
                                'name': 'S3_BUCKET',
                                'value': bucket
                            }
                        ]
                    }
                )
                
                # Add partition for the uploaded file
                add_partition(bucket, decoded_key)
                
                logger.info(f"Submitted transcription job {response['jobName']} with ID {response['jobId']}")
        
        return {
            'statusCode': 200,
            'body': 'Successfully processed all metadata files'
        }
        
    except Exception as e:
        logger.error(f"Error processing SNS event: {str(e)}")
        raise 