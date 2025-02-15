import json
import boto3
import logging
import os
from urllib.parse import unquote
import re

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

athena = boto3.client('athena')

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
        location = f"s3://{bucket}/videos/text/profile={profile}/processed_at={processed_at}"
        
        # Add partition using Athena
        query = f"""
        ALTER TABLE tiktok_analytics.text_analysis
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
            ResultConfiguration = {
                'OutputLocation': f's3://{os.environ["ATHENA_RESULTS_BUCKET"]}/athena-results/'
            }
        )
        
        # Wait for query to complete
        query_execution_id = response['QueryExecutionId']
        while True:
            query_status = athena.get_query_execution(
                QueryExecutionId = query_execution_id
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
                logger.info(f"Processing new text analysis upload - Bucket: {bucket}, Key: {decoded_key}")
                
                # Add partition for the uploaded file
                add_partition(bucket, decoded_key)
        
        return {
            'statusCode': 200,
            'body': 'Successfully processed all text analysis files'
        }
        
    except Exception as e:
        logger.error(f"Error processing SNS event: {str(e)}")
        raise 