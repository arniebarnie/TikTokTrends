import json
import boto3
import logging
import re
from urllib.parse import unquote

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client('athena')

def add_partition(bucket: str, key: str) -> None:
    """Add partition to text_analysis table for the uploaded file."""
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
            QueryString=query,
            QueryExecutionContext={'Database': 'tiktok_analytics'},
            ResultConfiguration={
                'OutputLocation': 's3://tiktoktrends/athena-results/'
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
        # Get the S3 bucket and key from the event
        bucket = event['Records'][0]['s3']['bucket']['name']
        key = event['Records'][0]['s3']['object']['key']
        
        # Decode the key for logging
        decoded_key = unquote(key)
        logger.info(f"Processing new text analysis file - Bucket: {bucket}, Key: {decoded_key}")
        
        # Add partition for the uploaded file
        add_partition(bucket, decoded_key)
        
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': 'Successfully added partition'
            })
        }
        
    except Exception as e:
        logger.error(f"Error processing S3 event: {str(e)}")
        raise 