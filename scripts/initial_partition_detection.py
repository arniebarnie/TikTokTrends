import boto3
import logging
import re
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def list_partitions(bucket: str, prefix: str) -> list:
    """
    List all partitions in the given S3 prefix by looking for parquet files
    Returns list of tuples: (profile, processed_at, full_path)
    """
    s3_client = boto3.client('s3')
    partitions = []
    
    # Regex to extract partition values from path
    partition_pattern = re.compile(r'profile=([^/]+)/processed_at=([^/]+)/[^/]+\.parquet$')
    
    paginator = s3_client.get_paginator('list_objects_v2')
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        if 'Contents' not in page:
            continue
            
        for obj in page['Contents']:
            key = obj['Key']
            match = partition_pattern.search(key)
            if match:
                profile = match.group(1)
                processed_at = match.group(2)
                full_path = f"s3://{bucket}/{key.rsplit('/', 1)[0]}"
                partitions.append((profile, processed_at, full_path))
    
    return partitions

def add_partitions(database: str, table: str, partitions: list) -> None:
    """
    Add partitions to table using Athena
    """
    athena_client = boto3.client('athena')
    
    for profile, processed_at, location in partitions:
        try:
            query = f"""
            ALTER TABLE {database}.{table}
            ADD IF NOT EXISTS PARTITION (
                profile = '{profile}',
                processed_at = '{processed_at}'
            )
            LOCATION '{location}'
            """
            
            LOGGER.info(f"Adding partition: profile={profile}, processed_at={processed_at}")
            
            # Execute the query
            response = athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': database},
                ResultConfiguration={
                    'OutputLocation': 's3://tiktoktrends/athena-results/'
                }
            )
            
            # Wait for query to complete
            query_execution_id = response['QueryExecutionId']
            while True:
                query_status = athena_client.get_query_execution(
                    QueryExecutionId=query_execution_id
                )['QueryExecution']['Status']['State']
                
                if query_status in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                    
            if query_status != 'SUCCEEDED':
                LOGGER.error(f"Failed to add partition: {query_status}")
                
        except Exception as e:
            LOGGER.error(f"Error adding partition: {str(e)}")

def main():
    BUCKET = "tiktoktrends"
    DATABASE = "tiktok_analytics"
    
    # Process metadata table
    LOGGER.info("Processing metadata table partitions...")
    metadata_partitions = list_partitions(BUCKET, "videos/metadata/")
    LOGGER.info(f"Found {len(metadata_partitions)} metadata partitions")
    add_partitions(DATABASE, "metadata", metadata_partitions)
    
    # Process text analysis table
    LOGGER.info("Processing text_analysis table partitions...")
    text_partitions = list_partitions(BUCKET, "videos/text/")
    LOGGER.info(f"Found {len(text_partitions)} text analysis partitions")
    add_partitions(DATABASE, "text_analysis", text_partitions)

if __name__ == "__main__":
    main()
