import boto3
import logging
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def repair_table_partitions(database_name: str, table_name: str) -> None:
    """
    Repair/refresh partitions for a Glue table (equivalent to MSCK REPAIR TABLE)
    
    Args:
        database_name (str): Name of the Glue database
        table_name (str): Name of the table to repair
    """
    glue_client = boto3.client('glue')
    
    try:
        LOGGER.info(f"Starting partition detection for {database_name}.{table_name}")
        
        response = glue_client.start_partition_detection(
            DatabaseName = database_name,
            TableName = table_name
        )
        
        # Get the task run ID
        task_run_id = response['TaskRunId']
        
        # Wait for completion
        waiter = glue_client.get_waiter('task_run_complete')
        waiter.wait(
            TaskRunId = task_run_id,
            WaiterConfig = {
                'Delay': 5,
                'MaxAttempts': 60
            }
        )
        
        LOGGER.info(f"Successfully refreshed partitions for {database_name}.{table_name}")
        
    except ClientError as e:
        LOGGER.error(f"Error repairing partitions for {database_name}.{table_name}: {str(e)}")
        raise

def main():
    DATABASE_NAME = "tiktok_analytics"
    TABLES = ["metadata", "text_analysis"]
    
    for table in TABLES:
        try:
            repair_table_partitions(DATABASE_NAME, table)
        except Exception as e:
            LOGGER.error(f"Failed to repair table {table}: {str(e)}")
            continue

if __name__ == "__main__":
    main()
