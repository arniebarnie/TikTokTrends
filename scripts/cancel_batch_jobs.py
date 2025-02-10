import boto3
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger(__name__)

def cancel_all_jobs():
    """Cancel all jobs in all job queues"""
    batch = boto3.client('batch')
    
    # All possible active job states
    JOB_STATES = ['SUBMITTED', 'PENDING', 'RUNNABLE', 'STARTING', 'RUNNING']
    
    try:
        # Get all job queues
        queues = []
        paginator = batch.get_paginator('describe_job_queues')
        for page in paginator.paginate():
            for queue in page['jobQueues']:
                queues.append(queue['jobQueueName'])
        
        LOGGER.info(f"Found {len(queues)} job queues")
        
        # For each queue, list and cancel all jobs
        for queue in queues:
            LOGGER.info(f"Processing queue: {queue}")
            
            # List all jobs in active states
            jobs = []
            paginator = batch.get_paginator('list_jobs')
            
            for status in JOB_STATES:
                LOGGER.info(f"Checking for {status} jobs in queue {queue}")
                for page in paginator.paginate(
                    jobQueue=queue,
                    jobStatus=status
                ):
                    jobs.extend(page['jobSummaryList'])
            
            LOGGER.info(f"Found {len(jobs)} active jobs in queue {queue}")
            
            # Cancel each job
            for job in jobs:
                job_id = job['jobId']
                job_status = job['status']
                try:
                    LOGGER.info(f"Cancelling job {job_id} (status: {job_status})")
                    batch.terminate_job(
                        jobId=job_id,
                        reason='Cancelled by administrator'
                    )
                except Exception as e:
                    LOGGER.error(f"Failed to cancel job {job_id}: {str(e)}")
                    
    except Exception as e:
        LOGGER.error(f"Error cancelling jobs: {str(e)}")
        raise

if __name__ == "__main__":
    cancel_all_jobs() 