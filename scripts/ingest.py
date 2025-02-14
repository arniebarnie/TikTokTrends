import argparse
import math
import boto3
import json
from pathlib import Path

# S3 Configuration
S3_BUCKET = 'tiktoktrends-2'
S3_BATCH_JOBS_PREFIX = 'batch-jobs'

# Batch Job Configuration
BATCH_JOB_QUEUE = 'tiktok-metadata-fargate-queue'
BATCH_JOB_DEFINITION = 'tiktok-metadata-job'
BATCH_JOB_NAME_PREFIX = 'tiktok-metadata'

# Environment Variable Names
ENV_S3_BUCKET = 'S3_BUCKET'
ENV_PROFILES_KEY = 'PROFILES_S3_KEY'

def split_profiles(profile_file: str, num_containers: int) -> list[list[str]]:
    """Split profiles into equal groups."""
    with open(profile_file, 'r') as f:
        profiles = [line.strip() for line in f if line.strip()]
    
    # Calculate profiles per container
    profiles_per_container = math.ceil(len(profiles) / num_containers)
    
    # Split profiles into groups
    profile_groups = []
    for i in range(0, len(profiles), profiles_per_container):
        group = profiles[i : i + profiles_per_container]
        profile_groups.append(group)
    
    return profile_groups

def submit_batch_job(profile_group: list[str], job_queue: str, job_definition: str) -> str:
    """Submit a metadata collection job to AWS Batch."""
    batch_client = boto3.client('batch')
    
    # Create temporary profiles file in S3
    s3_client = boto3.client('s3')
    profile_content = '\n'.join(profile_group)
    job_id = f"job_{hash(str(profile_group))}"
    profiles_key = f"{S3_BATCH_JOBS_PREFIX}/{job_id}/profiles.txt"
    
    s3_client.put_object(
        Bucket = S3_BUCKET,
        Key = profiles_key,
        Body = profile_content.encode('utf-8')
    )
    
    # Submit the batch job
    response = batch_client.submit_job(
        jobName = f'{BATCH_JOB_NAME_PREFIX}-{job_id}',
        jobQueue = job_queue,
        jobDefinition = job_definition,
        containerOverrides = {
            'environment': [
                {
                    'name': ENV_S3_BUCKET,
                    'value': S3_BUCKET
                },
                {
                    'name': ENV_PROFILES_KEY,
                    'value': profiles_key
                }
            ]
        }
    )
    
    return response['jobId']

def main():
    parser = argparse.ArgumentParser(description = 'Launch TikTok metadata collection batch jobs')
    parser.add_argument('profile_file', type = str, help = 'Path to profiles.txt')
    parser.add_argument('num_containers', type = int, help = 'Number of containers to run')
    args = parser.parse_args()
    
    # Split profiles into groups
    profile_groups = split_profiles(args.profile_file, args.num_containers)
    
    # Submit jobs
    job_ids = []
    for group in profile_groups:
        job_id = submit_batch_job(group, BATCH_JOB_QUEUE, BATCH_JOB_DEFINITION)
        job_ids.append(job_id)
        print(f"Submitted metadata job {job_id} with {len(group)} profiles")
    
    print(f"\nSubmitted {len(job_ids)} metadata collection jobs")
    print("Job IDs:", job_ids)

if __name__ == '__main__':
    main() 