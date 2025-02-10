import argparse
import math
import boto3
import json
from pathlib import Path

def split_profiles(profile_file: str, num_containers: int) -> list[list[str]]:
    """Split profiles into equal groups."""
    with open(profile_file, 'r') as f:
        profiles = [line.strip() for line in f if line.strip()]
    
    # Calculate profiles per container
    profiles_per_container = math.ceil(len(profiles) / num_containers)
    
    # Split profiles into groups
    profile_groups = []
    for i in range(0, len(profiles), profiles_per_container):
        group = profiles[i:i + profiles_per_container]
        profile_groups.append(group)
    
    return profile_groups

def submit_batch_job(profile_group: list[str], job_queue: str, job_definition: str) -> str:
    """Submit a metadata collection job to AWS Batch."""
    batch_client = boto3.client('batch')
    
    # Create temporary profiles file in S3
    s3_client = boto3.client('s3')
    profile_content = '\n'.join(profile_group)
    job_id = f"job_{hash(str(profile_group))}"
    profiles_key = f"batch-jobs/{job_id}/profiles.txt"
    
    s3_client.put_object(
        Bucket='tiktoktrends',
        Key=profiles_key,
        Body=profile_content.encode('utf-8')
    )
    
    # Submit the batch job
    response = batch_client.submit_job(
        jobName=f'tiktok-metadata-{job_id}',
        jobQueue=job_queue,
        jobDefinition=job_definition,
        containerOverrides={
            'environment': [
                {
                    'name': 'S3_BUCKET',
                    'value': 'tiktoktrends'
                },
                {
                    'name': 'PROFILES_S3_KEY',
                    'value': profiles_key
                }
            ]
        }
    )
    
    return response['jobId']

def main():
    parser = argparse.ArgumentParser(description='Launch TikTok metadata collection batch jobs')
    parser.add_argument('profile_file', type=str, help='Path to profiles.txt')
    parser.add_argument('num_containers', type=int, help='Number of containers to run')
    args = parser.parse_args()
    
    # Split profiles into groups
    profile_groups = split_profiles(args.profile_file, args.num_containers)
    
    # Get job queue and definition names from environment or config
    job_queue = 'tiktok-fargate-queue'  # Changed to use Fargate queue
    job_definition = 'tiktok-metadata-job'  # Changed to use metadata job definition
    
    # Submit jobs
    job_ids = []
    for group in profile_groups:
        job_id = submit_batch_job(group, job_queue, job_definition)
        job_ids.append(job_id)
        print(f"Submitted metadata job {job_id} with {len(group)} profiles")
    
    print(f"\nSubmitted {len(job_ids)} metadata collection jobs")
    print("Job IDs:", job_ids)

if __name__ == '__main__':
    main() 