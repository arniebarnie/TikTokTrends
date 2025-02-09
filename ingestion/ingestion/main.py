from datetime import datetime
import shutil
import tempfile
import boto3
import whisperx
import pandas as pd
from pathlib import Path

from ingestion.metadata import VideoMetadata
from ingestion.transcriber import Transcriber
from ingestion.config import CONFIG, LOGGER

s3_client = boto3.client('s3')

def setup_directories():
    CONFIG.download_dir.mkdir(parents = True, exist_ok = True)

def read_profiles(profile_file: str) -> list[str]:
    with open(profile_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def read_profiles_from_s3(profile_s3_key: str) -> list[str]:
    response = s3_client.get_object(Bucket = CONFIG.s3_bucket, Key = profile_s3_key)
    return [line.strip() for line in response['Body'].read().decode('utf-8').splitlines() if line.strip()]

def upload_to_s3(df: pd.DataFrame, key: str):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / 'data.parquet'
        df.to_parquet(temp_path)
        s3_client.upload_file(str(temp_path), CONFIG.s3_bucket, key)

def main():
    setup_directories()
    
    if CONFIG.profiles_s3_key:
        profiles = read_profiles_from_s3(CONFIG.profiles_s3_key)
    else:
        profiles = ['noonessafe_pranks', 'batfurai']
    
    # Load WhisperX models once
    LOGGER.info("Loading WhisperX models...")
    model = whisperx.load_model(
        CONFIG.model_name,
        CONFIG.device,
        compute_type = CONFIG.compute_type
    )
    
    align_model, metadata = whisperx.load_align_model(
        language_code = "en",
        device = CONFIG.device
    )
    
    # Create transcriber with loaded models
    transcriber = Transcriber(model, align_model, metadata)
    
    for profile in profiles:
        try:
            # Get video metadata
            LOGGER.info(f'Downloading metadata for {profile}...')
            video_metadata = VideoMetadata().get_profile_metadata(profile)
            if video_metadata is None:
                LOGGER.error(f"No metadata found for profile: {profile}")
                continue
                
            # Sample rows from metadata
            sampled_rows = video_metadata.sample(min(CONFIG.transcribed_per_profile, len(video_metadata)))
            video_ids = sampled_rows['id'].tolist()
            LOGGER.info(f"Transcribing {len(video_ids)} videos for {profile}...")
            transcripts = transcriber.process_videos(profile, video_ids)

            # Create a dataframe with the transcripts
            transcripts_df = pd.DataFrame({
                'id': video_ids,
                'title': sampled_rows['title'].tolist(),
                'description': sampled_rows['description'].tolist(),
                'transcript': transcripts
            })

            # Save metadata and transcripts to S3
            LOGGER.info(f"Uploading data to S3 for {profile}...")
            current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            upload_to_s3(
                video_metadata, 
                f"{CONFIG.s3_prefix}/PROFILE={profile}/PROCESSED_AT={current_time}/metadata.parquet"
            )
            upload_to_s3(
                transcripts_df, 
                f"{CONFIG.s3_prefix}/PROFILE={profile}/PROCESSED_AT={current_time}/text.parquet"
            )
            LOGGER.info(f"Finished uploading data for {profile}")

        except Exception as e:
            LOGGER.error(f"Error processing profile: {profile}")
            LOGGER.error(f"Error: {e}")
        finally:
            # Cleanup profile directory
            profile_dir = CONFIG.download_dir / profile
            if profile_dir.exists():
                shutil.rmtree(profile_dir)

if __name__ == "__main__":
    main()