from pathlib import Path
import json
import yt_dlp
import pandas as pd
from datetime import datetime
from typing import Optional
import tempfile
import shutil
import boto3
import os
import logging

# Configure logging
logging.basicConfig(level = logging.INFO)
LOGGER = logging.getLogger(__name__)

class Config:
    def __init__(self):
        # AWS Configuration
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_prefix = 'videos/metadata'
        self.profiles_s3_key = os.getenv('PROFILES_S3_KEY')
        
        # Local paths
        self.workspace = Path("/workspace")
        self.download_dir = self.workspace / "downloads"
        
CONFIG = Config()

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

def check_last_processed_at(profile: str) -> datetime:
    key = f"{CONFIG.s3_prefix}/PROFILE={profile}/PROCESSED_AT="
    response = s3_client.list_objects_v2(Bucket = CONFIG.s3_bucket, Prefix = key)
    if 'Contents' in response:
        last_processed_at = max(
            datetime.strptime(obj['Key'].split('PROCESSED_AT=')[1].split('/')[0], '%Y-%m-%d %H:%M:%S') 
            for obj in response['Contents']
        )
        return last_processed_at
    return None

class VideoMetadata:
    def __init__(self):
        self.ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'writeinfojson': True,
            'skip_download': True,
            'ignoreerrors': True,
            'outtmpl': str(CONFIG.download_dir) + '/%(uploader)s/%(id)s.%(ext)s',
            'extractor_args': {
                'tiktok': {
                    'app_name': 'trill',
                    'app_version': '34.1.2',
                    'manifest_app_version': '2023401020'
                }
            }
        }
    
    def download_metadata(self, profile: str) -> Optional[Path]:
        """
        Download metadata for all videos from a TikTok profile.
        
        Args:
            profile (str): TikTok profile name
            
        Returns:
            Optional[Path]: Path to the directory containing metadata files
        """
        
        # Create profile directory
        profile_dir = CONFIG.download_dir / profile
        profile_dir.mkdir(parents = True, exist_ok = True)
        
        try:
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                url = f"https://www.tiktok.com/@{profile}"
                ydl.download([url])
                
                # Verify directory exists and has files
                if profile_dir.exists() and any(profile_dir.iterdir()):
                    return profile_dir
                else:
                    LOGGER.error(f"No metadata files downloaded for {profile}")
                    return None
        except Exception as e:
            LOGGER.error(f'Error downloading metadata for {profile}: {e}')
            return None
    
    def extract_video_metadata(self, info_path: Path) -> dict:
        """
        Extract relevant metadata from a video's info.json file.
        
        Args:
            info_path (Path): Path to the info.json file
            
        Returns:
            dict: Extracted metadata
        """
        try:
            with open(info_path, 'r', encoding = 'utf-8') as f:
                info = json.load(f)
                
            # Extract track information
            track_info = {
                'track': info.get('track'),  # Original sound name
                'artists': info.get('artists', []),  # List of artists
                'artist': info.get('artist'),  # Main artist
            }
            
            # Combine video and track metadata
            return {
                'id': info['id'],
                'title': info.get('title'),
                'description': info.get('description'),
                'upload_date': datetime.strptime(info.get('upload_date', '19700101'), '%Y%m%d'),
                'like_count': info.get('like_count', 0),
                'repost_count': info.get('repost_count', 0),
                'comment_count': info.get('comment_count', 0),
                'view_count': info.get('view_count', 0),
                'duration': info.get('duration', 0),
                'webpage_url': info.get('webpage_url', ''),
                'channel': info.get('channel'),
                'timestamp': info.get('timestamp'),
                'uploader': info.get('uploader'),
                **track_info,  # Add track information
            }
        except Exception as e:
            LOGGER.error(f'Error extracting metadata from {info_path}: {e}')
            return {}
        
    def get_profile_metadata(self, profile: str) -> Optional[pd.DataFrame]:
        """
        Get metadata for all videos from a profile.
        
        Args:
            profile (str): TikTok profile name
            
        Returns:
            Optional[pd.DataFrame]: DataFrame containing video metadata
        """
        # Download metadata
        profile_dir = self.download_metadata(profile)
        if not profile_dir:
            return None

        try:
            # Process all metadata files
            video_data = []
            for info_file in profile_dir.glob("*.info.json"):
                metadata = self.extract_video_metadata(info_file)
                if metadata:
                    video_data.append(metadata)

            if not video_data:
                LOGGER.error(f"No valid metadata found for {profile}")
                return None

            # Create DataFrame
            df = pd.DataFrame(video_data)
            
            return df

        except Exception as e:
            LOGGER.error(f'Error processing metadata for {profile}: {e}')
            return None

def main():
    setup_directories()
    
    if CONFIG.profiles_s3_key:
        profiles = read_profiles_from_s3(CONFIG.profiles_s3_key)
    else:
        profiles = ['noonessafe_pranks', 'batfurai']
    
    for profile in profiles:
        try:
            LOGGER.info(f'Downloading metadata for {profile}...')
            video_metadata = VideoMetadata().get_profile_metadata(profile)
            if video_metadata is None:
                LOGGER.error(f"No metadata found for profile: {profile}")
                continue
            
            last_processed_at = check_last_processed_at(profile)
            if last_processed_at:
                LOGGER.info(f"{profile}: Last processed at: {last_processed_at}")
                video_metadata = video_metadata[video_metadata['upload_date'] > last_processed_at]
            else:
                LOGGER.info(f"No previous processing found for profile: {profile}")
            
            if len(video_metadata) == 0:
                LOGGER.error(f"No new metadata found for profile: {profile}")
                continue
            
            # Save metadata and transcripts to S3
            LOGGER.info(f"Uploading data to S3 for {profile}...")
            current_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
            upload_to_s3(
                video_metadata, 
                f"{CONFIG.s3_prefix}/profile={profile}/processed_at={current_time}/metadata.parquet"
            )
            
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