import os
import sys
import logging
from pathlib import Path
import json
import boto3
import whisperx
import yt_dlp
import pandas as pd
import shutil
from datetime import datetime
import torch

# Configure logging
logging.basicConfig(level = logging.INFO)
logger = logging.getLogger(__name__)

# Check GPU availability
if not torch.cuda.is_available():
    logger.error("No GPU detected")
else:
    logger.info(f"GPU detected: {torch.cuda.get_device_name(0)}")
    logger.info(f"CUDA Version: {torch.version.cuda}")

class Config:
    def __init__(self):
        # AWS Configuration
        self.s3_bucket = os.getenv('S3_BUCKET', 'tiktoktrends')
        self.s3_prefix = 'data'  # Fixed to 'data' folder
        self.profiles_s3_key = os.getenv('PROFILES_S3_KEY')
        
        # Whisper Configuration
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.compute_type = "float16" if self.device == "cuda" else "float32"
        self.model_name = "base.en"
        self.batch_size = 2
        
        # Local paths
        self.workspace = Path("/workspace")
        self.download_dir = self.workspace / "downloads"
        self.transcript_dir = self.workspace / "transcripts"
CONFIG = Config()

YDL_OPTS = {
    'format': 'bestaudio/best',
    'postprocessors': [{
        'key': 'FFmpegExtractAudio',
        'preferredcodec': 'wav',
    }],
    'ignoreerrors': True,
    'quiet': True,
    'noprogress': True,
    'extract_flat': False,
    'outtmpl': str(CONFIG.download_dir) + '/%(uploader)s/%(id)s.%(ext)s',
    'writeinfojson': True,
    'extractor_args': {
        'tiktok': {
            'app_name': 'trill',
            'app_version': '34.1.2',
            'manifest_app_version': '2023401020'
        }
    }
}

def setup_directories():
    """Create necessary directories."""
    CONFIG.download_dir.mkdir(parents = True, exist_ok = True)
    CONFIG.transcript_dir.mkdir(parents = True, exist_ok = True)
    logger.info(f"Created directories: {CONFIG.download_dir}, {CONFIG.transcript_dir}")

def read_profiles(profile_file: str) -> list[str]:
    """
    Read profile names from input file.
    
    Args:
        profile_file (str): Path to file containing profile names
        
    Returns:
        list[str]: List of profile names
    """
    with open(profile_file, 'r') as f:
        return [line.strip() for line in f if line.strip()]

def download_videos(profile: str) -> Path | None:
    """Download all videos for a given profile."""
    logger.info(f'Downloading videos for {profile}...')
    
    # Create profile directory before download
    profile_dir = CONFIG.download_dir / profile
    profile_dir.mkdir(parents = True, exist_ok = True)
    
    try:
        with yt_dlp.YoutubeDL(YDL_OPTS) as ydl:
            url = f"https://www.tiktok.com/@{profile}"
            ydl.download([url])
            
            # Verify directory exists and has files
            if profile_dir.exists() and any(profile_dir.iterdir()):
                return profile_dir
            else:
                logger.error(f"No files downloaded for {profile}")
                return None
    except Exception as e:
        logger.error(f'Error downloading videos for {profile}: {e}')
        return None

def get_video_metadata(video_path: Path) -> dict:
    """
    Extract metadata from the video's info.json file.
    
    Args:
        video_path (Path): Path to the video file
        
    Returns:
        dict: Video metadata
    """
    try:
        info_path = video_path.with_suffix('.info.json')
        if not info_path.exists():
            return {}
            
        with open(info_path, 'r', encoding='utf-8') as f:
            info = json.load(f)
            
        return {
            'id': info.get('id'),
            'title': info.get('title'),
            'description': info.get('description'),
            'upload_date': info.get('upload_date'),
            'like_count': info.get('like_count'),
            'repost_count': info.get('repost_count'),
            'comment_count': info.get('comment_count'),
            'view_count': info.get('view_count'),
            'duration': info.get('duration'),
        }
    except Exception as e:
        logger.error(f'Error extracting metadata from {video_path}: {e}')
        return {}

def process_video(video_path: str, model: any, align_model: any, metadata: dict) -> dict | None:
    """
    Process a single video file with WhisperX.
    
    Args:
        video_path (str): Path to video file
        model: WhisperX model
        align_model: Alignment model
        metadata: Alignment metadata
        
    Returns:
        dict | None: Transcription results
    """
    try:
        logger.info(f'Processing {video_path}')
        
        # Get video metadata
        video_metadata = get_video_metadata(Path(video_path))
        
        # Load audio
        audio = whisperx.load_audio(video_path)
        
        # Transcribe with WhisperX
        transcription = model.transcribe(
            audio, 
            batch_size = CONFIG.batch_size
        )
        
        # Align whisper output
        result = whisperx.align(
            transcription["segments"],
            align_model,
            metadata,
            audio,
            CONFIG.device,
            return_char_alignments = False
        )
        
        # Concatenate all segments to form the full transcript
        full_text = " ".join([segment["text"].strip() for segment in result["segments"]])
        
        return {
            "video_id": Path(video_path).stem,
            "transcript": full_text,
            "metadata": video_metadata
        }
        
    except Exception as e:
        logger.error(f'Error processing {video_path}: {e}')
        return None

def upload_to_s3(df: pd.DataFrame, profile: str) -> None:
    """
    Upload DataFrame to S3 in parquet format.
    
    Args:
        df (pd.DataFrame): DataFrame to upload
        profile (str): Profile name for the file path
    """
    if not CONFIG.s3_bucket:
        logger.error("S3_BUCKET not configured, skipping upload")
        return
        
    try:
        # Create a temporary file for the parquet data
        temp_path = CONFIG.transcript_dir / f"{profile}.parquet"
        
        # Save DataFrame as parquet
        df.to_parquet(temp_path, index = False)
        
        # Upload to S3
        s3_key = f"{CONFIG.s3_prefix}/{profile}/data.parquet"
        s3_client = boto3.client('s3')
        s3_client.upload_file(str(temp_path), CONFIG.s3_bucket, s3_key)
        
        # Cleanup
        temp_path.unlink()
        
        logger.info(f"Successfully uploaded data for {profile} to S3")
        
    except Exception as e:
        logger.error(f"Error uploading to S3: {e}")

def process_profile(profile: str, model, align_model, metadata):
    """Process a single TikTok profile."""
    logger.info(f'Processing profile: {profile}')
    
    # Download videos
    profile_dir = download_videos(profile)
    if not profile_dir or not profile_dir.exists():
        logger.error(f'{profile_dir} not found')
        return
    
    try:
        # Process each video
        video_data = []
        for video_file in profile_dir.glob("*.wav"):
            result = process_video(
                str(video_file),
                model,
                align_model,
                metadata
            )
            if result:
                row = result['metadata']
                row['transcript'] = result['transcript']
                row['profile_name'] = profile
                row['upload_date'] = datetime.strptime(row['upload_date'], '%Y%m%d')
                row['processed_at'] = datetime.now().isoformat()
                video_data.append(row)
        
        if video_data:
            # Convert to DataFrame and upload
            df = pd.DataFrame(video_data)
            upload_to_s3(df, profile)
        
        # Ensure complete cleanup
        if profile_dir.exists():
            try:
                shutil.rmtree(profile_dir)
                logger.info(f'Cleaned up directory for {profile}')
            except Exception as e:
                logger.error(f'Error cleaning up directory for {profile}: {e}')
        
        logger.info(f'Completed processing {profile}')
        
    except Exception as e:
        logger.error(f'Error processing profile {profile}: {e}')
        # Cleanup even if processing failed
        if profile_dir and profile_dir.exists():
            try:
                shutil.rmtree(profile_dir)
            except Exception as cleanup_error:
                logger.error(f'Error cleaning up directory for {profile}: {cleanup_error}')

def read_profiles_from_s3(profile_s3_key: str) -> list[str]:
    """Read profiles from S3."""
    s3_client = boto3.client('s3')
    try:
        response = s3_client.get_object(
            Bucket = CONFIG.s3_bucket,
            Key = profile_s3_key
        )
        content = response['Body'].read().decode('utf-8')
        return [line.strip() for line in content.splitlines() if line.strip()]
    except Exception as e:
        logger.error(f'Error reading profiles from S3: {e}')
        return []

def main():
    """Main execution function."""
    setup_directories()
    
    # Read profiles from S3 if PROFILES_S3_KEY is set
    if CONFIG.profiles_s3_key:
        profiles = read_profiles_from_s3(CONFIG.profiles_s3_key)
    else:
        # Fallback to local file
        if len(sys.argv) != 2:
            print("Usage: python main.py profiles.txt")
            sys.exit(1)
        profiles = read_profiles(sys.argv[1])
    
    # Load WhisperX model
    model = whisperx.load_model(
        CONFIG.model_name,
        CONFIG.device,
        compute_type = CONFIG.compute_type
    )
    
    # Load alignment model
    align_model, metadata = whisperx.load_align_model(
        language_code = "en",
        device = CONFIG.device
    )
    
    # Process each profile
    for profile in profiles:
        process_profile(profile, model, align_model, metadata)

if __name__ == "__main__":
    CONFIG = Config()
    main() 