from pathlib import Path
import whisperx
import yt_dlp
from typing import List, Optional, Dict, Any
import shutil
import tempfile
import boto3
import pandas as pd
from datetime import datetime
import logging
import torch
import os

# Configure logging
logging.basicConfig(level = logging.INFO)
LOGGER = logging.getLogger(__name__)

# Check GPU availability
if not torch.cuda.is_available():
    LOGGER.error("No GPU detected")
else:
    LOGGER.info(f"GPU detected: {torch.cuda.get_device_name(0)}")
    LOGGER.info(f"CUDA Version: {torch.version.cuda}")

class Config:
    def __init__(self):
        # AWS Configuration
        self.s3_bucket = os.getenv('S3_BUCKET', 'tiktoktrends')
        self.s3_prefix = 'videos/transcripts'
        self.metadata_s3_key = os.getenv('METADATA_S3_KEY')
        
        # Whisper Configuration
        self.device = "cuda" if torch.cuda.is_available() else "cpu"
        self.compute_type = "float16" if self.device == "cuda" else "float32"
        self.model_name = "base.en"
        self.batch_size = 32
        
        # Local paths
        self.workspace = Path("/workspace")
        self.download_dir = self.workspace / "downloads"
        
        # Transcription parameters
        self.max_transcribed_per_profile = 1000
CONFIG = Config()

s3_client = boto3.client('s3')

def setup_directories():
    CONFIG.download_dir.mkdir(parents = True, exist_ok = True)

def read_metadata_from_s3(metadata_s3_key: str) -> List[Dict[str, Any]]:
    """read in parquet file from s3"""
    return pd.read_parquet(f's3://{CONFIG.s3_bucket}/{metadata_s3_key}')

def upload_to_s3(df: pd.DataFrame, key: str):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / 'data.parquet'
        df.to_parquet(temp_path)
        s3_client.upload_file(str(temp_path), CONFIG.s3_bucket, key)

class Transcriber:
    def __init__(self, model: Any, align_model: Any, metadata: Dict):
        """
        Initialize Transcriber with pre-loaded WhisperX models.
        
        Args:
            model: WhisperX base model
            align_model: WhisperX alignment model
            metadata: Alignment model metadata
        """
        # YT-DLP configuration
        self.ydl_opts = {
            'format': 'bestaudio/best',
            'postprocessors': [{
                'key': 'FFmpegExtractAudio',
                'preferredcodec': 'wav',
            }],
            'outtmpl': str(CONFIG.download_dir) + '/%(uploader)s/%(id)s.%(ext)s',
            'ignoreerrors': True,
            'quiet': True,
            'no_warnings': True,
            'extractor_args': {
                'tiktok': {
                    'app_name': 'trill',
                    'app_version': '34.1.2',
                    'manifest_app_version': '2023401020'
                }
            }
        }
        
        # Store WhisperX models
        self.model = model
        self.align_model = align_model
        self.metadata = metadata
    
    def download_videos(self, profile: str, video_ids: List[str]) -> List[Optional[Path]]:
        """
        Download multiple videos' audio in a single batch.
        
        Args:
            profile (str): TikTok profile name
            video_ids (List[str]): List of video IDs
            
        Returns:
            List[Optional[Path]]: List of paths to downloaded audio files
        """
        try:
            # Create profile directory
            profile_dir = CONFIG.download_dir / profile
            profile_dir.mkdir(parents = True, exist_ok = True)
            
            # Create list of URLs
            urls = [
                f"https://www.tiktok.com/@{profile}/video/{video_id}"
                for video_id in video_ids
            ]
            
            # Download all videos in one call
            with yt_dlp.YoutubeDL(self.ydl_opts) as ydl:
                ydl.download(urls)
            
            # Check for downloaded files
            audio_paths = []
            for video_id in video_ids:
                wav_file = profile_dir / f"{video_id}.wav"
                if wav_file.exists():
                    audio_paths.append(wav_file)
                else:
                    LOGGER.error(f"Downloaded file not found for {video_id}")
                    audio_paths.append(None)
                    
            return audio_paths
            
        except Exception as e:
            LOGGER.error(f"Error downloading videos for {profile}: {e}")
            return [None] * len(video_ids)

    def transcribe_audio(self, audio_path: Path) -> Optional[str]:
        """
        Transcribe audio file using WhisperX.
        
        Args:
            audio_path (Path): Path to audio file
            
        Returns:
            Optional[str]: Transcription text
        """
        try:
            # Load audio
            audio = whisperx.load_audio(str(audio_path))
            
            # Transcribe with WhisperX
            result = self.model.transcribe(
                audio, 
                batch_size = CONFIG.batch_size
            )
            
            # Align whisper output
            aligned = whisperx.align(
                result["segments"],
                self.align_model,
                self.metadata,
                audio,
                CONFIG.device,
                return_char_alignments = False
            )
            
            # Concatenate all segments
            transcript = " ".join([
                segment["text"].strip() 
                for segment in aligned["segments"]
            ])
            
            return transcript
            
        except Exception as e:
            LOGGER.error(f"Error transcribing {audio_path}: {e}")
            return None

    def process_videos(self, profile: str, video_ids: List[str]) -> List[Optional[str]]:
        """
        Process multiple videos and return their transcripts.
        
        Args:
            profile (str): TikTok profile name
            video_ids (List[str]): List of video IDs to process
            
        Returns:
            List[Optional[str]]: List of transcripts in same order as video_ids
                               (None for failed transcriptions)
        """
        try:
            # Download all videos in batch
            audio_paths = self.download_videos(profile, video_ids)
            
            # Process each audio file
            transcripts = []
            for audio_path in audio_paths:
                if not audio_path:
                    transcripts.append(None)
                    continue
                
                # Transcribe audio
                transcript = self.transcribe_audio(audio_path)
                transcripts.append(transcript)
            
            # Cleanup profile directory
            profile_dir = CONFIG.download_dir / profile
            if profile_dir.exists():
                shutil.rmtree(profile_dir)
                
            return transcripts
            
        except Exception as e:
            LOGGER.error(f"Error processing videos for {profile}: {e}")
            return [None] * len(video_ids)
        
def main():
    
    if CONFIG.metadata_s3_key:
        video_metadata = read_metadata_from_s3(CONFIG.metadata_s3_key)
    else:
        video_metadata = read_metadata_from_s3(
            "videos/metadata/PROFILE=noonessafe_pranks/PROCESSED_AT=2025-02-09 11:02:28/metadata.parquet"
        )
    if 'PROFILE' in video_metadata.columns:
        video_metadata.drop(columns = ['PROFILE'], inplace = True)
    if 'PROCESSED_AT' in video_metadata.columns:
        video_metadata.drop(columns = ['PROCESSED_AT'], inplace = True)
    
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
    
    transcriber = Transcriber(model, align_model, metadata)
    
    num_to_sample = min(CONFIG.max_transcribed_per_profile, len(video_metadata))
    sampled_metadata = video_metadata.sample(num_to_sample)
    
    for profile in sampled_metadata['profile'].unique():
        LOGGER.info(f"Processing profile: {profile}")
        sampled_metadata_for_profile = sampled_metadata[sampled_metadata['profile'] == profile]
        sampled_ids = sampled_metadata_for_profile['id'].tolist()
        transcripts = transcriber.process_videos(profile, sampled_ids)
        
        transcripts_df = pd.DataFrame({
            'id': sampled_ids,
            'profile': [profile] * len(sampled_ids),
            'description': sampled_metadata_for_profile['description'].tolist(),
            'title': sampled_metadata_for_profile['title'].tolist(),
            'transcript': transcripts
        })
        
        # Save transcripts to S3
        LOGGER.info(f"Saving transcripts to S3 for {profile}")
        current_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        s3_key = f"{CONFIG.s3_prefix}/PROFILE={profile}/PROCESSED_AT={current_time}/transcripts.parquet"
        upload_to_s3(transcripts_df, s3_key)
        LOGGER.info(f"Transcripts saved to S3: {s3_key}")

if __name__ == "__main__":
    main()