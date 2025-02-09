from pathlib import Path
import whisperx
import yt_dlp
from typing import List, Optional, Dict, Any
import shutil

from .config import CONFIG, LOGGER

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
                
                # Cleanup audio file
                audio_path.unlink(missing_ok = True)
            
            # Cleanup profile directory if empty
            profile_dir = CONFIG.download_dir / profile
            if profile_dir.exists() and not any(profile_dir.iterdir()):
                profile_dir.rmdir()
                
            return transcripts
            
        except Exception as e:
            LOGGER.error(f"Error processing videos for {profile}: {e}")
            return [None] * len(video_ids)