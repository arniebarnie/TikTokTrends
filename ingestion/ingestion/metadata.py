from pathlib import Path
import json
import yt_dlp
import pandas as pd
from datetime import datetime
from typing import Union, Optional

from .config import CONFIG, LOGGER

class VideoMetadata:
    def __init__(self):
        self.ydl_opts = {
            'quiet': True,
            'no_warnings': True,
            'writeinfojson': True,
            'skip_download': True,
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
                    metadata['profile'] = profile
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
