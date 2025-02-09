import os
import logging
import torch
from pathlib import Path

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
        
        # Transcription parameters
        self.transcribed_per_profile = 100
        
CONFIG = Config()