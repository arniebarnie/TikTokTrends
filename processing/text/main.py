import os
import logging
import tempfile
import json
from pathlib import Path
import pandas as pd
import boto3
from openai import OpenAI
from datetime import datetime
# Configure logging
logging.basicConfig(level = logging.INFO)
LOGGER = logging.getLogger(__name__)

# Define categories
CATEGORIES = [
    "Dance", "Comedy/Skits", "Education/Tutorials", "Fitness/Workouts",
    "Beauty/Makeup", "Fashion/Style", "Food/Cooking", "Travel/Adventure",
    "Technology/Gadgets", "Motivational/Inspirational", "DIY/Crafts",
    "Gaming", "Pets/Animals", "Music/Singing", "Life Hacks",
    "Relationships/Dating", "Parenting/Family", "Memes/Trends",
    "Health/Wellness", "Science/Experiments"
]

class Config:
    def __init__(self):
        # Get secret ARN from environment
        secret_arn = os.getenv('OPENAI_SECRET_ARN')
        if not secret_arn:
            raise ValueError("OPENAI_SECRET_ARN environment variable is required")
        
        # Get OpenAI API key from Secrets Manager
        secrets_client = boto3.client('secretsmanager')
        try:
            response = secrets_client.get_secret_value(SecretId = secret_arn)
            secret = json.loads(response['SecretString'])
            self.openai_api_key = secret['OPENAI_API_KEY']
        except Exception as e:
            raise ValueError(f"Failed to get OpenAI API key from Secrets Manager: {e}")
        
        self.s3_bucket = os.getenv('S3_BUCKET', 'tiktoktrends')
        self.s3_prefix = 'videos/text'
        self.s3_transcripts_key = os.getenv('TRANSCRIPTS_S3_KEY')
        
        # Load prompt template
        prompt_path = Path(__file__).parent / "prompt_template.txt"
        with open(prompt_path, 'r') as f:
            self.prompt_template = f.read()
        
        self.openai_client = OpenAI(api_key = self.openai_api_key)
        self.temperature = 0.2
        
        self.s3_client = boto3.client('s3')
CONFIG = Config()

def read_transcripts_from_s3(transcripts_s3_key: str) -> pd.DataFrame:
    """read in parquet file from s3"""
    return pd.read_parquet(f's3://{CONFIG.s3_bucket}/{transcripts_s3_key}')

def upload_to_s3(df: pd.DataFrame, key: str):
    with tempfile.TemporaryDirectory() as temp_dir:
        temp_path = Path(temp_dir) / 'data.parquet'
        df.to_parquet(temp_path)
        CONFIG.s3_client.upload_file(str(temp_path), CONFIG.s3_bucket, key)

class TextProcessor:
    def process_video(self, title, description, transcript) -> dict:
        categories_list = "\n".join([f"        - {cat}" for cat in CATEGORIES])
        
        prompt = CONFIG.prompt_template.format(
            categories_list = categories_list,
            title = title,
            description = description,
            transcript = transcript
        )
        
        # Call OpenAI API
        try:
            response = CONFIG.openai_client.chat.completions.create(
                model = "gpt-3.5-turbo",
                messages = [
                    {
                        "role": "system",
                        "content": "You are an expert content analyzer. Respond only with valid JSON."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature = CONFIG.temperature,
                response_format = {"type": "json_object"}  # Force JSON response
            )
            
            # Parse the JSON response
            analysis = json.loads(response.choices[0].message.content)
            return analysis
            
        except Exception as e:
            LOGGER.error(f"Error calling OpenAI API: {e}")
            return None
    
    def process_video_transcripts(self, video_transcripts: pd.DataFrame) -> pd.DataFrame:
        # Initialize new columns
        video_transcripts['language'] = None
        video_transcripts['category'] = None
        video_transcripts['summary'] = ''
        video_transcripts['keywords'] = [[] for _ in range(len(video_transcripts))]
        
        for index, row in video_transcripts.iterrows():
            try:
                analysis = self.process_video(row['title'], row['description'], row['transcript'])
                if analysis:
                    video_transcripts.at[index, 'language'] = analysis['language']
                    video_transcripts.at[index, 'category'] = analysis['category']
                    video_transcripts.at[index, 'summary'] = analysis['summary']
                    video_transcripts.at[index, 'keywords'] = analysis['keywords']
            except Exception as e:
                LOGGER.error(f"Error processing video {row['id']}: {e}")
                continue
        
        return video_transcripts

def main():
    if CONFIG.s3_transcripts_key:
        video_transcripts = read_transcripts_from_s3(CONFIG.s3_transcripts_key)
    else:
        video_transcripts = read_transcripts_from_s3(
            "videos/transcripts/profile=noonessafe_pranks/processed_at=2025-02-09 23:23:15/transcripts.parquet"
        )
        video_transcripts['profile'] = 'noonessafe_pranks'
    
    if 'profile' in video_transcripts.columns:
        video_transcripts.drop(columns = ['profile'], inplace = True)
    if 'processed_at' in video_transcripts.columns:
        video_transcripts.drop(columns = ['processed_at'], inplace = True)

    text_processor = TextProcessor()
    
    for profile in video_transcripts['uploader'].unique():
        LOGGER.info(f"Processing profile: {profile}")
        video_transcripts_for_profile = video_transcripts[video_transcripts['uploader'] == profile]
        processed_transcripts = text_processor.process_video_transcripts(video_transcripts_for_profile)
        
        # Save processed transcripts to S3
        LOGGER.info(f"Saving processed transcripts to S3 for {profile}...")
        current_time = datetime.now().strftime('%Y-%m-%d_%H:%M:%S')
        s3_key = f"{CONFIG.s3_prefix}/profile={profile}/processed_at={current_time}/text.parquet"
        upload_to_s3(processed_transcripts, s3_key)
        LOGGER.info(f"Transcripts saved to S3: {s3_key}")

if __name__ == "__main__":
    main()