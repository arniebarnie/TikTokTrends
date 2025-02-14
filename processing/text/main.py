import os
import logging
import tempfile
import json
from pathlib import Path
import pandas as pd
import boto3
from openai import OpenAI, RateLimitError
from datetime import datetime
import time
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

# Define OpenAI models in order of preference
OPENAI_MODELS = [
    "gpt-4o-mini",
    "gpt-3.5-turbo",
    "o1-mini"
]

class Config:
    def __init__(self):
        # Get secret ARN from environment
        secret_arn = os.getenv('OPENAI_SECRET_ARN')
        if not secret_arn:
            raise ValueError("OPENAI_SECRET_ARN environment variable is required")
        
        # LOGGER.info(secret_arn)
        # Get OpenAI API key from Secrets Manager
        secrets_client = boto3.client('secretsmanager')
        try:
            response = secrets_client.get_secret_value(SecretId = secret_arn)
            # LOGGER.info(response)
            secret = response['SecretString']
            # LOGGER.info(secret)
            self.openai_api_key = secret
        except Exception as e:
            raise ValueError(f"Failed to get OpenAI API key from Secrets Manager: {e}")
        
        self.s3_bucket = os.getenv('S3_BUCKET')
        self.s3_prefix = 'videos/text'
        self.s3_transcripts_key = os.getenv('TRANSCRIPTS_S3_KEY')
        
        # Load prompt template
        prompt_path = Path(__file__).parent / "prompt_template.txt"
        with open(prompt_path, 'r') as f:
            self.prompt_template = f.read()
        
        self.current_model_index = 0  # Track current model
        self.openai_client = OpenAI(api_key = self.openai_api_key)
        self.temperature = 0.2
        
        self.s3_client = boto3.client('s3')

    @property
    def current_model(self) -> str:
        """Get the current model to use"""
        return OPENAI_MODELS[self.current_model_index]
    
    def switch_to_next_model(self) -> str:
        """Switch to next model in the list, cycling back to start if needed"""
        self.current_model_index = (self.current_model_index + 1) % len(OPENAI_MODELS)
        LOGGER.info(f"Switching to model: {self.current_model}")
        return self.current_model

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
        
        # Try each model until success or all models fail
        for _ in range(len(OPENAI_MODELS)):
            try:
                response = CONFIG.openai_client.chat.completions.create(
                    model = CONFIG.current_model,
                    messages = [
                        {
                            "role": "system",
                            "content": "You are an expert content analyzer. Respond only with valid JSON."
                        },
                        {"role": "user", "content": prompt}
                    ],
                    temperature = CONFIG.temperature,
                    response_format = {"type": "json_object"}
                )
                
                # If successful, return the analysis
                analysis = json.loads(response.choices[0].message.content)
                return analysis
                
            except RateLimitError as e:
                LOGGER.warning(f"Rate limit hit for model {CONFIG.current_model}. Switching models...")
                CONFIG.switch_to_next_model()
                continue
                
            except Exception as e:
                LOGGER.error(f"Error calling OpenAI API with model {CONFIG.current_model}: {e}")
                return None
        
        LOGGER.error("All models failed - rate limits hit for all models")
        return None
    
    def process_video_transcripts(self, video_transcripts: pd.DataFrame) -> pd.DataFrame:
        # Initialize new columns
        video_transcripts['language'] = None
        video_transcripts['category'] = None
        video_transcripts['summary'] = ''
        video_transcripts['keywords'] = [[] for _ in range(len(video_transcripts))]
        
        for index, row in video_transcripts.iterrows():
            try:
                time.sleep(1)  # Sleep for 1 second to avoid rate limit
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
    video_transcripts = read_transcripts_from_s3(CONFIG.s3_transcripts_key)
    
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