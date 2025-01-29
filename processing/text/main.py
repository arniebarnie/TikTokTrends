import os
import logging
import tempfile
import json
from pathlib import Path
import pandas as pd
import boto3
from openai import OpenAI

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Define categories
CATEGORIES = [
    "Dance", "Comedy/Skits", "Education/Tutorials", "Fitness/Workouts",
    "Beauty/Makeup", "Fashion/Style", "Food/Cooking", "Travel/Adventure",
    "Technology/Gadgets", "Motivational/Inspirational", "DIY/Crafts",
    "Gaming", "Pets/Animals", "Music/Singing", "Life Hacks",
    "Relationships/Dating", "Parenting/Family", "Memes/Trends",
    "Health/Wellness", "Science/Experiments"
]

class TextProcessor:
    def __init__(self):
        # OpenAI Configuration
        self.openai_api_key = os.getenv('OPENAI_API_KEY')
        if not self.openai_api_key:
            raise ValueError("OPENAI_API_KEY environment variable is required")
            
        # AWS Configuration
        self.s3_bucket = os.getenv('S3_BUCKET', 'tiktoktrends')
        self.s3_prefix = 'data'
        
        # Load prompt template
        prompt_path = Path(__file__).parent / "prompt_template.txt"
        with open(prompt_path, 'r') as f:
            self.prompt_template = f.read()
        
        # Initialize clients
        self.s3_client = boto3.client('s3')
        self.openai_client = OpenAI(api_key=self.openai_api_key)
        
    def get_profiles(self) -> list[str]:
        """Get list of profiles from S3 bucket."""
        try:
            response = self.s3_client.list_objects_v2(
                Bucket = self.s3_bucket,
                Prefix = f"{self.s3_prefix}/"
            )
            
            profiles = []
            for obj in response.get('Contents', []):
                parts = obj['Key'].split('/')
                if len(parts) == 3 and parts[2] == 'data.parquet':
                    profiles.append(parts[1])
            
            return profiles
            
        except Exception as e:
            logger.error(f"Error listing profiles: {e}")
            return []
        
    def get_profile_data(self, profile: str):
        """Process data for a single profile."""
        try:
            # Construct S3 path for the profile's data file
            s3_key = f"{self.s3_prefix}/{profile}/data.parquet"
            
            # Create and use temporary directory that auto-deletes
            with tempfile.TemporaryDirectory() as temp_dir:
                local_path = os.path.join(temp_dir, "data.parquet")
                
                # Download parquet file to temporary location
                self.s3_client.download_file(
                    Bucket = self.s3_bucket,
                    Key = s3_key,
                    Filename = local_path
                )
                
                # Read parquet file into dataframe
                df = pd.read_parquet(local_path)
                
                return df
            
        except Exception as e:
            logger.error(f"Error processing profile {profile}: {e}")
            return None
    
    def process_profile(self, profile: str):
        """Process data for a single profile."""
        df = self.get_profile_data(profile)
        if df is None:
            return None
        
        # Initialize new columns
        df['language'] = None
        df['category'] = None
        df['summary'] = ''
        df['keywords'] = [[] for _ in range(len(df))]
        
        # Process each video and update dataframe
        for index, row in df.iterrows():
            try:
                analysis = self.process_video(row)
                if analysis:
                    df.at[index, 'language'] = analysis['language']
                    df.at[index, 'category'] = analysis['category']
                    df.at[index, 'summary'] = analysis['summary']
                    df.at[index, 'keywords'] = analysis['keywords']
                    
            except Exception as e:
                logger.error(f"Error processing video at index {index}: {e}")
        
        # Save updated DataFrame back to S3
        try:
            with tempfile.TemporaryDirectory() as temp_dir:
                temp_path = Path(temp_dir) / "data.parquet"
                df.to_parquet(temp_path)
                
                s3_key = f"{self.s3_prefix}/{profile}/data.parquet"
                self.s3_client.upload_file(str(temp_path), self.s3_bucket, s3_key)
                logger.info(f"Successfully saved processed data for profile {profile}")
        except Exception as e:
            logger.error(f"Error saving processed data to S3: {e}")
        
        return df
    
    def process_video(self, video: dict):
        """Process a single video and return analysis."""
        # Format the categories as a bullet list
        categories_list = "\n".join([f"        - {cat}" for cat in CATEGORIES])
        
        # Format the prompt template
        prompt = self.prompt_template.format(
            categories_list=categories_list,
            title=video.get('title', ''),
            description=video.get('description', ''),
            transcript=video.get('transcript', '')
        )
        
        # Call OpenAI API
        try:
            response = self.openai_client.chat.completions.create(
                model="gpt-3.5-turbo",
                messages=[
                    {
                        "role": "system",
                        "content": "You are an expert content analyzer. Respond only with valid JSON."
                    },
                    {"role": "user", "content": prompt}
                ],
                temperature=0.2,
                response_format={"type": "json_object"}  # Force JSON response
            )
            
            # Parse the JSON response
            analysis = json.loads(response.choices[0].message.content)
            return analysis
            
        except Exception as e:
            logger.error(f"Error calling OpenAI API: {e}")
            return None

def main():
    processor = TextProcessor()
    
    for profile in processor.get_profiles():
        logger.info(f"Processing {profile}...")
        processor.process_profile(profile)
        logger.info(f"Processed {profile}")

if __name__ == "__main__":
    main()