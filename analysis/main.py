import os
import logging
import time
from pathlib import Path
import boto3
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class TikTokAnalyzer:
    def __init__(self):
        """Initialize the TikTok data analyzer."""
        self.athena_client = boto3.client('athena')
        self.s3_client = boto3.client('s3')
        self.bucket = 'tiktoktrends'
        self.database = 'tiktok_analysis'
        self.output_location = f's3://{self.bucket}/tables/'

    def repair_partitions(self):
        """Repair table partitions."""
        query = f"MSCK REPAIR TABLE {self.database}.tiktok_data"
        self.run_query(query)
        logger.info("Repaired table partitions")

    def generate_engagement_stats(self):
        """Generate engagement statistics by profile, language, and category."""
        metrics = ['like_count', 'repost_count', 'comment_count', 'view_count']
        groups = ['profile_name', 'language', 'category']

        for group in groups:
            query = f"""
            WITH stats AS (
                SELECT 
                    {group},
                    COUNT(*) as video_count,
                    {','.join([f'''
                    ROUND(AVG({metric})) as avg_{metric},
                    ROUND(APPROX_PERCENTILE({metric}, 0.5)) as median_{metric},
                    ROUND(APPROX_PERCENTILE({metric}, 0.25)) as p25_{metric},
                    ROUND(APPROX_PERCENTILE({metric}, 0.75)) as p75_{metric},
                    MIN({metric}) as min_{metric},
                    MAX({metric}) as max_{metric}
                    ''' for metric in metrics])}
                FROM {self.database}.tiktok_data
                GROUP BY {group}
                HAVING COUNT(*) >= 5
            )
            SELECT *
            FROM stats
            ORDER BY video_count DESC
            """
            self.run_query(query, f"engagement/by_{group}")

    def analyze_keywords(self):
        """Analyze keyword frequency by profile, language, and category."""
        groups = ['profile_name', 'language', 'category']

        for group in groups:
            query = f"""
            WITH exploded AS (
                SELECT 
                    {group},
                    kw as keyword
                FROM {self.database}.tiktok_data
                CROSS JOIN UNNEST(keywords) as t(kw)
            ),
            ranked AS (
                SELECT 
                    {group},
                    keyword,
                    COUNT(*) as frequency,
                    ROW_NUMBER() OVER (PARTITION BY {group} ORDER BY COUNT(*) DESC) as rank
                FROM exploded
                GROUP BY {group}, keyword
            )
            SELECT *
            FROM ranked
            WHERE rank <= 10
            ORDER BY {group}, rank
            """
            self.run_query(query, f"keywords/top_keywords_by_{group}")

    def analyze_video_duration(self):
        """Analyze video duration patterns."""
        query = """
        WITH duration_stats AS (
            SELECT 
                profile_name,
                language,
                category,
                ROUND(AVG(duration)) as avg_duration,
                ROUND(APPROX_PERCENTILE(duration, 0.5)) as median_duration,
                MIN(duration) as min_duration,
                MAX(duration) as max_duration,
                COUNT(*) as video_count,
                SUM(CASE WHEN duration <= 30 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_short,
                SUM(CASE WHEN duration > 30 AND duration <= 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_medium,
                SUM(CASE WHEN duration > 60 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as pct_long
            FROM tiktok_data
            GROUP BY profile_name, language, category
            HAVING COUNT(*) >= 5
        )
        SELECT *
        FROM duration_stats
        ORDER BY video_count DESC
        """
        self.run_query(query, "duration/duration_analysis")

    def analyze_upload_patterns(self):
        """Analyze video upload patterns."""
        query = """
        WITH upload_stats AS (
            SELECT 
                profile_name,
                language,
                category,
                DATE_TRUNC('month', upload_date) as month,
                COUNT(*) as uploads,
                AVG(like_count) as avg_likes,
                AVG(view_count) as avg_views
            FROM tiktok_data
            GROUP BY profile_name, language, category, DATE_TRUNC('month', upload_date)
        )
        SELECT 
            profile_name,
            language,
            category,
            COUNT(DISTINCT month) as active_months,
            ROUND(AVG(uploads)) as avg_uploads_per_month,
            ROUND(AVG(avg_likes)) as avg_likes_per_video,
            ROUND(AVG(avg_views)) as avg_views_per_video
        FROM upload_stats
        GROUP BY profile_name, language, category
        HAVING COUNT(DISTINCT month) >= 2
        ORDER BY avg_views_per_video DESC
        """
        self.run_query(query, "upload_patterns/monthly_stats")

    def test_data_presence(self):
        """Run a simple test query to check data presence and partitioning."""
        query = f"""
        SELECT 
            COUNT(*) as total_rows,
            COUNT(DISTINCT profile_name) as unique_profiles,
            COUNT(DISTINCT language) as unique_languages,
            COUNT(DISTINCT category) as unique_categories,
            MIN(upload_date) as earliest_date,
            MAX(upload_date) as latest_date
        FROM {self.database}.tiktok_data
        """
        self.run_query(query, "overview/data_summary")
        
        # Additional check for partition distribution
        query_partitions = """
        SELECT 
            profile_name,
            language,
            category,
            COUNT(*) as video_count,
            MIN(upload_date) as first_video,
            MAX(upload_date) as last_video,
            ROUND(AVG(duration)) as avg_duration_seconds,
            ROUND(AVG(view_count)) as avg_views,
            ROUND(AVG(like_count)) as avg_likes
        FROM tiktok_data
        GROUP BY profile_name, language, category
        ORDER BY video_count DESC
        """
        self.run_query(query_partitions, "overview/profile_statistics")

    def run_query(self, query: str, output_name: str = None) -> bool:
        """Execute Athena query and wait for completion."""
        try:
            # Construct full output location with output name
            output_location = (
                f"{self.output_location}{output_name}"
                if output_name
                else self.output_location
            )

            response = self.athena_client.start_query_execution(
                QueryString=query,
                QueryExecutionContext={'Database': self.database},
                ResultConfiguration={'OutputLocation': output_location}
            )
            
            execution_id = response['QueryExecutionId']
            
            # Wait for query to complete
            while True:
                response = self.athena_client.get_query_execution(QueryExecutionId=execution_id)
                state = response['QueryExecution']['Status']['State']
                
                if state in ['SUCCEEDED', 'FAILED', 'CANCELLED']:
                    break
                    
                time.sleep(1)
            
            if state != 'SUCCEEDED':
                error = response['QueryExecution']['Status'].get('StateChangeReason', 'Unknown error')
                logger.error(f"Query failed: {error}")
                return False
            
            if output_name:
                logger.info(f"Query completed successfully: {output_name}")
            
            return True
                
        except Exception as e:
            logger.error(f"Error running query: {e}")
            return False

def main():
    analyzer = TikTokAnalyzer()
    
    # Repair partitions first
    # analyzer.repair_partitions()
    
    # Run test query first
    analyzer.test_data_presence()
    
    # Run analyses
    analyzer.generate_engagement_stats()
    analyzer.analyze_keywords()
    analyzer.analyze_video_duration()
    analyzer.analyze_upload_patterns()

if __name__ == "__main__":
    main()