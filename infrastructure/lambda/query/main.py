import json
import mysql.connector
import boto3
import os
from typing import Dict, Any

def get_secret(secret_id: str) -> str:
    """Get secret from AWS Secrets Manager"""
    client = boto3.client('secretsmanager')
    response = client.get_secret_value(SecretId=secret_id)
    return response['SecretString']

def get_db_connection():
    """Create MySQL connection"""
    ssm = boto3.client('ssm')
    host = ssm.get_parameter(Name='/tiktok-analytics/db-endpoint')['Parameter']['Value']
    password = get_secret(os.environ['DB_SECRET_ARN'])
    
    return mysql.connector.connect(
        host=host,
        user='root',
        password=password,
        database='tiktok_analytics'
    )

def get_profile_stats(profile_name: str, conn) -> Dict[str, Any]:
    """Get profile statistics"""
    # Get statistics using SQL
    stats_query = """
    SELECT 
        -- View count statistics
        SUM(view_count) as total_views,
        MIN(view_count) as min_views,
        MAX(view_count) as max_views,
        AVG(view_count) as mean_views,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY view_count) as p25_views,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY view_count) as median_views,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY view_count) as p75_views,
        
        -- Like count statistics
        SUM(like_count) as total_likes,
        MIN(like_count) as min_likes,
        MAX(like_count) as max_likes,
        AVG(like_count) as mean_likes,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY like_count) as p25_likes,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY like_count) as median_likes,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY like_count) as p75_likes,
        
        -- Repost count statistics
        SUM(repost_count) as total_reposts,
        MIN(repost_count) as min_reposts,
        MAX(repost_count) as max_reposts,
        AVG(repost_count) as mean_reposts,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY repost_count) as p25_reposts,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY repost_count) as median_reposts,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY repost_count) as p75_reposts,
        
        -- Comment count statistics
        SUM(comment_count) as total_comments,
        MIN(comment_count) as min_comments,
        MAX(comment_count) as max_comments,
        AVG(comment_count) as mean_comments,
        PERCENTILE_CONT(0.25) WITHIN GROUP (ORDER BY comment_count) as p25_comments,
        PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY comment_count) as median_comments,
        PERCENTILE_CONT(0.75) WITHIN GROUP (ORDER BY comment_count) as p75_comments,
        
        -- Total video count
        COUNT(*) as video_count
    FROM tiktok_data 
    WHERE profile_name = %s
    """
    
    # Get random sample using SQL
    sample_query = """
    SELECT id, title, description, upload_date, duration,
           view_count, like_count, repost_count, comment_count,
           transcript, processed_at, category, summary, language
    FROM tiktok_data 
    WHERE profile_name = %s
    ORDER BY RAND()
    LIMIT 100
    """
    
    cursor = conn.cursor(dictionary=True)
    
    # Get statistics
    cursor.execute(stats_query, (profile_name,))
    stats_row = cursor.fetchone()
    
    if not stats_row or stats_row['video_count'] == 0:
        return {
            'error': 'Profile not found'
        }
    
    # Format statistics
    stats = {
        'view_count': {
            'total': int(stats_row['total_views']),
            'min': int(stats_row['min_views']),
            'max': int(stats_row['max_views']),
            'mean': float(stats_row['mean_views']),
            'median': float(stats_row['median_views']),
            'percentile_25': float(stats_row['p25_views']),
            'percentile_75': float(stats_row['p75_views'])
        },
        'like_count': {
            'total': int(stats_row['total_likes']),
            'min': int(stats_row['min_likes']),
            'max': int(stats_row['max_likes']),
            'mean': float(stats_row['mean_likes']),
            'median': float(stats_row['median_likes']),
            'percentile_25': float(stats_row['p25_likes']),
            'percentile_75': float(stats_row['p75_likes'])
        },
        'repost_count': {
            'total': int(stats_row['total_reposts']),
            'min': int(stats_row['min_reposts']),
            'max': int(stats_row['max_reposts']),
            'mean': float(stats_row['mean_reposts']),
            'median': float(stats_row['median_reposts']),
            'percentile_25': float(stats_row['p25_reposts']),
            'percentile_75': float(stats_row['p75_reposts'])
        },
        'comment_count': {
            'total': int(stats_row['total_comments']),
            'min': int(stats_row['min_comments']),
            'max': int(stats_row['max_comments']),
            'mean': float(stats_row['mean_comments']),
            'median': float(stats_row['median_comments']),
            'percentile_25': float(stats_row['p25_comments']),
            'percentile_75': float(stats_row['p75_comments'])
        }
    }
    
    # Get sample videos
    cursor.execute(sample_query, (profile_name,))
    sample_rows = cursor.fetchall()
    
    # Format sample data
    sample_list = []
    for row in sample_rows:
        row['upload_date'] = row['upload_date'].strftime('%Y-%m-%d %H:%M:%S')
        row['processed_at'] = row['processed_at'].strftime('%Y-%m-%d %H:%M:%S')
        # Convert numeric types to Python native types
        for key in ['duration', 'view_count', 'like_count', 'repost_count', 'comment_count']:
            row[key] = int(row[key])
        sample_list.append(row)
    
    cursor.close()
    
    return {
        'stats': stats,
        'video_count': stats_row['video_count'],
        'sample_videos': sample_list
    }

def handler(event, context):
    """Lambda handler"""
    try:
        # Get profile name from query parameters
        if not event.get('queryStringParameters') or 'profile' not in event['queryStringParameters']:
            return {
                'statusCode': 400,
                'body': json.dumps('Profile name is required')
            }
        
        profile_name = event['queryStringParameters']['profile']
        
        # Get database connection
        conn = get_db_connection()
        
        # Get profile statistics
        result = get_profile_stats(profile_name, conn)
        
        conn.close()
        
        if 'error' in result:
            return {
                'statusCode': 404,
                'body': json.dumps(result)
            }
        
        return {
            'statusCode': 200,
            'body': json.dumps(result)
        }
        
    except Exception as e:
        return {
            'statusCode': 500,
            'body': json.dumps(f'Error: {str(e)}')
        } 