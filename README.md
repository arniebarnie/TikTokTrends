<p align="center">
<img height="150" width="150" src="https://cdn.simpleicons.org/tiktok/gray"/>
</p>

<h1 align="center">TikTok Analytics Pipeline</h1>

<p align="center">
    <img src="https://img.shields.io/badge/AWS-232F3E?style=flat-square&logo=amazonaws&logoColor=white"/>
    <img src="https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white"/>
    <img src="https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white"/>
    <img src="https://img.shields.io/badge/AWS_CDK-232F3E?style=flat-square&logo=amazonaws&logoColor=white"/>
</p>

## Overview
This project implements a scalable data pipeline for analyzing TikTok videos using AWS services. It extracts video metadata and content, performs transcription and text analysis, and stores the results for further analysis.

## Dashboard
A Streamlit that visualizes insights from the analyzed TikTok data is publicly accessible at [tiktoktrends.streamlit.app](https://tiktoktrends.streamlit.app/).

### Features
- **Video Metrics**: Detailed statistics on views, likes, comments, and repost rates
- **Content Analysis**: Breakdown of content categories and their performance
- **Keyword Analysis**: AI-powered analysis of which keywords drive the most views
- **Interaction Patterns**: Insights into user engagement patterns across different content types

### Visualizations
- Interactive histograms showing metric distributions
- Bar charts comparing performance across content categories
- Time series analysis of engagement trends
- Keyword impact analysis with predictive modeling

## Architecture

### ETL Pipeline

1. **Metadata Extraction**
   - Downloads video metadata using yt-dlp
   - Extracts key information like views, likes, comments
   - Runs on AWS Batch Fargate containers
   - Stores results in S3

2. **Video Transcription** 
   - Triggered automatically via SNS when new metadata arrives in S3
   - Uses WhisperX for GPU-accelerated transcription
   - Runs on AWS Batch GPU instances (g4dn.xlarge)
   - Stores transcripts in S3

3. **Text Analysis**
   - Triggered automatically via SNS when new transcripts arrive in S3
   - Extracts categories, summaries, and keywords from transcripts and metadata using OpenAI GPT
   - Runs on AWS Batch Fargate containers
   - Stores analysis results in S3

### Data Lake Structure
The data is organized in an S3-based data lake with the following structure:

```
s3://tiktoktrends/
├── videos/
│   ├── metadata/                 # Raw video metadata
│   │   ├── profile=<profile>/
│   │   │   └── processed_at=<timestamp>/
│   │   │       └── metadata.parquet
│   ├── transcripts/             # Video transcripts
│   │   └── profile=<profile>/
│   │       └── processed_at=<timestamp>/
│   │           └── transcripts.parquet
│   └── text/                    # Analyzed text data
│       ├── profile=<profile>/
│       │   └── processed_at=<timestamp>/
│       │       └── analysis.parquet
└── athena-results/              # Athena query results
```

### AWS Glue Data Catalog
The data lake is cataloged using AWS Glue with the following structure:

#### Database: `tiktok_analytics`
Contains tables for querying TikTok video metadata and text analysis results.

##### Table: `metadata`
Stores video metadata in Parquet format with Hive-style partitioning:
- Partition Keys:
  - `profile` (string)
  - `processed_at` (string)
- Columns:
  - `id` (string)
  - `title` (string)
  - `description` (string)
  - `upload_date` (bigint)
  - `like_count` (bigint)
  - `repost_count` (bigint)
  - `comment_count` (bigint)
  - `view_count` (bigint)
  - `duration` (bigint)
  - `webpage_url` (string)
  - `channel` (string)
  - `timestamp` (bigint)
  - `track` (string)
  - `artists` (array<string>)
  - `artist` (string)
  - `uploader` (string)

##### Table: `text_analysis`
Stores processed text data in Parquet format with Hive-style partitioning:
- Partition Keys:
  - `profile` (string)
  - `processed_at` (string)
- Columns:
  - `id` (string)
  - `uploader` (string)
  - `description` (string)
  - `title` (string)
  - `transcript` (string)
  - `language` (string)
  - `category` (string)
  - `summary` (string)
  - `keywords` (array<string>)

#### Partition Management
The system automatically manages partitions through Lambda functions triggered by S3 events using Athena ALTER TABLE statements, ensuring that new data is immediately queryable through Athena without manual intervention or partition discovery jobs.

### Infrastructure

The pipeline is built using AWS CDK with Python and includes:

- **Networking**: VPC with public/private subnets
- **Compute**: AWS Batch compute environments (ECS and Fargate)
- **Storage**: S3 buckets for data storage
- **Serverless**: SNS and Lambda functions for pipeline orchestration
- **Security**: IAM roles and security groups
- **Containers**: ECR repositories for Docker images

## Technologies Used

### Core Services
![aws-batch](https://img.shields.io/badge/AWS_Batch-232F3E?style=flat-square&logo=amazonaws&logoColor=white)
![aws-lambda](https://img.shields.io/badge/AWS_Lambda-FF9900?style=flat-square&logo=awslambda&logoColor=white)
![aws-s3](https://img.shields.io/badge/Amazon_S3-569A31?style=flat-square&logo=amazons3&logoColor=white)
![aws-sns](https://img.shields.io/badge/Amazon_SNS-FF4F8B?style=flat-square&logo=amazonaws&logoColor=white)
![aws-ecr](https://img.shields.io/badge/Amazon_ECR-232F3E?style=flat-square&logo=amazonaws&logoColor=white)
![aws-glue](https://img.shields.io/badge/AWS_Glue-232F3E?style=flat-square&logo=amazonaws&logoColor=white)
![aws-athena](https://img.shields.io/badge/Amazon_Athena-232F3E?style=flat-square&logo=amazonaws&logoColor=white)

### AI/ML
![openai](https://img.shields.io/badge/OpenAI-412991?style=flat-square&logo=openai&logoColor=white)
![pytorch](https://img.shields.io/badge/PyTorch-EE4C2C?style=flat-square&logo=pytorch&logoColor=white)
![whisper](https://img.shields.io/badge/Whisper-000000?style=flat-square&logo=openai&logoColor=white)

### Development
![python](https://img.shields.io/badge/Python-3776AB?style=flat-square&logo=python&logoColor=white)
![docker](https://img.shields.io/badge/Docker-2496ED?style=flat-square&logo=docker&logoColor=white)
![aws-cdk](https://img.shields.io/badge/AWS_CDK-232F3E?style=flat-square&logo=amazonaws&logoColor=white)

### Visualization
![streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)

## Pipeline Flow

```mermaid
graph LR
    A[TikTok Videos] --> B[Metadata Extraction]
    B --> C[Metadata Storage]
    C --> D[Video Transcription]
    D --> E[Transcript Storage]
    E --> F[Text Analysis]
    F --> G[Analysis Storage]
```

## License
This project is licensed under the MIT License - see the LICENSE file for details.