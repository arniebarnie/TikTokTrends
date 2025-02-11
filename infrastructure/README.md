# TikTok Analytics Infrastructure

### Storage Stack
- S3 bucket for data storage
- Glue database and tables
- Data lifecycle management

### Secrets Stack
- OpenAI API key management
- IAM roles for secret access

### Network Stack
- VPC configuration
- Public/Private subnets
- Security groups

### Container Stack
- ECR repositories for:
  - Metadata extraction
  - Video transcription
  - Text analysis

### Batch Stack
- Fargate compute environment
- GPU compute environment (g4dn.xlarge)
- Job queues and definitions
- IAM roles and policies

### Serverless Stack
- Lambda functions for:
  - Metadata processing
  - Transcript processing
  - Text analysis
- S3 event triggers
