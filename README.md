# TikTok Analytics Platform

A comprehensive analytics platform for TikTok content that processes videos, extracts insights, and provides a dashboard interface for analysis.

## Project Structure 

## Components

### Analytics Stack
- **AWS Batch Environment**: GPU-enabled compute environment for video processing
  - Uses g4dn.xlarge instances for GPU acceleration
  - Spot instances for cost optimization
  - Auto-scaling from 0 to 8 vCPUs
- **ECR Repository**: Hosts Docker images for video processing
- **VPC Configuration**: Network setup with public and private subnets
- **IAM Roles**: Batch service and instance roles with required permissions

### Dashboard Stack
- **MariaDB Database**: Stores processed video data and analytics
  - Runs on EC2 t3.micro instance
  - MariaDB 10.5 for JSON support
  - Automated setup via user data script
- **Lambda Functions**: API endpoints for frontend queries
  - Profile analytics
  - Category analysis
  - Language statistics
- **Glue Catalog**: Data catalog for processed video data
- **EC2 Instance**: Hosts MariaDB database

### Frontend
- **Next.js Application**: React-based dashboard interface
- **Material UI**: Component library for UI elements
- **Recharts**: Data visualization library
- **Components**:
  - ProfileAnalytics: Profile-level statistics
  - CategoryAnalytics: Content category analysis
  - Navigation: Dashboard navigation
  - Header & Footer: Layout components

## Prerequisites
- Node.js 18+
- Python 3.9+
- AWS CLI configured with appropriate credentials
- Docker installed and configured
- AWS Account with permissions for:
  - EC2, 
  - Lambda
  - VPC
  - AWS Batch
  - ECR
  - Secrets Manager
  - SSM
  - Glue
  - IAM