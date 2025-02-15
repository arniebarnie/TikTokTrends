#!/bin/bash

# Check if Python 3.9+ is installed
if ! command -v python3 &> /dev/null; then
    echo "Python 3.9+ is required but not installed. Please install it first."
    exit 1
fi

# Create and activate Python virtual environment
echo "Creating Python virtual environment..."
python3 -m venv .venv
source .venv/bin/activate

# Install Python dependencies
echo "Installing Python dependencies..."
pip install -r requirements.txt

# Install and use Node.js 20
echo "Installing Node.js 20..."
nvm install 20
nvm use 20

# Install AWS CDK globally
echo "Installing AWS CDK CLI..."
npm install -g aws-cdk