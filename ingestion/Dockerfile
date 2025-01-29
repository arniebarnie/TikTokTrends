# Use NVIDIA CUDA base image
FROM pytorch/pytorch:2.1.0-cuda12.1-cudnn8-runtime

# Set environment variables
ENV DEBIAN_FRONTEND=noninteractive
ENV PYTHONUNBUFFERED=1

# Install system dependencies and NVIDIA repository
RUN apt-get update && apt-get install -y \
    ffmpeg \
    git \
    wget \
    gnupg \
    && wget https://developer.download.nvidia.com/compute/cuda/repos/ubuntu2004/x86_64/cuda-keyring_1.0-1_all.deb \
    && dpkg -i cuda-keyring_1.0-1_all.deb \
    && apt-get update \
    && apt-get install -y libcudnn8 \
    && rm -rf /var/lib/apt/lists/* \
    && rm cuda-keyring_1.0-1_all.deb

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Create workspace directory
RUN mkdir -p /workspace
WORKDIR /app

# Copy application code
COPY . .

# Enable TensorFloat-32 for better performance
RUN echo 'import torch; \
    torch.backends.cuda.matmul.allow_tf32 = True; \
    torch.backends.cudnn.allow_tf32 = True' > /root/.pytorch_startup.py
ENV PYTHONSTARTUP=/root/.pytorch_startup.py

# Set the entrypoint
ENTRYPOINT ["python", "main.py"] 