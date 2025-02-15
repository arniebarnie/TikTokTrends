# Build stage
FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime AS builder

# Install build dependencies
RUN apt-get update && apt-get install -y \
    ffmpeg \
    && rm -rf /var/lib/apt/lists/*

# Install CUDA/CUDNN dependencies
RUN pip install --no-cache-dir \
    nvidia-cublas-cu11 \
    nvidia-cudnn-cu11==8.9.6.50

# Setup workspace
WORKDIR /app

# Install Python packages
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Runtime stage
FROM pytorch/pytorch:2.1.0-cuda11.8-cudnn8-runtime

# Copy only necessary files from builder
COPY --from=builder /opt/conda/lib/python3.10/site-packages /opt/conda/lib/python3.10/site-packages
COPY --from=builder /usr/bin/ffmpeg /usr/bin/ffmpeg
COPY --from=builder /usr/lib/x86_64-linux-gnu/libav* /usr/lib/x86_64-linux-gnu/
COPY --from=builder /usr/lib/x86_64-linux-gnu/libpostproc* /usr/lib/x86_64-linux-gnu/

WORKDIR /app
RUN mkdir -p /workspace

# Copy application code
COPY . .

# Set CUDA library path
ENV LD_LIBRARY_PATH="/opt/conda/lib/python3.10/site-packages/nvidia/cudnn/lib:/opt/conda/lib/python3.10/site-packages/nvidia/cublas/lib:${LD_LIBRARY_PATH}"

# Enable TensorFloat-32
RUN echo 'import torch; \
    torch.backends.cuda.matmul.allow_tf32 = True; \
    torch.backends.cudnn.allow_tf32 = True' > /root/.pytorch_startup.py
ENV PYTHONSTARTUP=/root/.pytorch_startup.py

ENTRYPOINT ["python", "main.py"] 