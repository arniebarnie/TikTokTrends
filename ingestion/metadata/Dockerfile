FROM python:3.10-slim

WORKDIR /app
RUN mkdir -p /workspace

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

ENTRYPOINT ["python", "main.py"]