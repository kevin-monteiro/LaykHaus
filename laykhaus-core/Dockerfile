FROM python:3.11-slim

# Install system dependencies including Java for PySpark
RUN apt-get update && apt-get install -y \
    gcc \
    g++ \
    postgresql-client \
    curl \
    openjdk-21-jre-headless \
    && rm -rf /var/lib/apt/lists/*

# Set working directory
WORKDIR /app

# Install Python dependencies directly with pip
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn[standard]==0.24.0 \
    strawberry-graphql[fastapi]==0.211.0 \
    sqlalchemy==2.0.23 \
    asyncpg==0.29.0 \
    redis==5.0.1 \
    pydantic==2.5.0 \
    pydantic-settings==2.0.3 \
    prometheus-client==0.19.0 \
    httpx==0.25.2 \
    python-dotenv==1.0.0 \
    aiokafka==0.10.0 \
    structlog==23.2.0 \
    pytest==7.4.3 \
    pytest-asyncio==0.21.1 \
    faker==20.1.0 \
    sqlparse==0.4.4 \
    numpy==1.26.2 \
    pandas==2.1.3 \
    pyarrow==14.0.1 \
    scikit-learn==1.3.2 \
    pyspark==3.5.0

# Copy application code
COPY ./src ./src

# Set Python path
ENV PYTHONPATH=/app/src

# Expose ports
EXPOSE 8000

# Run application
CMD ["uvicorn", "laykhaus.main:app", "--host", "0.0.0.0", "--port", "8000", "--reload"]