FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir \
    kafka-python==2.0.2 \
    psycopg2-binary==2.9.9 \
    requests==2.31.0 \
    faker==20.1.0

# Copy generator script
COPY generator.py /app/

CMD ["python", "-u", "generator.py"]