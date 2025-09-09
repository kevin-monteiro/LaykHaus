FROM python:3.11-slim

WORKDIR /app

# Install FastAPI and dependencies
RUN pip install --no-cache-dir \
    fastapi==0.104.1 \
    uvicorn==0.24.0 \
    pydantic==2.5.0

# Copy API server
COPY api_server.py /app/

EXPOSE 8080

CMD ["uvicorn", "api_server:app", "--host", "0.0.0.0", "--port", "8080"]