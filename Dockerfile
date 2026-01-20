FROM python:3.11-slim

WORKDIR /app

# Install system dependencies for psycopg2 (if using binary, less needed, but good practice)
# RUN apt-get update && apt-get install -y libpq-dev gcc

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY src/ src/
# Create a valid package if needed, or just set pythonpath
ENV PYTHONPATH=/app

# User
RUN useradd -m appuser && chown -R appuser /app
USER appuser

EXPOSE 8000

CMD ["uvicorn", "src.aipe.main:app", "--host", "0.0.0.0", "--port", "8000"]
