# Use official Python 3.12 image
FROM python:3.12-slim

# Set environment variables
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    POETRY_VIRTUALENVS_CREATE=false \
    POETRY_NO_INTERACTION=1

# Set workdir
WORKDIR /app

# Install system dependencies and Poetry
RUN apt-get update && apt-get install -y curl build-essential && \
    pip install --upgrade pip && \
    pip install poetry && \
    apt-get clean

# Copy only the dependency files
COPY pyproject.toml poetry.lock* /app/

# Install dependencies
RUN poetry install --no-root

# Copy the rest of the application
COPY . .

# Expose FastAPI port
EXPOSE 8001

# Command to run the app
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8001"]
