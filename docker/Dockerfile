FROM python:3.11-slim

WORKDIR /app

# Install uv for fast package management
RUN pip install uv

# Copy requirements first (layer caching)
COPY requirements.txt .

# Install dependencies using uv
RUN uv pip install --system -r requirements.txt

# Copy project files
COPY . .

# Default command
CMD ["python", "-m", "extract.generator"]