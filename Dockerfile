FROM python:3.12-slim

# Set working directory
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    libgl1 \
    libglib2.0-0 \
    libsm6 \
    libxext6 \
    libxrender-dev \
 && rm -rf /var/lib/apt/lists/*

# Upgrade pip first
RUN pip install --upgrade pip

# Install uv
RUN pip install uv

# Copy requirements.txt
COPY requirements.txt /app/

# Install dependencies using uv
RUN uv pip install --system -r requirements.txt

# Copy the rest of the application
COPY . /app/

# Run your main script
CMD ["python", "main.py"]
