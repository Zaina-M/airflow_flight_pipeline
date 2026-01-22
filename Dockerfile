FROM apache/airflow:2.8.0-python3.11

USER root

# Install system dependencies for MySQL client
RUN apt-get update && apt-get install -y \
    default-libmysqlclient-dev \
    build-essential \
    pkg-config \
    curl \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

USER airflow

# Set the PATH to ensure airflow command is found
ENV PATH="/home/airflow/.local/bin:${PATH}"

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip install --user --no-cache-dir -r /tmp/requirements.txt
