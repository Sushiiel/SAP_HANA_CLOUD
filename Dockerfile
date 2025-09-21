FROM python:3.11-slim

# System deps needed for some Python packages and SSL
RUN apt-get update && apt-get install -y build-essential curl ca-certificates libssl-dev \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy requirements first (speeds up rebuilds)
COPY requirements.txt /app/requirements.txt
RUN pip install --no-cache-dir -r /app/requirements.txt

COPY . /app

EXPOSE 8501

CMD ["streamlit", "run", "st_app.py", "--server.port=8501", "--server.address=0.0.0.0"]
