FROM python:3.11-slim as builder

# Install git
RUN apt-get update && apt-get install -y git

# need to build psycopg2
RUN apt-get -y install libpq-dev gcc

# Clone the repo
RUN mkdir /app && cd /app && git clone --branch develop https://github.com/scales-okn/satyrn-api.git

# Install Python requirements
WORKDIR /app/satyrn-api
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# ------ Runner Stage ------
FROM python:3.11-slim

# Copy necessary files from builder stage
COPY --from=builder /usr/local /usr/local
COPY --from=builder /app/satyrn-api /app/satyrn-api

# Set default command, if necessary
# CMD ["gunicorn", "..."]
