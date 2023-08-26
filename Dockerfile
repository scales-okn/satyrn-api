# ------ Builder Stage ------
FROM python:3.11-slim as builder

# Install dependencies to build psycopg2
RUN apt-get update && \
    apt-get install -y libpq-dev gcc && \
    rm -rf /var/lib/apt/lists/*

RUN mkdir -p /app/satyrn-api
WORKDIR /app/satyrn-api
COPY . .
RUN pip install --no-cache-dir -r requirements.txt gunicorn

# ------ Runner Stage ------
FROM python:3.11-slim

WORKDIR /app/satyrn-api

# Copy necessary files from builder stage
RUN apt-get update && apt-get install libpq5 -y
COPY --from=builder /usr/local /usr/local
COPY --from=builder /app/satyrn-api /app/satyrn-api

# Expose port 5000
EXPOSE 5000

CMD ["gunicorn", "wsgi:app", "--bind", "0.0.0.0:5000", "--workers", "3", "--access-logfile", "-", "--error-logfile", "-", "--log-level", "info"]

