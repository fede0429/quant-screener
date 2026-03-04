FROM python:3.12-slim

# Install nginx + supervisor
RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx supervisor curl && \
    rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Python dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy backend
COPY api/ /app/api/

# Copy frontend to nginx
COPY index.html /var/www/html/
COPY css/ /var/www/html/css/
COPY js/ /var/www/html/js/
COPY data/ /var/www/html/data/

# Nginx config
COPY deploy/nginx.conf /etc/nginx/sites-available/default

# Supervisor config
COPY deploy/supervisord.conf /etc/supervisor/conf.d/quant.conf

# Create data directory for SQLite
RUN mkdir -p /app/data

EXPOSE 8002

CMD ["/usr/bin/supervisord", "-n", "-c", "/etc/supervisor/conf.d/quant.conf"]
