#!/usr/bin/env bash
set -euo pipefail

APP_NAME="redis-sql-demo"
APP_DIR="/home/ubuntu/${APP_NAME}"
PUBLIC_DIR="${APP_DIR}/public"
PY_DIR="${APP_DIR}/app"            # adjust to your code location
PY_ENTRY="app.main:app"            # e.g., FastAPI entrypoint "module:app"
PY_PORT=8000
SITE_IP="$(curl -s http://169.254.169.254/latest/meta-data/public-ipv4 || echo '0.0.0.0')" || true

echo "==> Updating packages"
sudo apt-get update -y
sudo apt-get install -y \
  apache2 libapache2-mod-proxy-html \
  python3-venv python3-pip \
  postgresql-client \
  redis-tools \
  jq curl ufw

echo "==> Ensure app directories exist"
sudo mkdir -p "${PUBLIC_DIR}"
sudo mkdir -p "${PY_DIR}"
sudo chown -R ubuntu:ubuntu "${APP_DIR}"

# --- If you already rsynced your project, skip copying.
# # Example seed (optional):
# echo "<h1>It works</h1>" > "${PUBLIC_DIR}/index.html"
# cat > "${PY_DIR}/main.py" <<'PY'
# from fastapi import FastAPI
# app = FastAPI()
# @app.get("/api/redis/redis-stats")
# def stats():
#     return {"ok": True}
# PY

echo "==> Create/refresh virtualenv and install app"
python3 -m venv "${APP_DIR}/venv"
source "${APP_DIR}/venv/bin/activate"
pip install --upgrade pip wheel
# pip install -r ${APP_DIR}/requirements.txt  # uncomment if you have it
# Example minimal deps if FastAPI/Uvicorn:
# pip install "fastapi[standard]" "uvicorn[standard]" psycopg2-binary

echo "==> Create systemd service"
SERVICE_FILE="/etc/systemd/system/${APP_NAME}.service"
sudo tee "${SERVICE_FILE}" >/dev/null <<SERVICE
[Unit]
Description=${APP_NAME} (Uvicorn)
After=network.target

[Service]
User=ubuntu
Group=ubuntu
WorkingDirectory=${APP_DIR}
Environment="PYTHONUNBUFFERED=1"
# Add your DB env here or in an EnvironmentFile=
# Environment="DATABASE_URL=postgresql://user:pass@host:5432/dbname"
ExecStart=${APP_DIR}/venv/bin/uvicorn ${PY_ENTRY} --host 127.0.0.1 --port ${PY_PORT} --proxy-headers --workers 2
Restart=always
RestartSec=3

[Install]
WantedBy=multi-user.target
SERVICE

echo "==> Enable + start app service"
sudo systemctl daemon-reload
sudo systemctl enable --now "${APP_NAME}.service"
sudo systemctl status "${APP_NAME}.service" --no-pager -l || true

echo "==> Apache vhost"
VHOST="/etc/apache2/sites-available/000-default.conf"
sudo tee "${VHOST}" >/dev/null <<APACHE
<VirtualHost *:80>
    ServerAdmin admin@localhost
    ServerName ${SITE_IP}
    DocumentRoot ${PUBLIC_DIR}

    <Directory ${PUBLIC_DIR}>
        Options Indexes FollowSymLinks
        AllowOverride All
        Require all granted
    </Directory>

    ProxyPreserveHost On
    RequestHeader set X-Forwarded-Proto "http"
    RequestHeader set X-Forwarded-Port "80"

    ProxyPass        /api/ http://127.0.0.1:${PY_PORT}/api/ retry=0 timeout=30
    ProxyPassReverse /api/ http://127.0.0.1:${PY_PORT}/api/

    <Location /api>
        Header always set Access-Control-Allow-Origin "*"
        Header always set Access-Control-Allow-Headers "Content-Type, Authorization"
        Header always set Access-Control-Allow-Methods "GET,POST,PUT,PATCH,DELETE,OPTIONS"
    </Location>

    ErrorLog \${APACHE_LOG_DIR}/error.log
    CustomLog \${APACHE_LOG_DIR}/access.log combined
</VirtualHost>
APACHE

echo "==> Enable Apache modules + reload"
sudo a2enmod proxy proxy_http headers
sudo apache2ctl configtest
sudo systemctl reload apache2

echo "==> Fix permissions so Apache can read"
sudo chmod 755 /home /home/ubuntu "${APP_DIR}" "${PUBLIC_DIR}"
sudo find "${PUBLIC_DIR}" -type d -exec sudo chmod 755 {} \;
sudo find "${PUBLIC_DIR}" -type f -exec sudo chmod 644 {} \;

# --- OPTIONAL: firewall (Ubuntu UFW)
# sudo ufw allow OpenSSH
# sudo ufw allow 80/tcp
# sudo ufw --force enable

# --- OPTIONAL: run DB migrations if your app needs them
# source "${APP_DIR}/venv/bin/activate"
# export DATABASE_URL="postgresql://user:pass@host:5432/db"
# alembic upgrade head   # or: python manage.py migrate --noinput

echo "==> Smoke tests"
curl -sI http://127.0.0.1/    | head -n1 || true
curl -sI http://${SITE_IP}/   | head -n1 || true
curl -sI http://127.0.0.1/api/ | head -n1 || true

echo "==> Done"