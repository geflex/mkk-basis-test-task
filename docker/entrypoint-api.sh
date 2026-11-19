#!/usr/bin/env sh
set -e

echo "Запускаю API..."
exec uvicorn app.api.app:app --host 0.0.0.0 --port 8000 --proxy-headers