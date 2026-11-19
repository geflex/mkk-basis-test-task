#!/usr/bin/env sh
set -e

echo "Запускаю consumer..."
exec faststream run app.consumer:app
