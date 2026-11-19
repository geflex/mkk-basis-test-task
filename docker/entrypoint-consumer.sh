#!/usr/bin/env sh
set -e

echo "Starting consumer..."
exec faststream run app.consumer:app
