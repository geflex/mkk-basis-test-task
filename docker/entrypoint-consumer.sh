#!/usr/bin/env sh
set -e

echo "Запускаю consumer..."
exec faststream run app.workers.payment_consumer:app