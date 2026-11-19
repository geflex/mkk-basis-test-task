"""Маленький приёмник webhook для локального тестирования.

Запустите его на хосте (или в той же сети, что и контейнеры)::

    python examples/webhook_receiver.py --port 9000

Затем передайте ``--webhook-url http://<доступный-хост>:9000/webhook`` при
создании платежа. Каждое уведомление печатается как отформатированный JSON.

Передайте ``--secret``, чтобы также проверять HMAC ``X-Webhook-Signature``; он
должен совпадать с ``WEBHOOK_SECRET``, настроенным для сервиса.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


def _sign(body: bytes, secret: str) -> str:
    digest = hmac.new(secret.encode("utf-8"), body, hashlib.sha256).hexdigest()
    return f"sha256={digest}"


class Handler(BaseHTTPRequestHandler):
    secret: str | None = None

    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)

        if self.secret:
            provided = self.headers.get("X-Webhook-Signature", "")
            if not hmac.compare_digest(_sign(raw, self.secret), provided):
                self._respond(401, {"error": "неверная подпись"})
                return

        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            payload = {"raw": raw.decode("utf-8", "replace")}

        print("Webhook получен:")
        print("  X-Webhook-Id:", self.headers.get("X-Webhook-Id"))
        print(json.dumps(payload, indent=2, ensure_ascii=False))

        self._respond(200, {"ok": True})

    def _respond(self, status: int, body: dict) -> None:
        data = json.dumps(body).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    def log_message(self, *args) -> None:  # отключаем логирование по умолчанию
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Приёмник webhook")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--secret",
        default=None,
        help="Проверять HMAC X-Webhook-Signature с помощью этого общего секрета",
    )
    args = parser.parse_args()

    Handler.secret = args.secret
    server = HTTPServer((args.host, args.port), Handler)
    print(f"Слушаю webhook'и на http://{args.host}:{args.port}/webhook")
    server.serve_forever()


if __name__ == "__main__":
    main()
