"""Tiny webhook receiver for local testing.

Run it on the host (or in the same network as the containers)::

    python examples/webhook_receiver.py --port 9000

Then pass ``--webhook-url http://<reachable-host>:9000/webhook`` when creating
a payment. Every notification is printed as pretty JSON.

Pass ``--secret`` to also verify the ``X-Webhook-Signature`` HMAC; it must
match ``WEBHOOK_SECRET`` configured for the service.
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
                self._respond(401, {"error": "invalid signature"})
                return

        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            payload = {"raw": raw.decode("utf-8", "replace")}

        print("Webhook received:")
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

    def log_message(self, *args) -> None:  # silence default logging
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Webhook receiver")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    parser.add_argument(
        "--secret",
        default=None,
        help="Verify the X-Webhook-Signature HMAC using this shared secret",
    )
    args = parser.parse_args()

    Handler.secret = args.secret
    server = HTTPServer((args.host, args.port), Handler)
    print(f"Listening for webhooks on http://{args.host}:{args.port}/webhook")
    server.serve_forever()


if __name__ == "__main__":
    main()
