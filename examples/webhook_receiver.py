"""Tiny webhook receiver for local testing.

Run it on the host (or in the same network as the containers)::

    python examples/webhook_receiver.py --port 9000

Then pass ``--webhook-url http://<reachable-host>:9000/webhook`` when creating
a payment. Every notification is printed as pretty JSON.
"""

from __future__ import annotations

import argparse
import json
from http.server import BaseHTTPRequestHandler, HTTPServer


class Handler(BaseHTTPRequestHandler):
    def do_POST(self) -> None:  # noqa: N802 - stdlib naming
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length)
        try:
            payload = json.loads(raw or b"{}")
        except json.JSONDecodeError:
            payload = {"raw": raw.decode("utf-8", "replace")}

        print("Webhook received:")
        print(json.dumps(payload, indent=2, ensure_ascii=False))

        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.end_headers()
        self.wfile.write(b'{"ok": true}')

    def log_message(self, *args) -> None:  # silence default logging
        return


def main() -> None:
    parser = argparse.ArgumentParser(description="Webhook receiver")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=9000)
    args = parser.parse_args()

    server = HTTPServer((args.host, args.port), Handler)
    print(f"Listening for webhooks on http://{args.host}:{args.port}/webhook")
    server.serve_forever()


if __name__ == "__main__":
    main()
