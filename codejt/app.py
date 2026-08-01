#!/usr/bin/env python3
"""
CodeJT
A minimal Python web application prototype for Combs Contracting LLC.
Copyright 2026 Jonathan Combs, Combs Contracting LLC.
"""

from http.server import HTTPServer, BaseHTTPRequestHandler
from datetime import datetime


class CodeJTHandler(BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-Type", "text/plain; charset=utf-8")
        self.end_headers()
        self.wfile.write(
            (
                "CodeJT by Combs Contracting LLC\n"
                "This is a minimal prototype running in Python.\n"
                f"UTC time: {datetime.utcnow().isoformat()}\n"
            ).encode("utf-8")
        )

    def log_message(self, format, *args):
        return


def run(host="0.0.0.0", port=8080):
    server = HTTPServer((host, port), CodeJTHandler)
    print(f"CodeJT is listening on http://{host}:{port}")
    server.serve_forever()


if __name__ == "__main__":
    run()
