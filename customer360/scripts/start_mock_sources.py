#!/usr/bin/env python3
"""
start_mock_sources.py
=====================
Launches the three mock API servers locally:
  - OMS API on port 8001 (http://127.0.0.1:8001)
  - PIM API on port 8002 (http://127.0.0.1:8002)
  - Loyalty API on port 8003 (http://127.0.0.1:8003)

Usage:
  python scripts/start_mock_sources.py
"""
import sys
import threading
import time
from pathlib import Path

# Ensure project root is on sys.path
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import uvicorn
from mock_sources.oms.main import app as oms_app
from mock_sources.pim.main import app as pim_app
from mock_sources.loyalty.main import app as loyalty_app


def run_server(app, port: int, name: str):
    config = uvicorn.Config(app=app, host="127.0.0.1", port=port, log_level="warning")
    server = uvicorn.Server(config)
    print(f"[{name}] Listening on http://127.0.0.1:{port}")
    server.run()


def main():
    servers = [
        (oms_app, 8001, "Mock-OMS"),
        (pim_app, 8002, "Mock-PIM"),
        (loyalty_app, 8003, "Mock-Loyalty"),
    ]
    threads = []
    print("=" * 60)
    print("Starting Customer 360 Mock Source API Servers...")
    print("  - OMS:     http://127.0.0.1:8001/api/v1/orders")
    print("  - PIM:     http://127.0.0.1:8002/api/v1/products")
    print("  - Loyalty: http://127.0.0.1:8003/api/v1/loyalty")
    print("=" * 60)

    for app, port, name in servers:
        t = threading.Thread(target=run_server, args=(app, port, name), daemon=True)
        t.start()
        threads.append(t)

    print("All mock source servers running. Press Ctrl+C to terminate.\n")
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down mock source servers...")


if __name__ == "__main__":
    main()
