#!/usr/bin/env python3
import os
import subprocess
import sys

port = os.environ.get("PORT", "8000")

cmd = [
    sys.executable, "-m", "uvicorn",
    "main:app",
    "--host", "0.0.0.0",
    "--port", port,
]

print(f"Starting on port {port}")
subprocess.run(cmd)
