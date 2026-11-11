#!/usr/bin/env python3
"""Root launcher for the Edge Anomaly Stream Filter interactive web application.

Usage:
    python app.py                              # Start web UI and open it in the browser
    python app.py --no-open                    # Start web UI without auto-opening browser
    python app.py --port 8765                  # Bind to a specific port
    python app.py --project-dir PATH           # Explicit repo path when launched elsewhere
    python app.py --export-standalone FILE.py  # Build a single self-contained .py app
    python app.py --dump-project               # Print full project structure and file contents
"""

from __future__ import annotations

import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from src.app.server import main


if __name__ == "__main__":
    raise SystemExit(main())
