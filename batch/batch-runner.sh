#!/usr/bin/env bash
set -euo pipefail

# career-ops batch runner — thin wrapper around batch-orchestrator.py
# For Hermes Agent (previously Claude Code)

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"

# Pass all arguments through to the Python orchestrator
cd "$PROJECT_DIR"
python3 "$SCRIPT_DIR/batch-orchestrator.py" "$@"
