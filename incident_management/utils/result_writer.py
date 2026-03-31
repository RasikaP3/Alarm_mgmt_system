"""
utils/result_writer.py
Centralised JSONL writer → all output goes to  <project_root>/result/
"""
import json
import os
from pathlib import Path

# project_root/result/
RESULT_DIR = Path(__file__).resolve().parent.parent / "result"


def write_result(filename: str, data: dict) -> Path:
    """Append one JSON line to result/<filename>. Returns the file path."""
    RESULT_DIR.mkdir(parents=True, exist_ok=True)
    out = RESULT_DIR / filename
    with open(out, "a", encoding="utf-8") as f:
        f.write(json.dumps(data, default=str) + "\n")
    return out