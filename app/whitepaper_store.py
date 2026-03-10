from __future__ import annotations

import re
from pathlib import Path
from typing import Dict


APP_DIR = Path(__file__).resolve().parent
WHITEPAPER_DIR = APP_DIR / "data" / "whitepapers"


def _safe_store_id(store_id: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9_-]", "_", str(store_id).strip())
    return cleaned or "store"


def get_whitepaper_path(store_id: str) -> Path:
    safe = _safe_store_id(store_id)
    return WHITEPAPER_DIR / f"{safe}.md"


def save_whitepaper(store_id: str, content: str) -> Path:
    WHITEPAPER_DIR.mkdir(parents=True, exist_ok=True)
    path = get_whitepaper_path(store_id)
    normalized = (content or "").replace("\r\n", "\n").replace("\r", "\n").strip()
    path.write_text(normalized, encoding="utf-8")
    return path


def load_whitepaper(store_id: str) -> str | None:
    path = get_whitepaper_path(store_id)
    if not path.exists():
        return None
    content = path.read_text(encoding="utf-8")
    return content


def whitepaper_info(store_id: str) -> Dict[str, object]:
    content = load_whitepaper(store_id)
    if content is None:
        return {
            "store_id": store_id,
            "exists": False,
            "char_count": 0,
            "line_count": 0,
            "content": "",
        }

    return {
        "store_id": store_id,
        "exists": True,
        "char_count": len(content),
        "line_count": content.count("\n") + 1,
        "content": content,
    }
