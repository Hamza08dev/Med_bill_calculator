from __future__ import annotations
from typing import Dict
import json, requests

def llm_complete(cfg: Dict, prompt: str) -> str:
    if not cfg or not cfg.get("enabled"): 
        return ""
    if cfg.get("provider") == "ollama":
        payload = {"model": cfg["model"], "prompt": prompt, "options": {"temperature": cfg.get("temperature", 0.0)}}
        r = requests.post(cfg["endpoint"], json=payload, timeout=60)
        r.raise_for_status()
        out = []
        for line in r.text.splitlines():
            if not line.strip(): continue
            obj = json.loads(line)
            if "response" in obj:
                out.append(obj["response"])
        return "".join(out)
    raise RuntimeError(f"Unsupported provider: {cfg.get('provider')}")
