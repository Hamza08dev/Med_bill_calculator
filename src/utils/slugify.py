import re

def slugify(text: str, allow_dot: bool = False) -> str:
    text = text.strip().lower()
    text = re.sub(r"[^\w\s\-.]" if allow_dot else r"[^\w\s-]", "", text)
    text = re.sub(r"\s+", "-", text)
    text = re.sub(r"-{2,}", "-", text)
    return text.strip("-") or "table"
