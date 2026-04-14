# src/ingest/md_table_extractor.py
from __future__ import annotations
from dataclasses import dataclass
from pathlib import Path
from typing import List, Dict, Any, Optional
import re

_TABLE_SEP_RE = re.compile(r"^\s*\|?\s*(:?-{3,}:?\s*\|)+\s*:?-{3,}:?\s*\|?\s*$")

@dataclass
class ExtractedTable:
    idx: int
    headers: List[str]
    rows: List[List[str]]
    start_line: int
    end_line: int
    heading_path: List[str]       # up to last 3 headings
    section_hint: Optional[str]   # e.g., "radiology"
    provider_hint: Optional[str]  # e.g., "chiropractic"
    region_hint: Optional[str]    # "I"/"II"/"III"/"IV" for ZIP tables

def _split_md_row(line: str) -> List[str]:
    token = "¶PIPE¶"
    line = line.replace(r"\|", token)
    core = line.strip()
    if "|" not in core:
        return []
    if core.startswith("|"):
        core = core[1:]
    if core.endswith("|"):
        core = core[:-1]
    parts = [c.strip().replace(token, "|") for c in core.split("|")]
    return parts

def _looks_heading(line: str) -> bool:
    return line.strip().startswith("#")

def _roman_region(h: str) -> Optional[str]:
    m = re.search(r"\bRegion\s+(I{1,3}|IV)\b", h, flags=re.I)
    return m.group(1).upper() if m else None

def _norm(s: str) -> str:
    return re.sub(r"\s+", " ", s.strip().lower())

def _classify_section_provider(heading_path: List[str], sections_cfg: dict, providers_cfg: dict):
    hp = [_norm(h) for h in heading_path]
    joined = " | ".join(hp)

    # section
    sec_hint = None
    for sec, payload in sections_cfg.get("sections", {}).items():
        for ali in payload.get("aliases", []):
            if ali in joined:
                sec_hint = sec
                break
        if sec_hint:
            break

    # provider
    prov_hint = None
    for p, payload in providers_cfg.get("providers", {}).items():
        for ali in payload.get("aliases", []):
            if ali in joined:
                prov_hint = p
                break
        if prov_hint:
            break

    return sec_hint, prov_hint

def extract_md_tables(md_path: Path, sections_cfg: dict, providers_cfg: dict) -> List[ExtractedTable]:
    lines = md_path.read_text(encoding="utf-8", errors="ignore").splitlines()
    tables: List[ExtractedTable] = []
    heading_path: List[str] = []
    current_region: Optional[str] = None

    i = 0
    tcount = 0
    while i < len(lines):
        ln = lines[i]

        if _looks_heading(ln):
            title = ln.lstrip("#").strip()
            heading_path.append(title)
            heading_path = heading_path[-4:]  # keep last few
            # track region hint
            r = _roman_region(title)
            if r:
                current_region = r
            i += 1
            continue

        if ("|" in ln) and (i + 1 < len(lines)) and _TABLE_SEP_RE.match(lines[i + 1]):
            header = _split_md_row(ln)
            if not header:
                i += 1
                continue
            # collect rows
            j = i + 2
            rows: List[List[str]] = []
            while j < len(lines):
                row_line = lines[j]
                if not row_line.strip():
                    break
                if "|" not in row_line:
                    break
                if _TABLE_SEP_RE.match(row_line):
                    j += 1
                    continue
                row = _split_md_row(row_line)
                if not row:
                    break
                # normalize length
                if len(row) < len(header):
                    row += [""] * (len(header) - len(row))
                elif len(row) > len(header):
                    row = row[:len(header)]
                rows.append(row)
                j += 1

            section_hint, provider_hint = _classify_section_provider(heading_path[-3:], sections_cfg, providers_cfg)
            tables.append(
                ExtractedTable(
                    idx=tcount,
                    headers=header,
                    rows=rows,
                    start_line=i + 1,
                    end_line=j,
                    heading_path=heading_path[-3:],
                    section_hint=section_hint,
                    provider_hint=provider_hint,
                    region_hint=current_region,
                )
            )
            tcount += 1
            i = j
            continue

        i += 1

    return tables
