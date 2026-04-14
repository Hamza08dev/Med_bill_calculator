# src/ingest/md_to_entities.py
from __future__ import annotations
import argparse, json, re, sys
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd

# ----------------- basic utilities -----------------

PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

PIPE_ROW  = re.compile(r"^\s*\|")  # markdown table row
SEP_ROW   = re.compile(r"^\s*\|?\s*:?-{3,}:?\s*(\|\s*:?-{3,}:?\s*)+\|?\s*$", re.I)
MONEY     = re.compile(r"[\$,]")
NUMERIC   = re.compile(r"^-?\d+(?:\.\d+)?$")
CODE_RE   = re.compile(r"^[A-Z]?\d{4,5}[A-Z]?T?$")  # CPT-ish, incl. T codes
REGION_HDRS = [f"region {r}" for r in ("i","ii","iii","iv")]
DATE_HINT = re.compile(r"(on|effective)\s+(or\s+after\s+)?([A-Z][a-z]+)\s+(\d{1,2}),\s*(\d{4})", re.I)

def _norm_text(s: Optional[str]) -> str:
    return (s or "").strip()

def _to_float(s: Optional[str]) -> Optional[float]:
    if s is None: return None
    x = MONEY.sub("", str(s)).strip()
    return float(x) if NUMERIC.match(x) else None

def _slug(s: str) -> str:
    t = re.sub(r"[^a-z0-9\- ]+", "", s.lower())
    t = re.sub(r"\s+", "-", t).strip("-")
    return t[:80]

def _norm_region_name(s: Optional[str]) -> Optional[str]:
    """
    Robustly pull the roman/arabic numeral AFTER an optional 'region' token.
    Fixes the previous bug that matched the 'i' in 'RegIon'.
    """
    if not s: return None
    s_l = s.strip().lower()
    m = re.search(r"\b(?:region\s+)?(i{1,3}|iv|v|vi|\d{1,2})\b", s_l)
    if not m:
        return s.strip().title()
    token = m.group(1)
    roman = {"i":"I","ii":"II","iii":"III","iv":"IV","v":"V","vi":"VI"}
    if token in roman:
        return f"Region {roman[token]}"
    else:
        return f"Region {token}"

def _ctx_effective_date(lines: List[str]) -> Optional[str]:
    ctx = " ".join([l for l in lines[-5:] if l.strip()])
    m = DATE_HINT.search(ctx)
    if not m: return None
    month, day, year = m.group(3), m.group(4), m.group(5)
    try:
        import datetime as dt
        return dt.datetime.strptime(f"{month} {day}, {year}", "%B %d, %Y").date().isoformat()
    except Exception:
        return None

# ----------------- heading / anchors -----------------

@dataclass
class Anchor:
    anchor_id: str
    level: int
    title: str
    line_no: int

def parse_anchors(md_lines: List[str]) -> List[Anchor]:
    out: List[Anchor] = []
    for i, line in enumerate(md_lines, start=1):
        m = re.match(r"^(#{1,6})\s+(.*)$", line.strip())
        if not m: continue
        level = len(m.group(1)); title = m.group(2).strip()
        out.append(Anchor(anchor_id=f"h{level}-{_slug(title)}-{i}", level=level, title=title, line_no=i))
    return out

def nearest_anchor(anchors: List[Anchor], line_no: int) -> Optional[Anchor]:
    prev = [a for a in anchors if a.line_no <= line_no]
    return prev[-1] if prev else None

# ----------------- raw table capture -----------------

@dataclass
class MDTable:
    table_index: int
    start_line: int
    end_line: int
    header_line_raw: str
    sep_line_raw: str
    row_lines_raw: List[str]
    header_cells_raw: List[str]
    header_cells_trim: List[str]
    rows_cells_raw: List[List[str]]
    rows_cells_trim: List[List[str]]
    raw_block: str
    anchor_id: Optional[str]
    anchor_title: Optional[str]
    region_context: Optional[str]
    effective_date_hint: Optional[str]
    title_hint: Optional[str]
    mapped_as: Optional[str]  # set after mapping (e.g., 'conversion_factors')

def _split_md_cells_raw(line: str) -> List[str]:
    inner = line.rstrip("\n")
    # remove a single leading/trailing pipe if present
    if inner.startswith("|"): inner = inner[1:]
    if inner.endswith("|"): inner = inner[:-1]
    return inner.split("|")

def _split_md_cells_trim(line: str) -> List[str]:
    return [c.strip() for c in _split_md_cells_raw(line)]

def iter_md_tables_raw(md_lines: List[str]) -> List[MDTable]:
    """
    Collect *all* MD tables with raw + trimmed cells and useful context.
    """
    anchors = parse_anchors(md_lines)
    out : List[MDTable] = []
    i = 0
    recent_nonempty: List[str] = []

    # Map of nearest Region heading above any line
    region_by_line: Dict[int, str] = {}
    for a in anchors:
        if re.match(r"^region\s+", a.title.strip(), re.I):
            region_by_line[a.line_no] = a.title

    def _ctx_region(line_no: int) -> Optional[str]:
        keys = [ln for ln in region_by_line.keys() if ln <= line_no]
        if not keys: return None
        return region_by_line[max(keys)]

    while i < len(md_lines)-1:
        if PIPE_ROW.match(md_lines[i]) and SEP_ROW.match(md_lines[i+1]):
            start = i + 1
            header_line = md_lines[i]
            sep_line    = md_lines[i+1]
            i += 2
            row_lines   = []
            while i < len(md_lines) and PIPE_ROW.match(md_lines[i]):
                row_lines.append(md_lines[i])
                i += 1
            end_line = i

            header_cells_raw  = _split_md_cells_raw(header_line)
            header_cells_trim = [c.strip() for c in header_cells_raw]
            rows_cells_raw = [_split_md_cells_raw(r) for r in row_lines]
            rows_cells_trim = [[c.strip() for c in r] for r in rows_cells_raw]

            na = nearest_anchor(anchors, start)
            title_hint = None
            # peek above the table for a few non-empty lines to get a hint
            for ln in range(max(0, start-6), start):
                if md_lines[ln].strip():
                    title_hint = md_lines[ln].strip()
            mdt = MDTable(
                table_index=len(out)+1,
                start_line=start,
                end_line=end_line,
                header_line_raw=header_line.rstrip("\n"),
                sep_line_raw=sep_line.rstrip("\n"),
                row_lines_raw=[r.rstrip("\n") for r in row_lines],
                header_cells_raw=header_cells_raw,
                header_cells_trim=header_cells_trim,
                rows_cells_raw=rows_cells_raw,
                rows_cells_trim=rows_cells_trim,
                raw_block="\n".join([header_line.rstrip("\n"), sep_line.rstrip("\n"), *[r.rstrip("\n") for r in row_lines]]),
                anchor_id=na.anchor_id if na else None,
                anchor_title=na.title if na else None,
                region_context=_ctx_region(start),
                effective_date_hint=_ctx_effective_date(recent_nonempty),
                title_hint=title_hint,
                mapped_as=None,
            )
            out.append(mdt)

            # update context window
            for ln in md_lines[max(0, start-6):start]:
                if ln.strip(): recent_nonempty.append(ln.strip())
            if len(recent_nonempty) > 40: recent_nonempty = recent_nonempty[-40:]
        else:
            i += 1
    return out

def write_md_tables_json(md_tables: List[MDTable], out_dir: Path) -> None:
    out_dir.mkdir(parents=True, exist_ok=True)
    # Per-table JSON files
    for t in md_tables:
        p = out_dir / f"mdtable-{t.table_index:04d}.json"
        p.write_text(json.dumps(asdict(t), ensure_ascii=False, indent=2), encoding="utf-8")
    # Flat JSONL index for quick loading
    with (out_dir.parent / "md_tables.jsonl").open("w", encoding="utf-8") as f:
        for t in md_tables:
            f.write(json.dumps(asdict(t), ensure_ascii=False) + "\n")
    # Summary index
    summary = {
        "count": len(md_tables),
        "files": [f"mdtable-{t.table_index:04d}.json" for t in md_tables],
        "note": "Each file contains header/rows raw+trim, line numbers, anchor, context, and raw block."
    }
    (out_dir / "index.json").write_text(json.dumps(summary, indent=2), encoding="utf-8")

# ----------------- generic table parser for normalized entities -----------------

def iter_md_tables_for_mapping(md_tables: List[MDTable]):
    """
    Provide the same tuple API as before, but from MDTable objects.
    """
    for t in md_tables:
        yield (t.start_line, t.header_cells_trim, t.rows_cells_trim, t)

# ----------------- shape-aware table mappers (unchanged except where noted) -----------------

def map_conversion_factors(header: List[str], rows: List[List[str]], eff: Optional[str]) -> Optional[List[Dict[str,Any]]]:
    hl = [h.lower() for h in header]
    if "section" not in hl or not any(h in hl for h in REGION_HDRS):
        return None
    out: List[Dict[str,Any]] = []
    region_idx = [(idx, header[idx]) for idx,h in enumerate(hl) if h in REGION_HDRS]
    for r_i, row in enumerate(rows, start=1):
        section = row[hl.index("section")].strip()
        if not section: continue
        for idx, htxt in region_idx:
            val = _to_float(row[idx])
            if val is None: continue
            out.append({
                "tname":"conversion_factors",
                "table_id":"tbl-conversion-factors",
                "row_id":f"tbl-cf-r{r_i:05d}-{idx}",
                "service_type": section,
                "region": _norm_region_name(htxt),
                "conv_factor": val,
                "rvu": None, "code": None, "description": None,
                "section":"CONVERSION FACTORS",
                "page":None,
                "effective_from": eff, "effective_to": None,
                "modifiers": None, "footnotes": None,
                "source_anchor": None, "table_title":"Regional conversion factors",
            })
    return out

def map_zip_ranges(header: List[str], rows: List[List[str]], region_ctx: Optional[str]) -> Optional[List[Dict[str,Any]]]:
    hl = [h.lower() for h in header]
    if hl.count("from") >= 2 and hl.count("thru") >= 2 and "region" not in hl:
        reg = _norm_region_name(region_ctx)
        out: List[Dict[str,Any]] = []
        for r_i, row in enumerate(rows, start=1):
            pairs = [(0,1), (2,3)]
            for pi,(fi,ti) in enumerate(pairs, start=1):
                if len(row) <= ti: continue
                f, t = row[fi], row[ti]
                if not f or not t: continue
                out.append({
                    "tname":"zip_regions",
                    "table_id":"tbl-zip-range",
                    "row_id":f"tbl-zip-r{r_i:05d}-{pi}",
                    "zip_from": float(f) if f.strip().isdigit() else float(_norm_text(f).replace(" ", "")) if _norm_text(f).replace(" ","").isdigit() else None,
                    "zip_to": float(t) if t.strip().isdigit() else float(_norm_text(t).replace(" ", "")) if _norm_text(t).replace(" ","").isdigit() else None,
                    "region": reg, "region_context": reg,
                    "section":"Postal ZIP codes by region",
                    "page":None,
                    "service_type":None, "code":None, "description":None,
                    "rvu":None, "conv_factor":None,
                    "effective_from":None, "effective_to":None,
                    "modifiers":None, "footnotes":None,
                    "source_anchor":None, "table_title":"Regional ZIP ranges",
                })
        return out
    return None

def map_zip_numeric(header: List[str], rows: List[List[str]]) -> Optional[List[Dict[str,Any]]]:
    hl = [h.lower() for h in header]
    if hl.count("from") >= 2 and hl.count("thru") >= 2 and hl.count("region") >= 2:
        out: List[Dict[str,Any]] = []
        for r_i, row in enumerate(rows, start=1):
            if len(row) < 6: continue
            for pi,(fi,ti,ri) in enumerate([(0,1,2),(3,4,5)], start=1):
                f,t,reg = row[fi], row[ti], row[ri]
                if not f or not t or not reg: continue
                out.append({
                    "tname":"zip_regions_numbered",
                    "table_id":"tbl-zip-numeric",
                    "row_id":f"tbl-zipn-r{r_i:05d}-{pi}",
                    "zip_from": float(f) if _norm_text(f).replace(" ","").isdigit() else None,
                    "zip_to": float(t) if _norm_text(t).replace(" ","").isdigit() else None,
                    "region": _norm_region_name(reg),
                    "section":"Numerical List of Postal ZIP Codes",
                    "page":None,"service_type":None,"code":None,"description":None,
                    "rvu":None,"conv_factor":None,"effective_from":None,"effective_to":None,
                    "modifiers":None,"footnotes":None,"source_anchor":None,"table_title":"Numerical ZIP → Region",
                })
        return out
    return None

def map_specialty(header: List[str], rows: List[List[str]]) -> Optional[List[Dict[str,Any]]]:
    hl = [h.lower() for h in header]
    if set(hl) == {"rating","description"}:
        out = []
        for i,row in enumerate(rows, start=1):
            if len(row) < 2: continue
            if not (row[0].strip() or row[1].strip()): continue
            out.append({
                "tname":"specialty_classifications","table_id":"tbl-specialty","row_id":f"tbl-spec-r{i:05d}",
                "code":row[0].strip(),"description":row[1].strip(),
                "service_type":None,"region":None,"rvu":None,"conv_factor":None,"section":"SPECIALTY CLASSIFICATIONS",
                "page":None,"effective_from":None,"effective_to":None,"modifiers":None,"footnotes":None,
                "source_anchor":None,"table_title":"Specialty Classifications",
            })
        return out
    return None

def map_section_code_ranges(header: List[str], rows: List[List[str]]) -> Optional[List[Dict[str,Any]]]:
    hl = [h.lower() for h in header]
    if "section" in hl and any("code" in h for h in hl):
        out = []
        for i,row in enumerate(rows, start=1):
            sec = row[hl.index("section")].strip()
            codes = " ".join([c for j,c in enumerate(row) if j != hl.index("section") and c.strip()])
            out.append({
                "tname":"section_codes","table_id":"tbl-section-codes","row_id":f"tbl-seccode-r{i:05d}",
                "section_name":sec,"codes_text":codes or None,
                "section":"FORMAT","page":None,"service_type":None,"code":None,"description":None,
                "rvu":None,"conv_factor":None,"region":None,
                "effective_from":None,"effective_to":None,"modifiers":None,"footnotes":None,
                "source_anchor":None,"table_title":"Section → CPT ranges",
            })
        return out
    return None

def map_code_grid(header: List[str], rows: List[List[str]], tname: str, title: str) -> Optional[List[Dict[str,Any]]]:
    if len(header) >= 2 and sum(1 for h in header if CODE_RE.match(h)) >= max(2, len(header)//2):
        codes: List[str] = []
        codes.extend([h for h in header if CODE_RE.match(h)])
        for r in rows:
            for c in r:
                if CODE_RE.match(c): codes.append(c)
        out = []
        for i,code in enumerate(codes, start=1):
            out.append({
                "tname": tname, "table_id": f"tbl-{tname}", "row_id": f"tbl-{tname}-r{i:06d}",
                "code": code, "section": title, "page": None,
                "service_type": None, "description": None, "rvu": None, "conv_factor": None, "region": None,
                "effective_from": None, "effective_to": None, "modifiers": None, "footnotes": None,
                "source_anchor": None, "table_title": title,
            })
        return out
    return None

def map_changed_values(header: List[str], rows: List[List[str]]) -> Optional[List[Dict[str,Any]]]:
    hl = [h.lower().strip().replace(" ", "") for h in header]
    needed = {"code","ny2018rvu","ny2012rvu","ny2018fud","ny2012fud"}
    if not needed.issubset(set(hl)): return None
    def col(name: str) -> int: return hl.index(name)
    out = []
    for i,row in enumerate(rows, start=1):
        code = row[col("code")].strip()
        rvu18_raw = row[col("ny2018rvu")].strip()
        rvu12_raw = row[col("ny2012rvu")].strip()
        fud18 = row[col("ny2018fud")].strip()
        fud12 = row[col("ny2012fud")].strip()
        pctc18 = row[hl.index("ny2018pc/tcsplit")] if "ny2018pc/tcsplit" in hl else ""
        pctc12 = row[hl.index("ny2012pc/tcsplit")] if "ny2012pc/tcsplit" in hl else ""
        out.append({
            "tname":"codes_changed_values","table_id":"tbl-changed-values","row_id":f"tbl-chgval-r{i:06d}",
            "code": code or None,
            "rvu_2018_raw": rvu18_raw or None, "rvu_2018": _to_float(rvu18_raw),
            "rvu_2012_raw": rvu12_raw or None, "rvu_2012": _to_float(rvu12_raw),
            "fud_2018": fud18 or None, "fud_2012": fud12 or None,
            "pctc_2018": pctc18.strip() or None, "pctc_2012": pctc12.strip() or None,
            "section":"CHANGED CODES / Changed Values", "page":None,
            "service_type":None,"description":None,"rvu":None,"conv_factor":None,"region":None,
            "effective_from":None,"effective_to":None,"modifiers":None,"footnotes":None,
            "source_anchor":None,"table_title":"Changed Values",
        })
    return out

def map_fud_defs(header: List[str], rows: List[List[str]]) -> Optional[List[Dict[str,Any]]]:
    sample = [c.strip().upper() for c in header[:2]]
    candidates = {"MMM","XXX","YYY","ZZZ"}
    if any(x in candidates for x in sample) or any((r and r[0].strip().upper() in candidates) for r in rows):
        out=[]
        allrows: List[List[str]] = []
        if header and header[0].strip().upper() in candidates:
            allrows.append([header[0].strip(), header[1].strip() if len(header)>1 else ""])
        allrows.extend(rows)
        for i,r in enumerate(allrows, start=1):
            if not r: continue
            code = r[0].strip().upper()
            if code not in candidates: continue
            desc = (r[1] if len(r)>1 else "").strip()
            out.append({
                "tname":"fud_defs","table_id":"tbl-fud-defs","row_id":f"tbl-fud-r{i:05d}",
                "fud_code": code, "description": desc or None,
                "section":"FUD", "page":None, "table_title":"FUD Codes",
                "service_type":None,"code":None,"rvu":None,"conv_factor":None,"region":None,
                "effective_from":None,"effective_to":None,"modifiers":None,"footnotes":None,"source_anchor":None,
            })
        return out
    return None

# ----------------- narrative harvesters (unchanged) -----------------

def harvest_icons(md_lines: List[str]) -> List[Dict[str,Any]]:
    out=[]
    in_icons=False
    for i,line in enumerate(md_lines, start=1):
        if re.match(r"^##\s*Icons\b", line.strip(), re.I):
            in_icons=True; continue
        if in_icons and line.strip().startswith("## "):  # next section
            break
        if in_icons and line.strip().startswith("- "):
            item = line.strip()[2:].strip()
            m = re.match(r"^([■\+\*B®∞])\s+(.*)$", item)
            if m:
                token, meaning = m.group(1), m.group(2).strip()
            else:
                parts = item.split(" ",1)
                token = parts[0]; meaning = parts[1].strip() if len(parts)>1 else ""
            out.append({"token":token,"meaning":meaning})
    return out

def harvest_modifiers(md_lines: List[str]) -> List[Dict[str,Any]]:
    out=[]
    current=None
    buf=[]
    head_pat = re.compile(r"^##\s*([TPA-Z]{1,2}C|TC|AJ|NP|PA|83|[0-9]{2})\s+(.*)$", re.I)
    for i,line in enumerate(md_lines, start=1):
        m = head_pat.match(line.strip())
        if m:
            if current:
                out.append(current | {"body":"\n".join(buf).strip(), "line_start": current["line_start"]})
                buf=[]
            code = m.group(1).strip()
            title = m.group(2).strip()
            current={"code":code, "title":title, "line_start": i}
            continue
        if current:
            if line.strip().startswith("## "):
                out.append(current | {"body":"\n".join(buf).strip(), "line_start": current["line_start"]})
                current=None; buf=[]; continue
            buf.append(line.rstrip())
    if current:
        out.append(current | {"body":"\n".join(buf).strip(), "line_start": current["line_start"]})
    return out

def harvest_glossary(md_lines: List[str]) -> List[Dict[str,Any]]:
    text = "\n".join(md_lines)
    def grab(section: str) -> Optional[str]:
        m = re.search(rf"##\s*{re.escape(section)}.*?(##|$)", text, re.S|re.I)
        if not m: return None
        block = m.group(0)
        body = "\n".join([l for l in block.splitlines()[1:] if l.strip()])[:2000]
        return body.strip()
    items=[]
    for term, section in [
        ("RVU","Relative Value"),
        ("CF","CONVERSION FACTORS"),
        ("E/M","Evaluation and Management"),
        ("BR","BR"),
        ("NC","NC"),
        ("FUD","FUD"),
        ("PC/TC","PC/TC Split"),
        ("CPT","Code"),
    ]:
        body = grab(section)
        if body:
            items.append({"term":term,"definition":body})
    return items

def harvest_base_rules(md_lines: List[str], out_dir: Path) -> None:
    txt = "\n".join(md_lines)
    rules=[]
    if re.search(r"Relative value\s*x\s*applicable conversion factor\s*=\s*fee", txt, re.I):
        rules.append({
            "rule_id":"BASE_FEE_RULE",
            "type":"base",
            "expression":"FEE = RVU * CF",
            "provenance":{"section":"Calculating Fees Using Relative Values and Conversion Factors"}
        })
    (out_dir / "rules.base.json").write_text(json.dumps(rules, indent=2), encoding="utf-8")

def harvest_pc_tc_rule(md_lines: List[str], out_dir: Path) -> None:
    txt = "\n".join(md_lines)
    if re.search(r"PC/TC Split", txt, re.I):
        rules=[{
            "rule_id":"PC_TC_SPLIT_RULE",
            "type":"pc_tc",
            "expression":"PC = (RVU * CF) * (PC_percent/100); TC = (RVU * CF) * (TC_percent/100)",
            "constraints":["PC + TC <= RVU * CF"],
            "provenance":{"section":"PC/TC Split"}
        }]
        (out_dir / "rules.pc_tc.json").write_text(json.dumps(rules, indent=2), encoding="utf-8")

def harvest_crossrefs(md_lines: List[str], anchors: List[Anchor]) -> List[Dict[str,Any]]:
    out=[]
    pat = re.compile(r"\b[Ss]ee\s+(the\s+)?(?P<what>(Category\s+III\s+Codes\s+section|modifier\s+\d{2}|modifier\s+TC|codes?\s+[0-9A-Z\- ,]+))", re.I)
    for i, line in enumerate(md_lines, start=1):
        for m in pat.finditer(line):
            what = m.group("what").strip()
            kind="text"
            target=None
            if re.search(r"modifier\s+(TC|[0-9]{2})", what, re.I):
                kind="modifier"
                target = re.search(r"(TC|[0-9]{2})", what, re.I).group(1).upper()
            elif re.search(r"Category\s+III", what, re.I):
                kind="section"; target="Category III Codes"
            elif re.search(r"codes?\s+", what, re.I):
                kind="code_list"; target=what
            na = nearest_anchor(anchors, i)
            out.append({
                "source_anchor": na.anchor_id if na else None,
                "kind": kind, "target": target, "text": line.strip(), "line_no": i
            })
    return out

# ----------------- main orchestrator -----------------

def main():
    ap = argparse.ArgumentParser(description="Phase 1: MD → Entities (tables.parquet + JSON sidecars + raw md_tables JSON)")
    ap.add_argument("--version-dir", required=True, help="law_versions/<id>")
    ap.add_argument("--engine", choices=["pyarrow","fastparquet"], default="pyarrow")
    args = ap.parse_args()

    vdir = Path(args.version_dir)
    md_path = vdir / "derived" / "law.md"
    ddir = vdir / "derived"
    out_parquet = ddir / "tables.parquet"
    out_gloss = ddir / "glossary.json"
    out_mods = ddir / "modifiers.json"
    out_icons = ddir / "icons.json"
    out_anchors = ddir / "anchors.json"
    out_xrefs = ddir / "crossrefs.json"
    out_md_tables_dir = ddir / "md_tables"

    assert md_path.exists(), f"missing {md_path}"
    md_lines = md_path.read_text(encoding="utf-8", errors="ignore").splitlines()

    anchors = parse_anchors(md_lines)
    out_anchors.write_text(json.dumps([a.__dict__ for a in anchors], indent=2), encoding="utf-8")

    # RAW MD TABLES (as-is)
    md_tables = iter_md_tables_raw(md_lines)
    write_md_tables_json(md_tables, out_md_tables_dir)

    # Narrative harvesters
    icons = harvest_icons(md_lines)
    if icons: out_icons.write_text(json.dumps(icons, indent=2), encoding="utf-8")
    mods = harvest_modifiers(md_lines)
    if mods: out_mods.write_text(json.dumps(mods, indent=2), encoding="utf-8")
    gloss = harvest_glossary(md_lines)
    if gloss: out_gloss.write_text(json.dumps(gloss, indent=2), encoding="utf-8")
    harvest_base_rules(md_lines, ddir)
    harvest_pc_tc_rule(md_lines, ddir)
    xrefs = harvest_crossrefs(md_lines, anchors)
    if xrefs: out_xrefs.write_text(json.dumps(xrefs, indent=2), encoding="utf-8")

    # Normalized entities (using trimmed cells)
    all_rows: List[Dict[str,Any]] = []
    for start, header, rows, tmeta in iter_md_tables_for_mapping(md_tables):
        eff = tmeta.effective_date_hint
        region_ctx = tmeta.region_context

        mapped_name = None
        for mapper, tag in (
            (map_conversion_factors, "conversion_factors"),
            (map_zip_numeric, "zip_regions_numbered"),
            (lambda h,r: map_zip_ranges(h,r,region_ctx), "zip_regions"),
            (map_specialty, "specialty_classifications"),
            (map_section_code_ranges, "section_codes"),
            (map_changed_values, "codes_changed_values"),
        ):
            out = mapper(header, rows, eff) if mapper is map_conversion_factors else mapper(header, rows)
            if out:
                all_rows.extend(out)
                mapped_name = tag
                break
        else:
            titled_block = (tmeta.title_hint or "").lower()
            out = None
            if "new cpt codes" in titled_block:
                out = map_code_grid(header, rows, tname="codes_new", title="NEW CPT CODES")
            elif "changed descriptions" in titled_block:
                out = map_code_grid(header, rows, tname="codes_changed_desc", title="Changed Descriptions")
            elif "deleted cpt codes" in titled_block:
                out = map_code_grid(header, rows, tname="codes_deleted", title="DELETED CPT CODES")
            if not out:
                out = map_fud_defs(header, rows)
            if out:
                all_rows.extend(out)
                mapped_name = out[0]["tname"]
            else:
                for r_i, r in enumerate(rows, start=1):
                    all_rows.append({
                        "tname":"other","table_id":"tbl-generic","row_id":f"tbl-gen-r{len(all_rows)+1:06d}",
                        "section":None,"page":None,"service_type":None,"code":None,
                        "description":" | ".join(r),"rvu":None,"conv_factor":None,"region":_norm_region_name(region_ctx),
                        "effective_from":eff,"effective_to":None,"modifiers":None,"footnotes":None,"source_anchor":tmeta.anchor_id,
                        "table_title":"Generic table",
                    })
                mapped_name = "other"

        # annotate raw table with mapper tag for your analysis
        tmeta.mapped_as = mapped_name

    # Write parquet
    df = pd.DataFrame(all_rows) if all_rows else pd.DataFrame(columns=[
        "tname","table_id","row_id","section","page","service_type","code","description",
        "rvu","conv_factor","region","effective_from","effective_to","modifiers","footnotes",
        "source_anchor","table_title"
    ])
    out_parquet.parent.mkdir(parents=True, exist_ok=True)
    if args.engine == "pyarrow":
        df.to_parquet(out_parquet, engine="pyarrow", index=False)
    else:
        df.to_parquet(out_parquet, engine="fastparquet", index=False)

    # Also update md_tables files with 'mapped_as'
    for t in md_tables:
        p = out_md_tables_dir / f"mdtable-{t.table_index:04d}.json"
        if p.exists():
            p.write_text(json.dumps(asdict(t), ensure_ascii=False, indent=2), encoding="utf-8")

    # console summary
    by = df.groupby("tname").size().reset_index(name="rows").sort_values("rows", ascending=False)
    print("\n== tables.parquet groups ==")
    if not by.empty:
        print(by.to_string(index=False))
    else:
        print("(no rows)")
    print(f"\n[OK] Wrote:\n - {out_parquet}\n - {out_gloss}\n - {out_mods}\n - {out_icons}\n - {out_anchors}\n - {out_xrefs}\n - {out_md_tables_dir}/ (per-table JSON) + {ddir/'md_tables.jsonl'}\n - rules.base.json, rules.pc_tc.json\n")

if __name__ == "__main__":
    main()
