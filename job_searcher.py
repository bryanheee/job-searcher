"""
job_searcher.py
---------------
Automated job searcher for control systems / mechatronics / GNC roles in the Netherlands.
Scrapes both Indeed NL and LinkedIn, scores each job on keyword depth and seniority,
blacklists PLC/SCADA/field-service, and outputs a ranked list with a fit score.

Also maintains a persistent SQLite database (jobs.db) that accumulates every scrape run,
storing matched jobs AND rejected jobs with rejection reasons. Generates analytics.html
with trend charts powered by Chart.js.

Setup:
    pip install python-jobspy pandas schedule

Email setup (Gmail):
    1. Go to myaccount.google.com > Security > App Passwords
    2. Create an app password for "Mail"
    3. Set environment variables EMAIL_SENDER, EMAIL_PASSWORD  (or fill in below)

Usage:
    python job_searcher.py              # print to terminal
    python job_searcher.py --email      # also send email digest
    python job_searcher.py --save       # also save to CSV
    python job_searcher.py --site       # also generate index.html for GitHub Pages
    python job_searcher.py --daily      # run once now + schedule daily at 08:00
    python job_searcher.py --site --save --email   # full run
"""

import os
import re
import math
import hashlib
import sqlite3
import json
import pandas as pd
import smtplib
import schedule
import time
import argparse
from jobspy import scrape_jobs
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from datetime import datetime, timedelta

# ─────────────────────────────────────────────────────────────────
# CONFIG LOADING
# ─────────────────────────────────────────────────────────────────

# Output directory for generated files (index.html, CSV, jobs.db)
OUTPUT_DIR  = os.path.dirname(os.path.abspath(__file__))
DB_PATH     = os.path.join(OUTPUT_DIR, "jobs.db")
CONFIG_PATH = os.path.join(OUTPUT_DIR, "config.json")

_DEFAULT_CONFIG: dict = {
    "search_queries": [
        "control systems engineer",
        "mechatronics engineer",
        "motion control engineer",
        "GNC engineer",
        "control engineer MATLAB",
        "systems and control engineer",
        "servo control engineer",
        "embedded control engineer",
        "flight dynamics engineer",
        "precision engineering control",
    ],
    "scored_keywords": [
        {
            "weight": 3,
            "keywords": [
                "mpc", "model predictive control",
                "robust control", "h-infinity", "h infinity", "mu-synthesis",
                "nonlinear control", "nonlinear systems",
                "kalman", "state observer", "luenberger",
                "lqr", "lqg",
                "state-space", "state space",
                "system identification", "sysid",
                "gnc", "guidance navigation control",
                "deep learning", "machine learning",
                "python", "pytorch", "tensorflow",
            ],
        },
        {
            "weight": 2,
            "keywords": [
                "matlab", "simulink",
                "control theory", "feedback control",
                "motion control", "servo", "servomotor",
                "mechatronics", "mechatronic",
                "robotics", "robot",
                "aerospace", "avionics", "flight control",
                "embedded control", "real-time control", "real time control",
            ],
        },
        {
            "weight": 1,
            "keywords": [
                "control", "automation", "dynamical systems",
                "high-tech", "high tech",
                "precision", "semiconductor",
                "sensor fusion", "localization",
                "navigation", "inertial",
                "signal processing",
                "pid", "pid control",
                "frequency domain", "bode", "nyquist",
                "optical", "lithography",
                "unmanned", "uav", "drone",
                "satellite", "space",
            ],
        },
    ],
    "blacklist_title": [
        "plc", "scada", "hmi",
        "automation engineer",
        "welding",
    ],
    "blacklist_full": [
        "ladder logic", "tia portal", "siemens s7", "step 7",
        "beckhoff ads", "codesys",
    ],
    "senior_patterns": [
        r"\bsenior\b", r"\bsr\b", r"\blead\b", r"\bprincipal\b",
        r"\bstaff\b", r"\bdirector\b", r"\bvp\b", r"\bhead of\b",
        r"\bmanager\b", r"\barchitect\b",
        r"\b[5-9]\+\s*year", r"\b1[0-9]\+\s*year",
    ],
    "target_companies": [
        "asml", "sioux", "nobleo", "demcon", "tmc", "nlr", "tno",
        "orange aerospace", "vanderlande", "lely",
        "thales", "fokker", "airbus", "canon production printing",
        "marin", "smart robotics", "avular", "philips",
        "nxp", "prodrive", "daf", "vi grade", "mecaer",
    ],
    "results_per_query": 30,
    "hours_old": 168,
    "min_keywords": 1,
}


def load_config() -> dict:
    """
    Load settings from config.json.
    If the file does not exist, write the default config and return it.
    If the file is malformed, warn and fall back to defaults.
    """
    if not os.path.exists(CONFIG_PATH):
        print(f"[config] config.json not found — writing defaults to {CONFIG_PATH}")
        _write_config(_DEFAULT_CONFIG)
        return _DEFAULT_CONFIG

    try:
        with open(CONFIG_PATH, "r", encoding="utf-8") as fh:
            cfg = json.load(fh)
        print(f"[config] Loaded configuration from {CONFIG_PATH}")
        return cfg
    except Exception as exc:
        print(f"[config] Warning: could not parse config.json ({exc}) — using defaults.")
        return _DEFAULT_CONFIG


def _write_config(cfg: dict) -> None:
    """Write a config dict to config.json."""
    with open(CONFIG_PATH, "w", encoding="utf-8") as fh:
        json.dump(cfg, fh, indent=2, ensure_ascii=False)


# ─────────────────────────────────────────────────────────────────
# CONFIGURATION (loaded from config.json at startup)
# ─────────────────────────────────────────────────────────────────

_cfg = load_config()

SEARCH_QUERIES: list[str] = _cfg.get("search_queries", _DEFAULT_CONFIG["search_queries"])

LOCATION          = "Netherlands"
RESULTS_PER_QUERY: int = int(_cfg.get("results_per_query", _DEFAULT_CONFIG["results_per_query"]))
HOURS_OLD:         int = int(_cfg.get("hours_old",         _DEFAULT_CONFIG["hours_old"]))
MIN_KEYWORDS:      int = max(0, int(_cfg.get("min_keywords", _DEFAULT_CONFIG["min_keywords"])))

# ── Scored keyword tiers ───────────────────────────────────────────
# Each keyword group has a weight. A job's score =
# sum over tiers of (weight × number of unique keywords matched in that tier).
# Theoretical max = _MAX_POSSIBLE_SCORE (every keyword in every tier matched).
# config.json stores these as [{weight: N, keywords: [...]}] objects.
SCORED_KEYWORDS: list[tuple[int, list[str]]] = [
    (int(entry["weight"]), list(entry["keywords"]))
    for entry in _cfg.get("scored_keywords", _DEFAULT_CONFIG["scored_keywords"])
]

# Flat list for the "required" gate — job must contain at least one weight-2/3 keyword
# (prevents pure weight-1 matches like "automation" in irrelevant jobs sneaking through)
GATE_KEYWORDS: list[str] = [kw for weight, group in SCORED_KEYWORDS if weight >= 2 for kw in group]

# ── Blacklist: title-scoped vs full-text ───────────────────────────
BLACKLIST_TITLE: list[str] = _cfg.get("blacklist_title", _DEFAULT_CONFIG["blacklist_title"])
BLACKLIST_FULL:  list[str] = _cfg.get("blacklist_full",  _DEFAULT_CONFIG["blacklist_full"])

# ── Seniority filter ───────────────────────────────────────────────
SENIOR_TITLE_PATTERNS: list[str] = _cfg.get("senior_patterns", _DEFAULT_CONFIG["senior_patterns"])

# Target companies — flagged with star and boosted in ranking
TARGET_COMPANIES: list[str] = _cfg.get("target_companies", _DEFAULT_CONFIG["target_companies"])

# ── Email ──────────────────────────────────────────────────────────
EMAIL_SENDER   = os.environ.get("EMAIL_SENDER",   "your_gmail@gmail.com")
EMAIL_PASSWORD = os.environ.get("EMAIL_PASSWORD", "your_app_password_here")
EMAIL_RECEIVER = os.environ.get("EMAIL_RECEIVER", "oscarhe1998@gmail.com")

# ─────────────────────────────────────────────────────────────────
# SCRAPER
# ─────────────────────────────────────────────────────────────────

def scrape_all() -> pd.DataFrame:
    """Scrape Indeed + LinkedIn for all search queries. Return combined, URL-deduped DataFrame."""
    all_frames: list[pd.DataFrame] = []

    for query in SEARCH_QUERIES:
        print(f"  Searching: '{query}' ...")
        try:
            jobs = scrape_jobs(
                site_name                  = ["indeed", "linkedin"],
                search_term                = query,
                location                   = LOCATION,
                results_wanted             = RESULTS_PER_QUERY,
                country_indeed             = "Netherlands",
                linkedin_fetch_description = True,
                hours_old                  = HOURS_OLD,
            )
            if not jobs.empty:
                jobs["query"] = query
                all_frames.append(jobs)
            time.sleep(2)  # polite delay
        except Exception as exc:
            print(f"  [!] Error searching '{query}': {exc}")

    if not all_frames:
        print("  [!] No results returned from any query.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True)

    # Primary dedup: exact URL
    if "job_url" in combined.columns:
        combined = combined.drop_duplicates(subset="job_url", keep="first")

    # Secondary dedup: same title + company (catches cross-board duplicates)
    combined = _fuzzy_dedup(combined)

    print(f"  {len(combined)} unique raw results after deduplication.")
    return combined


def _normalise_str(s: str) -> str:
    """Lowercase, collapse whitespace, strip punctuation for fuzzy comparison."""
    s = str(s).lower().strip()
    s = re.sub(r"[^a-z0-9 ]", " ", s)
    return re.sub(r"\s+", " ", s)


def _fuzzy_dedup(df: pd.DataFrame) -> pd.DataFrame:
    """Remove rows where (normalised title, normalised company) pair is duplicated."""
    if df.empty:
        return df
    df = df.copy()
    df["_title_norm"]   = df.get("title",   pd.Series(dtype=str)).fillna("").apply(_normalise_str)
    df["_company_norm"] = df.get("company", pd.Series(dtype=str)).fillna("").apply(_normalise_str)
    df["_dedup_key"]    = df["_title_norm"] + " || " + df["_company_norm"]
    before = len(df)
    df = df.drop_duplicates(subset="_dedup_key", keep="first")
    removed = before - len(df)
    if removed:
        print(f"  Fuzzy dedup removed {removed} cross-board duplicate(s).")
    df = df.drop(columns=["_title_norm", "_company_norm", "_dedup_key"])
    return df


# ─────────────────────────────────────────────────────────────────
# SCORING & FILTERING
# ─────────────────────────────────────────────────────────────────

_MAX_POSSIBLE_SCORE: float = sum(w * len(kws) for w, kws in SCORED_KEYWORDS)


def _compute_fit_score(text: str) -> tuple[float, list[str]]:
    """
    Score a job against SCORED_KEYWORDS. Returns (score, matched_kws).
    Each tier contributes weight * unique_keyword_count — so matching more
    keywords in a tier gives a proportionally higher score.
    """
    raw   = 0.0
    found: list[str] = []
    for weight, group in SCORED_KEYWORDS:
        hits = [kw for kw in group if kw in text]
        if hits:
            raw += weight * len(hits)   # multiply by count, not just binary
            found.extend(hits[:3])  # cap per-group display to 3
    score = round(raw, 1)
    return score, found


def _count_matched_groups(text: str) -> int:
    """
    Count total unique individual keywords matched across ALL tiers.
    Used by the min_keywords gate — e.g. min_keywords=5 means at least
    5 individual keywords (across any tier) must appear in the job text.
    """
    return sum(
        1
        for _weight, group in SCORED_KEYWORDS
        for kw in group
        if kw in text
    )


def _is_senior_title(title: str) -> bool:
    """Return True if the title contains a seniority signal Oscar cannot pass."""
    t = title.lower()
    return any(re.search(p, t) for p in SENIOR_TITLE_PATTERNS)


def _passes_blacklist(title_text: str, full_text: str) -> bool:
    """Return True if the job is NOT blacklisted."""
    title_lower = title_text.lower()
    full_lower  = full_text.lower()
    if any(kw in title_lower for kw in BLACKLIST_TITLE):
        return False
    if any(kw in full_lower for kw in BLACKLIST_FULL):
        return False
    return True


def filter_jobs(df: pd.DataFrame) -> pd.DataFrame:
    """Apply seniority filter, blacklist, gate, and fit scoring. Return ranked DataFrame."""
    if df.empty:
        return df

    df = df.copy()

    # Build searchable text blobs
    text_cols = ["title", "company", "description", "location"]
    available = [c for c in text_cols if c in df.columns]
    df["_full_text"]  = df[available].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
    df["_title_text"] = df.get("title", pd.Series(dtype=str)).fillna("").str.lower()

    # 1. Seniority filter (title only)
    before = len(df)
    df = df[~df["_title_text"].apply(_is_senior_title)]
    print(f"  Seniority filter removed {before - len(df)} senior/lead roles.")

    # 2. Blacklist
    before = len(df)
    df = df[df.apply(lambda r: _passes_blacklist(r["_title_text"], r["_full_text"]), axis=1)]
    print(f"  Blacklist removed {before - len(df)} jobs.")

    # 3. Gate: must match at least one weight-2 or weight-3 keyword
    gate_pattern = "|".join(re.escape(kw) for kw in GATE_KEYWORDS)
    before = len(df)
    df = df[df["_full_text"].str.contains(gate_pattern, regex=True)]
    print(f"  Gate filter removed {before - len(df)} low-signal jobs.")

    # 3b. Minimum total keywords gate (configurable via min_keywords)
    if MIN_KEYWORDS > 1 and not df.empty:
        before = len(df)
        df = df[df["_full_text"].apply(_count_matched_groups) >= MIN_KEYWORDS]
        print(f"  Min-keywords gate (>= {MIN_KEYWORDS} total keywords) removed {before - len(df)} jobs.")

    if df.empty:
        print("  No jobs passed all filters.")
        return df

    # 4. Fit score
    scores_and_kws = df["_full_text"].apply(_compute_fit_score)
    df["fit_score"]       = scores_and_kws.apply(lambda x: x[0])
    df["matched_keywords"] = scores_and_kws.apply(lambda x: x[1])

    # 5. Target company flag
    df["is_target"] = df.get("company", pd.Series(dtype=str)).fillna("").str.lower().apply(
        lambda c: any(tc in c for tc in TARGET_COMPANIES)
    )

    # 6. Sort: target companies first, then by fit score descending
    df = df.sort_values(
        by=["is_target", "fit_score"],
        ascending=[False, False]
    )

    # Drop internal helper columns
    df = df.drop(columns=["_full_text", "_title_text"])

    print(f"  {len(df)} jobs passed all filters.")
    return df


# ─────────────────────────────────────────────────────────────────
# CATEGORY CLASSIFICATION
# ─────────────────────────────────────────────────────────────────

# Category definitions: evaluated in order, first match wins.
# Each entry: (category_name, list_of_keyword_signals)
_CATEGORY_RULES: list[tuple[str, list[str]]] = [
    ("GNC/Aerospace", [
        "gnc", "guidance navigation control", "flight control", "flight dynamics",
        "avionics", "aerospace", "satellite", "space systems", "astrodynamics",
        "uav", "drone", "unmanned", "fokker", "airbus", "nlr", "orange aerospace",
    ]),
    ("High-Tech/Precision", [
        "asml", "lithography", "semiconductor", "wafer", "optical",
        "high-tech", "high tech", "precision motion", "canon production printing",
        "nxp", "philips", "prodrive", "demcon", "sioux",
    ]),
    ("Robotics", [
        "robotics", "robot", "ros", "ros2", "manipulator", "mobile robot",
        "autonomous", "smart robotics", "avular", "nobleo",
    ]),
    ("General Control", [
        "control systems", "control engineer", "mechatronics", "motion control",
        "servo", "embedded control", "state-space", "matlab", "simulink",
        "feedback control", "pid control",
    ]),
]

_BLACKLISTED_CATEGORY_RULES: list[tuple[str, list[str]]] = [
    ("Blacklisted-PLC", [
        "plc", "scada", "hmi", "ladder logic", "tia portal",
        "siemens s7", "step 7", "beckhoff ads", "codesys",
    ]),
    ("Blacklisted-Other", []),  # catch-all for other blacklisted reasons
]


def _classify_job(title: str, full_text: str, rejection_reason: str) -> str:
    """
    Assign a category string to a job.
    Accepted jobs are classified by domain. Rejected jobs get a Blacklisted/Rejected label.
    """
    if rejection_reason:
        if "senior" in rejection_reason.lower() or "seniority" in rejection_reason.lower():
            return "Rejected-Senior"
        title_lower = title.lower()
        full_lower  = full_text.lower()
        plc_signals = [
            "plc", "scada", "hmi", "automation engineer", "welding",
            "ladder logic", "tia portal", "siemens s7", "step 7",
            "beckhoff ads", "codesys",
        ]
        if any(s in title_lower or s in full_lower for s in plc_signals):
            return "Blacklisted-PLC"
        if "gate" in rejection_reason.lower() or "no gate" in rejection_reason.lower():
            return "Rejected-No-Gate"
        return "Blacklisted-Other"

    # Accepted job — classify by domain
    combined = (title + " " + full_text).lower()
    for category, signals in _CATEGORY_RULES:
        if any(s in combined for s in signals):
            return category

    return "General Control"  # fallback for accepted jobs


# ─────────────────────────────────────────────────────────────────
# SQLITE PERSISTENCE
# ─────────────────────────────────────────────────────────────────

def _init_db() -> sqlite3.Connection:
    """
    Open (or create) jobs.db and ensure the schema exists.
    Returns an open connection. Caller is responsible for closing it.
    """
    conn = sqlite3.connect(DB_PATH)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("""
        CREATE TABLE IF NOT EXISTS jobs (
            job_id          TEXT NOT NULL,
            run_date        TEXT NOT NULL,
            year            INTEGER NOT NULL,
            week_number     INTEGER NOT NULL,
            title           TEXT,
            company         TEXT,
            location        TEXT,
            site            TEXT,
            fit_score       REAL,
            category        TEXT,
            is_target       INTEGER,
            job_url         TEXT,
            rejection_reason TEXT,
            PRIMARY KEY (job_id, run_date)
        )
    """)
    conn.commit()
    return conn


def _job_id(title: str, company: str, url: str) -> str:
    """
    Stable hash-based ID for a job. Uses URL when available; falls back to
    title+company. This lets the DB track whether the same posting reappears
    across multiple scrape runs without creating duplicate primary keys.
    """
    key = url.strip() if url and url != "#" else f"{title}|{company}"
    return hashlib.md5(key.encode("utf-8")).hexdigest()


def save_to_db(
    accepted_df: pd.DataFrame,
    raw_df: pd.DataFrame,
    accepted_ids: set,
    run_dt: datetime,
):
    """
    Persist this scrape run to jobs.db.

    Accepted jobs: all rows in accepted_df.
    Rejected jobs: all rows in raw_df whose job_id is NOT in accepted_ids,
                   with a computed rejection_reason.
    """
    conn = _init_db()
    run_date    = run_dt.strftime("%Y-%m-%d")
    year        = run_dt.isocalendar()[0]
    week_number = run_dt.isocalendar()[1]

    rows_inserted = 0
    rows_skipped  = 0

    def _upsert(job_id, title, company, location, site, fit_score,
                category, is_target, job_url, rejection_reason):
        nonlocal rows_inserted, rows_skipped
        try:
            conn.execute(
                """
                INSERT OR IGNORE INTO jobs
                  (job_id, run_date, year, week_number,
                   title, company, location, site,
                   fit_score, category, is_target, job_url, rejection_reason)
                VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?)
                """,
                (
                    job_id, run_date, year, week_number,
                    title, company, location, site,
                    fit_score, category, int(bool(is_target)),
                    job_url, rejection_reason,
                ),
            )
            if conn.execute("SELECT changes()").fetchone()[0]:
                rows_inserted += 1
            else:
                rows_skipped += 1
        except Exception as exc:
            print(f"  [DB] Insert error for '{title}': {exc}")

    # ── Accepted jobs ──────────────────────────────────────────────
    for _, row in accepted_df.iterrows():
        title   = str(row.get("title",   "") or "")
        company = str(row.get("company", "") or "")
        url     = str(row.get("job_url", "") or "")
        jid     = _job_id(title, company, url)
        full_text = (
            title + " " +
            str(row.get("description", "") or "")
        ).lower()
        category = _classify_job(title, full_text, rejection_reason="")
        _upsert(
            job_id           = jid,
            title            = title,
            company          = company,
            location         = str(row.get("location", "") or ""),
            site             = str(row.get("site", "") or ""),
            fit_score        = float(row.get("fit_score", 0) or 0),
            category         = category,
            is_target        = bool(row.get("is_target", False)),
            job_url          = url,
            rejection_reason = None,
        )

    # ── Rejected jobs ──────────────────────────────────────────────
    # Rebuild full_text for raw_df rows the same way filter_jobs does.
    if not raw_df.empty:
        text_cols = ["title", "company", "description", "location"]
        available = [c for c in text_cols if c in raw_df.columns]
        raw_df = raw_df.copy()
        raw_df["_full_text"]  = raw_df[available].fillna("").astype(str).agg(" ".join, axis=1).str.lower()
        raw_df["_title_text"] = raw_df.get("title", pd.Series(dtype=str)).fillna("").str.lower()

        gate_pattern = "|".join(re.escape(kw) for kw in GATE_KEYWORDS)

        for _, row in raw_df.iterrows():
            title     = str(row.get("title",   "") or "")
            company   = str(row.get("company", "") or "")
            url       = str(row.get("job_url", "") or "")
            jid       = _job_id(title, company, url)

            if jid in accepted_ids:
                continue  # already recorded above

            title_lower = row["_title_text"]
            full_lower  = row["_full_text"]

            # Determine rejection reason
            if _is_senior_title(title_lower):
                reason = "Seniority filter: title contains senior/lead/principal"
            elif not _passes_blacklist(title_lower, full_lower):
                # Determine which blacklist triggered
                plc_title = any(kw in title_lower for kw in BLACKLIST_TITLE)
                reason = (
                    "Blacklisted (title): PLC/SCADA/HMI/automation engineer/welding"
                    if plc_title
                    else "Blacklisted (full text): ladder logic/TIA Portal/Beckhoff/CodeSys"
                )
            elif not re.search(gate_pattern, full_lower):
                reason = "No gate keyword: missing weight-2/3 signal (MATLAB/robotics/control etc.)"
            elif MIN_KEYWORDS > 1 and _count_matched_groups(full_lower) < MIN_KEYWORDS:
                reason = f"Below min_keywords gate: fewer than {MIN_KEYWORDS} total keywords matched across all tiers"
            else:
                reason = "Below fit threshold or unclassified rejection"

            category = _classify_job(title, full_lower, rejection_reason=reason)
            _upsert(
                job_id           = jid,
                title            = title,
                company          = company,
                location         = str(row.get("location", "") or ""),
                site             = str(row.get("site", "") or ""),
                fit_score        = 0.0,
                category         = category,
                is_target        = False,
                job_url          = url,
                rejection_reason = reason,
            )

    conn.commit()
    conn.close()
    print(f"  [DB] {rows_inserted} rows inserted, {rows_skipped} already present — {DB_PATH}")


# ─────────────────────────────────────────────────────────────────
# ANALYTICS DATA PREPARATION
# ─────────────────────────────────────────────────────────────────

# Categories Oscar considers "control theory" jobs — used for the analytics toggle
_CONTROL_THEORY_CATEGORIES = {"GNC/Aerospace", "High-Tech/Precision", "General Control"}


def _aggregate_records(records: list[dict], week_keys: list[str]) -> dict:
    """
    Aggregate a list of job records into the chart payload shape.
    Pulled out of _load_analytics_data so the same logic can produce
    both the full payload and a control-theory-only filtered payload.
    """
    all_categories = [
        "GNC/Aerospace", "High-Tech/Precision", "Robotics", "General Control",
        "Blacklisted-PLC", "Blacklisted-Other", "Rejected-Senior", "Rejected-No-Gate",
    ]

    def week_key(r):
        return f"{r['year']}-W{r['week_number']:02d}"

    accepted = [r for r in records if not r["rejection_reason"]]
    rejected = [r for r in records if r["rejection_reason"]]

    jobs_per_week     = {wk: 0 for wk in week_keys}
    rejected_per_week = {wk: 0 for wk in week_keys}
    for r in accepted:
        jobs_per_week[week_key(r)] += 1
    for r in rejected:
        rejected_per_week[week_key(r)] += 1

    cat_per_week = {wk: {cat: 0 for cat in all_categories} for wk in week_keys}
    for r in records:
        wk  = week_key(r)
        cat = r["category"] or "General Control"
        if wk in cat_per_week and cat in cat_per_week[wk]:
            cat_per_week[wk][cat] += 1

    sources = ["linkedin", "indeed", "other"]
    src_per_week = {wk: {s: 0 for s in sources} for wk in week_keys}
    for r in accepted:
        wk  = week_key(r)
        src = (r["site"] or "other").lower()
        if src not in sources:
            src = "other"
        src_per_week[wk][src] += 1

    company_counts: dict[str, int] = {}
    for r in accepted:
        c = (r["company"] or "Unknown").strip()
        company_counts[c] = company_counts.get(c, 0) + 1
    top_companies = sorted(company_counts.items(), key=lambda x: x[1], reverse=True)[:20]

    ratio_per_week = []
    for wk in week_keys:
        acc = jobs_per_week[wk]
        rej = rejected_per_week[wk]
        total = acc + rej
        ratio_per_week.append({
            "week":     wk,
            "accepted": acc,
            "rejected": rej,
            "total":    total,
            "accept_pct": round(acc / total * 100, 1) if total else 0,
        })

    weekly_accepted_list = [jobs_per_week[wk] for wk in week_keys]
    rolling_avg = []
    for i in range(len(weekly_accepted_list)):
        window = weekly_accepted_list[max(0, i - 3): i + 1]
        rolling_avg.append(round(sum(window) / len(window), 2))

    fit_sum   = {wk: 0.0 for wk in week_keys}
    fit_count = {wk: 0   for wk in week_keys}
    for r in accepted:
        wk = week_key(r)
        fit_sum[wk]   += r["fit_score"] or 0
        fit_count[wk] += 1
    avg_fit_per_week = [
        round(fit_sum[wk] / fit_count[wk], 2) if fit_count[wk] else 0
        for wk in week_keys
    ]

    target_per_week = {wk: 0 for wk in week_keys}
    for r in accepted:
        if r["is_target"]:
            target_per_week[week_key(r)] += 1

    total_accept = len(accepted)
    total_reject = len(rejected)
    avg_fit_all = (
        round(sum(r["fit_score"] or 0 for r in accepted) / total_accept, 2)
        if total_accept else 0
    )

    return {
        "jobs_per_week":     [jobs_per_week[wk] for wk in week_keys],
        "rejected_per_week": [rejected_per_week[wk] for wk in week_keys],
        "rolling_avg":       rolling_avg,
        "avg_fit_per_week":  avg_fit_per_week,
        "target_per_week":   [target_per_week[wk] for wk in week_keys],
        "cat_per_week":      {cat: [cat_per_week[wk][cat] for wk in week_keys] for cat in all_categories},
        "src_per_week":      {src: [src_per_week[wk][src] for wk in week_keys] for src in sources},
        "top_companies":     [{"company": c, "count": n} for c, n in top_companies],
        "ratio_per_week":    ratio_per_week,
        "summary": {
            "total_seen":     len(records),
            "total_accepted": total_accept,
            "total_rejected": total_reject,
            "avg_fit":        avg_fit_all,
        },
    }


def _load_analytics_data() -> dict:
    """
    Read jobs.db and produce a dict of pre-aggregated analytics payloads
    that will be embedded as JSON into analytics.html.
    Returns an empty-safe dict even if the DB does not yet exist.

    Returns two payloads under keys 'all' and 'control_theory' so the
    UI can toggle between them without re-fetching data.
    """
    if not os.path.exists(DB_PATH):
        return {}

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    rows = conn.execute(
        "SELECT * FROM jobs ORDER BY run_date, year, week_number"
    ).fetchall()
    conn.close()

    if not rows:
        return {}

    records = [dict(r) for r in rows]

    # Build sorted unique list of "YYYY-Www" strings — shared across both views
    week_keys: list[str] = sorted(
        set(f"{r['year']}-W{r['week_number']:02d}" for r in records)
    )

    # Full payload — all records
    full = _aggregate_records(records, week_keys)

    # Control-theory payload — only records whose category is in the control-theory set.
    # We keep rejected jobs out of this filter regardless of their category labels —
    # the toggle is about "show me only matched control-theory jobs".
    ct_records = [
        r for r in records
        if (r["category"] or "") in _CONTROL_THEORY_CATEGORIES and not r["rejection_reason"]
    ]
    # For the control-theory view, "rejected" loses meaning (we already filtered) —
    # the rejected/ratio series will be zero, which is correct.
    control = _aggregate_records(ct_records, week_keys)

    total_runs = len(set(r["run_date"] for r in records))
    last_run   = max(r["run_date"] for r in records)

    # Augment summary blocks with run-level metadata (only meaningful for the full view)
    full["summary"]["total_runs"] = total_runs
    full["summary"]["last_run"]   = last_run
    control["summary"]["total_runs"] = total_runs
    control["summary"]["last_run"]   = last_run

    return {
        "week_labels":    week_keys,
        "all":            full,
        "control_theory": control,
        "control_theory_categories": sorted(_CONTROL_THEORY_CATEGORIES),
    }


# ─────────────────────────────────────────────────────────────────
# ANALYTICS HTML GENERATOR
# ─────────────────────────────────────────────────────────────────

# Colours for the category stacked bar — must stay consistent with all_categories order.
_CATEGORY_COLORS = {
    "GNC/Aerospace":       "#1a3a5c",
    "High-Tech/Precision": "#2e86ab",
    "Robotics":            "#28a745",
    "General Control":     "#6c757d",
    "Blacklisted-PLC":     "#dc3545",
    "Blacklisted-Other":   "#fd7e14",
    "Rejected-Senior":     "#ffc107",
    "Rejected-No-Gate":    "#adb5bd",
}

_SOURCE_COLORS = {
    "linkedin": "#0a66c2",
    "indeed":   "#2e7d32",
    "other":    "#888888",
}


def generate_analytics():
    """
    Read jobs.db, build analytics payload, and write analytics.html.
    The page is fully static — all data is embedded as JSON so it works
    on GitHub Pages without any server-side rendering.
    """
    data = _load_analytics_data()
    data_json = json.dumps(data, ensure_ascii=False, indent=None)

    updated = datetime.now().strftime("%d %B %Y at %H:%M UTC")

    # Category and source colour maps — passed into the JS as JSON so the
    # client can build datasets dynamically after range-slicing.
    cat_colors_json = json.dumps(_CATEGORY_COLORS)
    src_colors_json = json.dumps(_SOURCE_COLORS)

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Job Search Analytics — Control Systems NL</title>
<script src="https://cdn.jsdelivr.net/npm/chart.js@4.4.2/dist/chart.umd.min.js"></script>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{
  font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
  background: #f0f2f5; color: #2c3e50;
}}

header {{
  background: #1a3a5c; color: white; padding: 22px 32px;
  display: flex; justify-content: space-between; align-items: center;
  flex-wrap: wrap; gap: 10px;
}}
header h1 {{ font-size: 20px; font-weight: 600; }}
header p  {{ font-size: 12px; opacity: 0.65; margin-top: 4px; }}
.nav-link {{
  color: rgba(255,255,255,0.85); font-size: 13px; font-weight: 500;
  text-decoration: none; border: 1px solid rgba(255,255,255,0.4);
  padding: 6px 14px; border-radius: 6px;
}}
.nav-link:hover {{ background: rgba(255,255,255,0.15); }}

.summary-bar {{
  background: white; border-bottom: 1px solid #e0e0e0;
  padding: 14px 32px; display: flex; gap: 32px; flex-wrap: wrap;
  font-size: 13px; color: #666;
}}
.summary-bar strong {{ color: #1a3a5c; font-size: 22px; display: block; }}

.view-toggle {{
  background: white; border-bottom: 1px solid #e0e0e0;
  padding: 12px 32px; display: flex; gap: 14px; align-items: center;
  font-size: 13px; flex-wrap: wrap;
}}
.view-toggle label {{
  display: inline-flex; align-items: center; gap: 8px;
  cursor: pointer; user-select: none; font-weight: 500; color: #444;
}}
.view-toggle input[type="checkbox"] {{
  width: 16px; height: 16px; cursor: pointer; accent-color: #1a3a5c;
}}
.view-toggle .toggle-hint {{
  font-size: 12px; color: #888;
}}

.section {{
  padding: 24px 32px;
}}
.section-title {{
  font-size: 15px; font-weight: 600; color: #1a3a5c;
  margin-bottom: 16px; border-left: 3px solid #2e86ab;
  padding-left: 10px;
}}

.charts-grid {{
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(520px, 1fr));
  gap: 20px;
}}

.chart-card {{
  background: white; border-radius: 10px; padding: 20px 22px;
  border: 1px solid #e8e8e8;
}}
.chart-card h3 {{
  font-size: 13px; font-weight: 600; color: #555;
  margin-bottom: 14px;
}}
.chart-wrap {{
  position: relative; height: 260px;
}}
.chart-wide {{ grid-column: 1 / -1; }}
.chart-wide .chart-wrap {{ height: 280px; }}

table {{
  width: 100%; border-collapse: collapse; font-size: 13px;
}}
th {{
  background: #f8f9fa; text-align: left; padding: 8px 12px;
  font-weight: 600; color: #555; border-bottom: 2px solid #e8e8e8;
}}
td {{
  padding: 8px 12px; border-bottom: 1px solid #f0f0f0; color: #333;
}}
tr:hover td {{ background: #fafafa; }}

.bar-cell {{ display: flex; align-items: center; gap: 8px; }}
.mini-bar {{
  height: 8px; background: #e8e8e8; border-radius: 4px;
  flex: 1; min-width: 60px; max-width: 160px; overflow: hidden;
}}
.mini-bar-fill {{
  height: 100%; background: #1a3a5c; border-radius: 4px;
}}

.trend-badge {{
  display: inline-block; padding: 3px 9px; border-radius: 10px;
  font-size: 11px; font-weight: 600;
}}
.trend-up   {{ background: #d4edda; color: #155724; }}
.trend-down {{ background: #f8d7da; color: #721c24; }}
.trend-flat {{ background: #e2e3e5; color: #495057; }}

.no-data {{
  text-align: center; padding: 80px 0; color: #aaa; font-size: 15px;
}}

footer {{
  text-align: center; padding: 24px; font-size: 11px; color: #bbb;
}}

@media (max-width: 700px) {{
  .charts-grid {{ grid-template-columns: 1fr; }}
  .section {{ padding: 16px; }}
  header, .summary-bar {{ padding-left: 16px; padding-right: 16px; }}
}}
</style>
</head>
<body>

<header>
  <div>
    <h1>Job Search Analytics — Control Systems NL</h1>
    <p>Updated: {updated} &nbsp;|&nbsp; Data from jobs.db — all scrape runs accumulated</p>
  </div>
  <div style="display:flex; gap:8px; flex-wrap:wrap;">
    <a class="nav-link" href="index.html">Live Job Board</a>
    <a class="nav-link" href="settings.html">&#9881; Settings</a>
  </div>
</header>

<div class="summary-bar" id="summaryBar">
  <!-- Populated by JS -->
  <div class="no-data">No data yet — run job_searcher.py --site to populate.</div>
</div>

<div class="view-toggle">
  <label>
    <input type="checkbox" id="controlTheoryToggle" onchange="onViewToggle()">
    Show only control-theory jobs
  </label>
  <span class="toggle-hint" id="toggleHint">
    Filters all charts to matched jobs categorised as GNC/Aerospace, High-Tech/Precision, or General Control.
  </span>
</div>

<div class="section">
  <div class="section-title">Market Volume Over Time</div>
  <div class="charts-grid">

    <div class="chart-card chart-wide">
      <h3>Jobs found per week (matched vs rejected)</h3>
      <div class="chart-wrap">
        <canvas id="chartWeekly"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3>Breakdown by job category per week</h3>
      <div class="chart-wrap">
        <canvas id="chartCategory"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3>Source: LinkedIn vs Indeed per week</h3>
      <div class="chart-wrap">
        <canvas id="chartSource"></canvas>
      </div>
    </div>

  </div>
</div>

<div class="section">
  <div class="section-title">Quality &amp; Trend Signals</div>
  <div class="charts-grid">

    <div class="chart-card">
      <h3>Rolling 4-week average (matched jobs) — is the market improving?</h3>
      <div class="chart-wrap">
        <canvas id="chartTrend"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3>Average fit score per week</h3>
      <div class="chart-wrap">
        <canvas id="chartFit"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3>Target company appearances per week</h3>
      <div class="chart-wrap">
        <canvas id="chartTarget"></canvas>
      </div>
    </div>

    <div class="chart-card">
      <h3>Match rate: accepted / (accepted + rejected) per week</h3>
      <div class="chart-wrap">
        <canvas id="chartRatio"></canvas>
      </div>
    </div>

  </div>
</div>

<div class="section">
  <div class="section-title">Top Companies in Results</div>
  <div style="background:white; border-radius:10px; border:1px solid #e8e8e8; overflow:hidden;">
    <table id="companyTable">
      <thead>
        <tr>
          <th>#</th>
          <th>Company</th>
          <th>Appearances</th>
          <th>Share</th>
        </tr>
      </thead>
      <tbody id="companyTbody">
        <tr><td colspan="4" class="no-data">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<div class="section">
  <div class="section-title">Weekly Breakdown Table</div>
  <div style="background:white; border-radius:10px; border:1px solid #e8e8e8; overflow:hidden; overflow-x:auto;">
    <table id="weekTable">
      <thead>
        <tr>
          <th>Week</th>
          <th>Matched</th>
          <th>Rejected</th>
          <th>Match Rate</th>
          <th>Avg Fit</th>
          <th>Target Co.</th>
          <th>Trend</th>
        </tr>
      </thead>
      <tbody id="weekTbody">
        <tr><td colspan="7" class="no-data">Loading...</td></tr>
      </tbody>
    </table>
  </div>
</div>

<footer>
  Auto-generated by job_searcher.py &nbsp;&middot;&nbsp;
  Charts: Chart.js (CDN) &nbsp;&middot;&nbsp;
  Data: accumulated SQLite runs
</footer>

<script>
// ── Embedded analytics payload ────────────────────────────────────
// DATA has shape: {{ week_labels, all: {{...}}, control_theory: {{...}} }}
const DATA = {data_json};

// ── View state ────────────────────────────────────────────────────
// VIEW always points to the active aggregation (either DATA.all or DATA.control_theory).
// Charts read from VIEW.<field>; toggling the checkbox rebuilds with the other view.
let VIEW = (DATA && DATA.all) ? DATA.all : null;

// ── Guard: no data yet ────────────────────────────────────────────
const hasData = !!(DATA && DATA.week_labels && DATA.week_labels.length > 0 && VIEW);

// ── Chart instance registry (so we can destroy + rebuild on toggle) ──
const CHARTS = {{}};

function destroyAllCharts() {{
  for (const key in CHARTS) {{
    if (CHARTS[key]) {{ CHARTS[key].destroy(); CHARTS[key] = null; }}
  }}
}}

// ── Summary bar ───────────────────────────────────────────────────
function renderSummary() {{
  if (!hasData) return;
  const s   = VIEW.summary;
  const bar = document.getElementById('summaryBar');
  const isControl = document.getElementById('controlTheoryToggle').checked;
  const seenLabel = isControl ? 'control-theory jobs seen' : 'total jobs seen';
  const rejBlock  = isControl
    ? ''
    : `<div><strong>${{s.total_rejected}}</strong> rejected / blacklisted</div>`;
  bar.innerHTML = `
    <div><strong>${{s.total_runs}}</strong> scrape runs</div>
    <div><strong>${{s.total_seen}}</strong> ${{seenLabel}}</div>
    <div><strong>${{s.total_accepted}}</strong> matched</div>
    ${{rejBlock}}
    <div><strong>${{s.avg_fit}}</strong> avg fit score</div>
    <div style="font-size:12px; color:#aaa; align-self:center;">Last run: ${{s.last_run}}</div>
  `;
}}

// ── Chart defaults ────────────────────────────────────────────────
Chart.defaults.font.family = '-apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif';
Chart.defaults.font.size   = 11;
Chart.defaults.color       = '#666';
const GRID_COLOR = 'rgba(0,0,0,0.06)';

function baseScales(stacked) {{
  return {{
    x: {{
      stacked: stacked || false,
      grid: {{ color: GRID_COLOR }},
      ticks: {{ maxRotation: 45 }},
    }},
    y: {{
      stacked: stacked || false,
      grid: {{ color: GRID_COLOR }},
      beginAtZero: true,
    }},
  }};
}}

// ── Chart 1: Weekly matched + rejected bar ───────────────────────
function buildChartWeekly() {{
  if (!hasData) return;
  CHARTS.weekly = new Chart(document.getElementById('chartWeekly'), {{
    type: 'bar',
    data: {{
      labels: DATA.week_labels,
      datasets: [
        {{
          label: 'Matched',
          data: VIEW.jobs_per_week,
          backgroundColor: '#2e86ab',
        }},
        {{
          label: 'Rejected / Blacklisted',
          data: VIEW.rejected_per_week,
          backgroundColor: '#dc3545aa',
        }},
      ],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart 2: Category stacked bar ────────────────────────────────
function buildChartCategory() {{
  if (!hasData) return;
  const datasets = [
    {cat_datasets_js}
  ];
  CHARTS.category = new Chart(document.getElementById('chartCategory'), {{
    type: 'bar',
    data: {{ labels: DATA.week_labels, datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'bottom', labels: {{ boxWidth: 10, font: {{ size: 10 }} }} }} }},
      scales: baseScales(true),
    }},
  }});
}}

// ── Chart 3: Source stacked bar ───────────────────────────────────
function buildChartSource() {{
  if (!hasData) return;
  const datasets = [
    {src_datasets_js}
  ];
  CHARTS.source = new Chart(document.getElementById('chartSource'), {{
    type: 'bar',
    data: {{ labels: DATA.week_labels, datasets }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(true),
    }},
  }});
}}

// ── Chart 4: Rolling trend line ───────────────────────────────────
function buildChartTrend() {{
  if (!hasData) return;
  CHARTS.trend = new Chart(document.getElementById('chartTrend'), {{
    type: 'line',
    data: {{
      labels: DATA.week_labels,
      datasets: [
        {{
          label: 'Weekly matched',
          data: VIEW.jobs_per_week,
          borderColor: '#2e86ab44',
          backgroundColor: '#2e86ab11',
          borderWidth: 1,
          pointRadius: 2,
          fill: true,
          tension: 0.3,
        }},
        {{
          label: '4-week rolling avg',
          data: VIEW.rolling_avg,
          borderColor: '#1a3a5c',
          backgroundColor: 'transparent',
          borderWidth: 2.5,
          pointRadius: 3,
          fill: false,
          tension: 0.3,
        }},
      ],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart 5: Avg fit score line ───────────────────────────────────
function buildChartFit() {{
  if (!hasData) return;
  CHARTS.fit = new Chart(document.getElementById('chartFit'), {{
    type: 'line',
    data: {{
      labels: DATA.week_labels,
      datasets: [{{
        label: 'Avg fit score (matched)',
        data: VIEW.avg_fit_per_week,
        borderColor: '#28a745',
        backgroundColor: '#28a74511',
        borderWidth: 2,
        pointRadius: 3,
        fill: true,
        tension: 0.3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: {{
        ...baseScales(false),
        y: {{ ...baseScales(false).y, min: 0 }},
      }},
    }},
  }});
}}

// ── Chart 6: Target company count per week ───────────────────────
function buildChartTarget() {{
  if (!hasData) return;
  CHARTS.target = new Chart(document.getElementById('chartTarget'), {{
    type: 'bar',
    data: {{
      labels: DATA.week_labels,
      datasets: [{{
        label: 'Target company matches',
        data: VIEW.target_per_week,
        backgroundColor: '#f39c12',
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: baseScales(false),
    }},
  }});
}}

// ── Chart 7: Match rate line ──────────────────────────────────────
function buildChartRatio() {{
  if (!hasData) return;
  const pcts = VIEW.ratio_per_week.map(r => r.accept_pct);
  CHARTS.ratio = new Chart(document.getElementById('chartRatio'), {{
    type: 'line',
    data: {{
      labels: DATA.week_labels,
      datasets: [{{
        label: 'Match rate (%)',
        data: pcts,
        borderColor: '#856404',
        backgroundColor: '#ffc10711',
        borderWidth: 2,
        pointRadius: 3,
        fill: true,
        tension: 0.3,
      }}],
    }},
    options: {{
      responsive: true, maintainAspectRatio: false,
      plugins: {{ legend: {{ position: 'top' }} }},
      scales: {{
        ...baseScales(false),
        y: {{ ...baseScales(false).y, min: 0, max: 100,
               ticks: {{ callback: v => v + '%' }} }},
      }},
    }},
  }});
}}

// ── Top companies table ───────────────────────────────────────────
function buildCompanyTable() {{
  if (!hasData) return;
  const tbody    = document.getElementById('companyTbody');
  if (!VIEW.top_companies.length) {{
    tbody.innerHTML = '<tr><td colspan="4" class="no-data">No companies in this view.</td></tr>';
    return;
  }}
  const maxCount = VIEW.top_companies[0].count;
  tbody.innerHTML = VIEW.top_companies.map((row, i) => {{
    const pct     = maxCount ? Math.round(row.count / maxCount * 100) : 0;
    const totalAcc = VIEW.summary.total_accepted;
    const share   = totalAcc ? ((row.count / totalAcc) * 100).toFixed(1) + '%' : '—';
    return `<tr>
      <td>${{i + 1}}</td>
      <td>${{row.company}}</td>
      <td>
        <div class="bar-cell">
          <span>${{row.count}}</span>
          <div class="mini-bar">
            <div class="mini-bar-fill" style="width:${{pct}}%"></div>
          </div>
        </div>
      </td>
      <td>${{share}}</td>
    </tr>`;
  }}).join('');
}}

// ── Weekly breakdown table ────────────────────────────────────────
function buildWeekTable() {{
  if (!hasData) return;
  const tbody   = document.getElementById('weekTbody');
  const n       = DATA.week_labels.length;
  const rows    = [];
  for (let i = n - 1; i >= 0; i--) {{   // newest first
    const matched  = VIEW.jobs_per_week[i];
    const rejected = VIEW.rejected_per_week[i];
    const total    = matched + rejected;
    const matchPct = total ? ((matched / total) * 100).toFixed(0) + '%' : '—';
    const avgFit   = VIEW.avg_fit_per_week[i];
    const target   = VIEW.target_per_week[i];
    const rolling  = VIEW.rolling_avg[i];

    // Trend: compare rolling avg to previous week's rolling avg
    let trendBadge = '';
    if (i > 0) {{
      const prev = VIEW.rolling_avg[i - 1];
      const delta = rolling - prev;
      if (delta > 0.3)       trendBadge = '<span class="trend-badge trend-up">improving</span>';
      else if (delta < -0.3) trendBadge = '<span class="trend-badge trend-down">declining</span>';
      else                   trendBadge = '<span class="trend-badge trend-flat">stable</span>';
    }}

    rows.push(`<tr>
      <td>${{DATA.week_labels[i]}}</td>
      <td>${{matched}}</td>
      <td>${{rejected}}</td>
      <td>${{matchPct}}</td>
      <td>${{avgFit || '—'}}</td>
      <td>${{target}}</td>
      <td>${{trendBadge}}</td>
    </tr>`);
  }}
  tbody.innerHTML = rows.join('');
}}

// ── Render everything (called on boot + on toggle) ────────────────
function renderAll() {{
  renderSummary();
  buildChartWeekly();
  buildChartCategory();
  buildChartSource();
  buildChartTrend();
  buildChartFit();
  buildChartTarget();
  buildChartRatio();
  buildCompanyTable();
  buildWeekTable();
}}

// ── Toggle handler: swap active view + rebuild ───────────────────
function onViewToggle() {{
  if (!hasData) return;
  const isControl = document.getElementById('controlTheoryToggle').checked;
  VIEW = isControl ? DATA.control_theory : DATA.all;
  destroyAllCharts();
  renderAll();
}}

// ── Boot ──────────────────────────────────────────────────────────
renderAll();
</script>
</body>
</html>"""

    out_path = os.path.join(OUTPUT_DIR, "analytics.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"[OK] Analytics page generated: {out_path}")


# ─────────────────────────────────────────────────────────────────
# OUTPUT HELPERS
# ─────────────────────────────────────────────────────────────────

def _format_date(row) -> str:
    """Format date_posted safely."""
    try:
        col = row.get("date_posted") or row.get("date") or ""
        if pd.isna(col) or col == "":
            return "Date unknown"
        if hasattr(col, "strftime"):
            return col.strftime("%d %b %Y")
        return str(col)[:10]
    except Exception:
        return "Date unknown"


def _score_bar(score: float, width: int = 10) -> str:
    """Return a simple ASCII progress bar for fit score."""
    filled = round(score / _MAX_POSSIBLE_SCORE * width)
    filled = max(0, min(filled, width))
    return "[" + "#" * filled + "-" * (width - filled) + f"] {score:.1f}"


# ─────────────────────────────────────────────────────────────────
# TERMINAL OUTPUT
# ─────────────────────────────────────────────────────────────────

def print_results(df: pd.DataFrame):
    """Print ranked numbered list to terminal."""
    print(f"\n{'='*68}")
    print(f"  CONTROL SYSTEMS JOB RESULTS — {datetime.now().strftime('%Y-%m-%d %H:%M')}")
    print(f"  Indeed NL + LinkedIn | Fresh grad filter ON | Blacklist: PLC, SCADA, HMI...")
    print(f"{'='*68}\n")

    if df.empty:
        print("  No relevant jobs found. Try again tomorrow.")
        return

    for i, (_, row) in enumerate(df.iterrows(), 1):
        star     = "* " if row.get("is_target") else "  "
        title    = str(row.get("title",    "Unknown title"))
        company  = str(row.get("company",  "Unknown company"))
        location = str(row.get("location", "Unknown location"))
        site     = str(row.get("site",     "")).upper()
        link     = str(row.get("job_url",  "No link"))
        date     = _format_date(row)
        score    = float(row.get("fit_score", 0))
        keywords = ", ".join(row.get("matched_keywords", [])[:6])
        bar      = _score_bar(score)

        print(f"{i:>3}. {star}{title}")
        print(f"       {company} | {location} | {site}")
        print(f"       Fit: {bar}")
        print(f"       Date: {date}")
        print(f"       {link}")
        print(f"       Keywords: {keywords}")
        print()


# ─────────────────────────────────────────────────────────────────
# EMAIL
# ─────────────────────────────────────────────────────────────────

def _score_color(score: float) -> str:
    if score >= 0.4 * _MAX_POSSIBLE_SCORE:
        return "#1e7e34"  # green
    if score >= 0.2 * _MAX_POSSIBLE_SCORE:
        return "#856404"  # amber
    return "#721c24"      # red


def build_email_html(df: pd.DataFrame) -> str:
    date_str  = datetime.now().strftime("%Y-%m-%d")
    target_n  = int(df["is_target"].sum()) if not df.empty else 0
    high_fit  = int((df["fit_score"] >= 0.4 * _MAX_POSSIBLE_SCORE).sum()) if not df.empty else 0

    html = f"""
<html><body style="font-family: Arial, sans-serif; max-width: 700px; margin: 0 auto;">
<h2 style="color:#1a3a5c;">Control Systems Job Alert — {date_str}</h2>
<p>Found <b>{len(df)}</b> relevant jobs &nbsp;|&nbsp;
   <b>{target_n}</b> target companies &nbsp;|&nbsp;
   <b>{high_fit}</b> high fit (score &ge; {0.4 * _MAX_POSSIBLE_SCORE:.0f})
</p>
<p style="color:#888; font-size:12px;">
  Filters: fresh grad (no senior/lead), PLC/SCADA/HMI blacklisted,
  gate requires MATLAB/Simulink/control/robotics signal.
  Score = raw weighted keyword depth (max {_MAX_POSSIBLE_SCORE:.0f}).
</p>
<hr>
"""

    if df.empty:
        html += "<p>No new relevant jobs found today.</p>"
    else:
        for i, (_, row) in enumerate(df.iterrows(), 1):
            is_target = bool(row.get("is_target", False))
            score     = float(row.get("fit_score", 0))
            border    = "#f39c12" if is_target else "#cccccc"
            star      = "* " if is_target else ""
            title     = str(row.get("title",    "Unknown"))
            company   = str(row.get("company",  "Unknown"))
            location  = str(row.get("location", ""))
            site      = str(row.get("site",     "")).upper()
            link      = str(row.get("job_url",  "#"))
            date      = _format_date(row)
            keywords  = ", ".join(row.get("matched_keywords", [])[:6])
            summary   = str(row.get("description", ""))[:250].strip()
            if summary:
                summary += "..."
            score_col = _score_color(score)

            html += f"""
<div style="border-left:4px solid {border}; padding:10px 15px;
            margin:14px 0; background:#f9f9f9; border-radius:4px;">
  <div style="font-size:11px; color:#888;">#{i} &mdash; {site} &mdash; {date}</div>
  <h3 style="margin:4px 0; color:#1a3a5c; font-size:16px;">{star}{title}</h3>
  <p style="margin:3px 0; font-weight:bold; font-size:14px;">{company}</p>
  <p style="margin:3px 0; color:#555; font-size:13px;">{location}</p>
  <p style="margin:6px 0; font-size:13px;">
    Fit score: <b style="color:{score_col};">{score}</b>
  </p>
  <p style="margin:3px 0; color:#555; font-size:12px;">{summary}</p>
  <p style="margin:3px 0; font-size:12px; color:#888;">Keywords: {keywords}</p>
  <a href="{link}" style="color:#2980b9; font-size:13px; font-weight:500;">View posting</a>
</div>
"""

    html += """
<hr>
<p style="color:#aaa; font-size:11px;">
  Generated by job_searcher.py &nbsp;|&nbsp;
  Edit SCORED_KEYWORDS / BLACKLIST_TITLE / TARGET_COMPANIES to tune.
</p>
</body></html>
"""
    return html


def send_email(df: pd.DataFrame):
    if EMAIL_SENDER == "your_gmail@gmail.com":
        print("\n[!] Email not configured — set EMAIL_SENDER and EMAIL_PASSWORD env vars.")
        return

    target_n = int(df["is_target"].sum()) if not df.empty else 0
    subject  = (
        f"Control Jobs NL — {len(df)} results, {target_n} target "
        f"({datetime.now().strftime('%Y-%m-%d')})"
    )
    msg = MIMEMultipart("alternative")
    msg["Subject"] = subject
    msg["From"]    = EMAIL_SENDER
    msg["To"]      = EMAIL_RECEIVER
    msg.attach(MIMEText(build_email_html(df), "html"))

    try:
        with smtplib.SMTP_SSL("smtp.gmail.com", 465) as server:
            server.login(EMAIL_SENDER, EMAIL_PASSWORD)
            server.sendmail(EMAIL_SENDER, EMAIL_RECEIVER, msg.as_string())
        print(f"\n[OK] Email sent to {EMAIL_RECEIVER}")
    except Exception as exc:
        print(f"\n[!] Email failed: {exc}")


# ─────────────────────────────────────────────────────────────────
# CSV SAVE
# ─────────────────────────────────────────────────────────────────

def save_results(df: pd.DataFrame):
    if df.empty:
        return
    cols_wanted = [
        "title", "company", "location", "site",
        "fit_score", "is_target", "date_posted",
        "job_url", "matched_keywords",
    ]
    cols = [c for c in cols_wanted if c in df.columns]
    filename = os.path.join(OUTPUT_DIR, f"jobs_{datetime.now().strftime('%Y%m%d')}.csv")
    df[cols].to_csv(filename, index=False, encoding="utf-8-sig")
    print(f"\n[OK] Saved {len(df)} jobs to {filename}")


# ─────────────────────────────────────────────────────────────────
# GITHUB PAGES SITE
# ─────────────────────────────────────────────────────────────────

def generate_site(df: pd.DataFrame):
    """Generate a self-contained index.html for GitHub Pages."""
    updated   = datetime.now().strftime("%d %B %Y at %H:%M UTC")
    total     = len(df)
    target_n  = int(df["is_target"].sum()) if not df.empty else 0
    high_fit  = int((df["fit_score"] >= 0.4 * _MAX_POSSIBLE_SCORE).sum()) if not df.empty else 0

    cards_html = ""
    if df.empty:
        cards_html = '<div class="empty"><p>No relevant jobs found today. Check back tomorrow.</p></div>'
    else:
        for _, row in df.iterrows():
            is_target  = bool(row.get("is_target", False))
            score      = float(row.get("fit_score", 0))
            title      = str(row.get("title",    "Unknown"))
            company    = str(row.get("company",  "Unknown"))
            location   = str(row.get("location", ""))
            site_name  = str(row.get("site",     "")).lower()
            link       = str(row.get("job_url",  "#"))
            date       = _format_date(row)
            keywords   = ", ".join(row.get("matched_keywords", [])[:6])
            summary    = str(row.get("description", ""))[:280].strip()
            if summary:
                summary += "..."

            # Salary — surfaced if jobspy returns it
            salary_str = ""
            if row.get("min_amount") and row.get("max_amount"):
                currency = str(row.get("currency", "EUR"))
                salary_str = f"{currency} {int(row['min_amount']):,} – {int(row['max_amount']):,}"
            elif row.get("min_amount"):
                currency = str(row.get("currency", "EUR"))
                salary_str = f"From {currency} {int(row['min_amount']):,}"

            # Score badge colour
            if score >= 0.4 * _MAX_POSSIBLE_SCORE:
                score_cls = "score-high"
            elif score >= 0.2 * _MAX_POSSIBLE_SCORE:
                score_cls = "score-mid"
            else:
                score_cls = "score-low"

            # Score bar (HTML) — width as % of max possible raw score
            bar_pct = min(score / _MAX_POSSIBLE_SCORE * 100, 100)
            bar_html = (
                f'<div class="score-bar">'
                f'<div class="bar-fill {score_cls}" style="width:{bar_pct:.1f}%"></div>'
                f'</div>'
            )

            star_badge   = '<span class="badge badge-target">Target company</span>' if is_target else ""
            site_badge   = f'<span class="badge badge-{site_name}">{site_name.upper()}</span>'
            salary_badge = f'<span class="badge badge-salary">{salary_str}</span>' if salary_str else ""
            card_class   = "card target-card" if is_target else "card"

            cards_html += f"""
<div class="{card_class}" data-score="{score}" data-site="{site_name}" data-target="{str(is_target).lower()}">
  <div class="card-header">
    <h3><a href="{link}" target="_blank" rel="noopener">{title}</a></h3>
    <div class="badges">{star_badge}{site_badge}{salary_badge}</div>
  </div>
  <div class="card-meta">
    <span>{company}</span>
    <span>{location}</span>
    <span>{date}</span>
  </div>
  <div class="score-row">
    <span class="score-label">Fit:</span>
    <span class="score-num {score_cls}">{score}</span>
    {bar_html}
  </div>
  <p class="summary">{summary}</p>
  <div class="keywords">Keywords: {keywords}</div>
  <a class="apply-btn" href="{link}" target="_blank" rel="noopener">View posting</a>
</div>"""

    html = f"""<!DOCTYPE html>
<html lang="en">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>Control Systems Jobs NL</title>
<style>
* {{ box-sizing: border-box; margin: 0; padding: 0; }}
body {{ font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, sans-serif;
       background: #f0f2f5; color: #2c3e50; }}

header {{ background: #1a3a5c; color: white; padding: 22px 32px;
          display: flex; justify-content: space-between; align-items: center;
          flex-wrap: wrap; gap: 10px; }}
header h1 {{ font-size: 21px; font-weight: 600; }}
header p  {{ font-size: 12px; opacity: 0.65; margin-top: 5px; }}
.nav-link {{
  color: rgba(255,255,255,0.85); font-size: 13px; font-weight: 500;
  text-decoration: none; border: 1px solid rgba(255,255,255,0.4);
  padding: 6px 14px; border-radius: 6px; white-space: nowrap;
}}
.nav-link:hover {{ background: rgba(255,255,255,0.15); }}

.stats {{
  background: white; border-bottom: 1px solid #e0e0e0;
  padding: 12px 32px; font-size: 13px; color: #666;
  display: flex; gap: 28px; align-items: center; flex-wrap: wrap;
}}
.stats strong {{ color: #1a3a5c; font-size: 20px; }}

.controls {{
  padding: 14px 32px; display: flex; gap: 10px; flex-wrap: wrap; align-items: center;
  background: white; border-bottom: 1px solid #eee;
}}
.controls input {{
  padding: 8px 14px; border: 1px solid #ccc; border-radius: 6px;
  font-size: 13px; width: 260px; outline: none;
}}
.controls input:focus {{ border-color: #1a3a5c; }}
.filter-btn {{
  padding: 7px 14px; border: 1px solid #ccc; border-radius: 6px;
  background: white; cursor: pointer; font-size: 12px; font-weight: 500;
}}
.filter-btn.active {{ background: #1a3a5c; color: white; border-color: #1a3a5c; }}
.sort-label {{ font-size: 12px; color: #888; margin-left: 8px; }}

.grid {{
  padding: 20px 32px 40px;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(380px, 1fr));
  gap: 16px;
}}

.card {{
  background: white; border-radius: 10px; padding: 18px 20px;
  border: 1px solid #e8e8e8; transition: box-shadow 0.2s;
  display: flex; flex-direction: column; gap: 8px;
}}
.card:hover {{ box-shadow: 0 4px 18px rgba(0,0,0,0.09); }}
.target-card {{ border-left: 4px solid #f39c12; }}

.card-header {{
  display: flex; justify-content: space-between;
  align-items: flex-start; gap: 8px;
}}
.card-header h3 {{ font-size: 14px; font-weight: 600; line-height: 1.35; }}
.card-header h3 a {{ color: #1a3a5c; text-decoration: none; }}
.card-header h3 a:hover {{ text-decoration: underline; }}
.badges {{ display: flex; flex-direction: column; gap: 3px; flex-shrink: 0; align-items: flex-end; }}

.badge {{
  font-size: 10px; padding: 2px 7px; border-radius: 10px;
  white-space: nowrap; font-weight: 500;
}}
.badge-target  {{ background: #fef3cd; color: #856404; }}
.badge-linkedin {{ background: #e8f4fd; color: #0a66c2; }}
.badge-indeed   {{ background: #e8f8f0; color: #2e7d32; }}
.badge-salary   {{ background: #f3e8ff; color: #6b21a8; }}

.card-meta {{
  display: flex; flex-wrap: wrap; gap: 10px;
  font-size: 12px; color: #666;
}}

.score-row {{
  display: flex; align-items: center; gap: 8px; font-size: 12px;
}}
.score-label {{ color: #888; flex-shrink: 0; }}
.score-num   {{ font-weight: 700; flex-shrink: 0; min-width: 36px; }}
.score-high  {{ color: #1e7e34; }}
.score-mid   {{ color: #856404; }}
.score-low   {{ color: #721c24; }}

.score-bar {{
  flex: 1; height: 6px; background: #e8e8e8;
  border-radius: 4px; overflow: hidden;
}}
.bar-fill {{
  height: 100%; border-radius: 4px;
  transition: width 0.3s;
}}
.bar-fill.score-high {{ background: #28a745; }}
.bar-fill.score-mid  {{ background: #ffc107; }}
.bar-fill.score-low  {{ background: #dc3545; }}

.summary  {{ font-size: 12px; color: #555; line-height: 1.5; }}
.keywords {{ font-size: 11px; color: #999; }}

.apply-btn {{
  display: inline-block; margin-top: 4px;
  padding: 7px 16px; background: #1a3a5c;
  color: white; border-radius: 6px; font-size: 12px;
  text-decoration: none; font-weight: 500; align-self: flex-start;
}}
.apply-btn:hover {{ background: #2e6da4; }}

.empty {{ text-align: center; padding: 60px; color: #888; font-size: 15px; }}
.hidden {{ display: none !important; }}

footer {{ text-align: center; padding: 24px; font-size: 11px; color: #bbb; }}

@media (max-width: 600px) {{
  .grid {{ padding: 12px 16px; grid-template-columns: 1fr; }}
  header, .stats, .controls {{ padding-left: 16px; padding-right: 16px; }}
}}
</style>
</head>
<body>

<header>
  <div>
    <h1>Control Systems Jobs — Netherlands</h1>
    <p>Updated: {updated} &nbsp;|&nbsp; Filters: no senior/lead, no PLC/SCADA/HMI, gate requires MATLAB/control/robotics signal</p>
  </div>
  <div style="display:flex; gap:8px; flex-wrap:wrap;">
    <a class="nav-link" href="analytics.html">Analytics Dashboard</a>
    <a class="nav-link" href="settings.html">&#9881; Settings</a>
  </div>
</header>

<div class="stats">
  <div><strong>{total}</strong> jobs found</div>
  <div><strong>{target_n}</strong> target companies</div>
  <div><strong>{high_fit}</strong> high fit (score &ge; {0.4 * _MAX_POSSIBLE_SCORE:.0f})</div>
  <div style="font-size:12px;">Sources: Indeed NL + LinkedIn</div>
</div>

<div class="controls">
  <input type="text" id="searchBox" placeholder="Filter by title, company, keyword..."
         oninput="applyFilters()">
  <input type="number" id="minScoreBox" placeholder="Min score" min="0" step="1"
         title="Show only jobs with fit score >= this value"
         oninput="applyFilters()"
         style="width: 110px;">
  <button class="filter-btn active" onclick="filterSite('all', this)">All</button>
  <button class="filter-btn" onclick="filterSite('linkedin', this)">LinkedIn</button>
  <button class="filter-btn" onclick="filterSite('indeed', this)">Indeed</button>
  <button class="filter-btn" onclick="filterTarget(this)">Target only</button>
  <span class="sort-label">Sort:</span>
  <button class="filter-btn active" onclick="sortCards('score', this)">Fit score</button>
  <button class="filter-btn" onclick="sortCards('date', this)">Newest</button>
</div>

<div class="grid" id="jobGrid">
{cards_html}
</div>

<footer>
  Auto-generated by job_searcher.py &nbsp;&middot;&nbsp;
  Runs daily at 09:00 NL time via GitHub Actions &nbsp;&middot;&nbsp;
  Fit score = raw weighted keyword depth (max {_MAX_POSSIBLE_SCORE:.0f})
</footer>

<script>
  let activeSite   = 'all';
  let targetOnly   = false;
  let activeSort   = 'score';

  function applyFilters() {{
    const q = document.getElementById('searchBox').value.toLowerCase();
    const minScoreRaw = document.getElementById('minScoreBox').value;
    const minScore = minScoreRaw === '' ? null : parseFloat(minScoreRaw);
    document.querySelectorAll('.card').forEach(card => {{
      const text     = card.innerText.toLowerCase();
      const site     = card.dataset.site || '';
      const isTarget = card.dataset.target === 'true';
      const score    = parseFloat(card.dataset.score || '0');
      const matchText   = q === '' || text.includes(q);
      const matchSite   = activeSite === 'all' || site === activeSite;
      const matchTarget = !targetOnly || isTarget;
      const matchScore  = minScore === null || isNaN(minScore) || score >= minScore;
      card.classList.toggle('hidden', !(matchText && matchSite && matchTarget && matchScore));
    }});
  }}

  function filterSite(site, btn) {{
    activeSite = site;
    document.querySelectorAll('.filter-btn').forEach(b => {{
      if (['all','linkedin','indeed'].includes(b.innerText.toLowerCase()) ||
          b.innerText === 'All') b.classList.remove('active');
    }});
    btn.classList.add('active');
    applyFilters();
  }}

  function filterTarget(btn) {{
    targetOnly = !targetOnly;
    btn.classList.toggle('active', targetOnly);
    applyFilters();
  }}

  function sortCards(mode, btn) {{
    activeSort = mode;
    document.querySelectorAll('.sort-label + .filter-btn, .sort-label ~ .filter-btn')
      .forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    const grid  = document.getElementById('jobGrid');
    const cards = Array.from(grid.querySelectorAll('.card'));
    cards.sort((a, b) => {{
      if (mode === 'score') {{
        return parseFloat(b.dataset.score || 0) - parseFloat(a.dataset.score || 0);
      }}
      // 'date' sort — rely on DOM order (already sorted by date server-side)
      return 0;
    }});
    cards.forEach(c => grid.appendChild(c));
  }}
</script>
</body>
</html>"""

    out_path = os.path.join(OUTPUT_DIR, "index.html")
    with open(out_path, "w", encoding="utf-8") as f:
        f.write(html)
    print(f"\n[OK] Site generated: {out_path} ({total} jobs, {target_n} target, {high_fit} high-fit)")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────

def run_search(send_mail: bool = False, save: bool = False, make_site: bool = False):
    run_dt = datetime.now()
    print(f"\n[{run_dt.strftime('%Y-%m-%d %H:%M')}] Starting job search...")

    raw_df      = scrape_all()
    filtered_df = filter_jobs(raw_df)
    print_results(filtered_df)

    # Build set of accepted job IDs for the DB writer
    accepted_ids: set[str] = set()
    if not filtered_df.empty:
        for _, row in filtered_df.iterrows():
            title   = str(row.get("title",   "") or "")
            company = str(row.get("company", "") or "")
            url     = str(row.get("job_url", "") or "")
            accepted_ids.add(_job_id(title, company, url))

    # Always persist to DB (regardless of --site / --save flags)
    save_to_db(filtered_df, raw_df, accepted_ids, run_dt)

    if send_mail:
        send_email(filtered_df)
    if save:
        save_results(filtered_df)
    if make_site:
        generate_site(filtered_df)
        generate_analytics()


def run_daily(send_mail: bool = False):
    """Run once immediately, then schedule daily at 08:00 local time.
    NOTE: If you are using GitHub Actions, you do not need --daily.
    The workflow already handles scheduling via cron.
    """
    print("Daily scheduler started. Runs every day at 08:00 local time.")
    print("Press Ctrl+C to stop.\n")
    run_search(send_mail=send_mail, save=True, make_site=True)
    schedule.every().day.at("08:00").do(
        run_search, send_mail=send_mail, save=True, make_site=True
    )
    while True:
        schedule.run_pending()
        time.sleep(60)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Control Systems Job Searcher — Indeed NL + LinkedIn"
    )
    parser.add_argument("--email",     action="store_true", help="Send HTML email digest")
    parser.add_argument("--save",      action="store_true", help="Save results to CSV")
    parser.add_argument("--site",      action="store_true", help="Generate index.html + analytics.html for GitHub Pages")
    parser.add_argument("--daily",     action="store_true", help="Run now + schedule daily at 08:00")
    parser.add_argument("--analytics", action="store_true", help="Regenerate analytics.html from existing jobs.db (no scrape)")
    args = parser.parse_args()

    if args.analytics:
        # Regenerate analytics from existing DB without scraping
        generate_analytics()
    elif args.daily:
        run_daily(send_mail=args.email)
    else:
        run_search(send_mail=args.email, save=args.save, make_site=args.site)
