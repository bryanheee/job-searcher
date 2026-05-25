# Job Searcher — Control Systems NL (Indeed + LinkedIn)

Scrapes Indeed NL and LinkedIn daily, applies a multi-stage filter pipeline,
scores each job by keyword depth, and outputs a ranked list with fit scores.

---

## Setup

```
pip install python-jobspy pandas schedule
```

---

## Usage

| Command | What it does |
|---|---|
| `python job_searcher.py` | Search now, print to terminal |
| `python job_searcher.py --save` | Search + save to CSV |
| `python job_searcher.py --email` | Search + send email digest |
| `python job_searcher.py --site` | Search + generate index.html |
| `python job_searcher.py --site --save --email` | Full run (same as GitHub Actions) |
| `python job_searcher.py --daily` | Run now + repeat daily at 08:00 |

---

## Filter pipeline

Each job passes through four stages in order:

1. **Seniority filter** — removes jobs with "senior", "lead", "principal", etc. in the title,
   and titles containing "5+ years" patterns. Keeps entry/medior roles only.
2. **Blacklist (title)** — removes jobs whose *title* contains PLC, SCADA, HMI,
   field service, automation engineer, process engineer, HVAC, etc.
3. **Blacklist (full text)** — removes jobs mentioning ladder logic, TIA Portal,
   Siemens S7, oil & gas, building automation, etc. anywhere.
4. **Gate** — job must contain at least one weight-2 or weight-3 keyword
   (MATLAB, Simulink, control theory, mechatronics, robotics, etc.).
   Pure "automation" or "precision" mentions without a control/robotics signal are rejected.

---

## Fit score (0–10)

After filtering, each job is scored by weighted keyword depth:

| Weight | Examples |
|---|---|
| 3 | MPC, robust control, H-infinity, Kalman, LQR/LQG, nonlinear control, state-space, GNC, system identification, Python, PyTorch |
| 2 | MATLAB, Simulink, mechatronics, robotics, aerospace, servo, motion control, C++, ROS, dSpace, embedded control |
| 1 | PID, control, high-tech, precision, sensor fusion, navigation, signal processing, UAV, satellite |

Each keyword *group* contributes its weight at most once — a job with 20 weight-1
hits does not beat one with 2 weight-3 hits.

**Score = (sum of matched group weights / 6) * 10**

Jobs are sorted: target companies first, then by fit score descending.

---

## Output format (terminal)

```
  1. *  Control Systems Engineer
       Sioux Technologies | Eindhoven, NL | LINKEDIN
       Fit: [########--] 8.3/10
       Date: 25 May 2026
       https://linkedin.com/jobs/view/...
       Keywords: mpc, model predictive control, matlab, simulink, state-space

  2.    Mechatronics Engineer (Medior)
       Vanderlande | Veghel, NL | INDEED
       Fit: [######----] 6.7/10
       ...
```

`*` = target company (ASML, TMC, Sioux, NLR, Orange Aerospace, Demcon, etc.)

---

## Email setup (Gmail)

1. Go to myaccount.google.com > Security > App Passwords
2. Generate a 16-character app password (requires 2FA)
3. Set environment variables (or edit `job_searcher.py` directly):
   - `EMAIL_SENDER`   — your Gmail address
   - `EMAIL_PASSWORD` — the 16-character app password
4. `EMAIL_RECEIVER` defaults to oscarhe1998@gmail.com

---

## GitHub Pages (automated)

The included workflow (`.github/workflows/job_search.yml`) runs daily at 09:00
Netherlands time. It:

1. Runs `python job_searcher.py --site --save --email`
2. Copies `index.html` into a clean `_site/` directory
3. Deploys `_site/` to the `gh-pages` branch

Only `index.html` is published — source code and CSV files are never exposed.

Enable GitHub Pages in repo Settings > Pages > Deploy from `gh-pages` branch.

Required GitHub Secrets: `EMAIL_SENDER`, `EMAIL_PASSWORD`, `EMAIL_RECEIVER`.

---

## Customise in job_searcher.py

| Variable | Purpose |
|---|---|
| `SEARCH_QUERIES` | Search terms sent to Indeed + LinkedIn |
| `SCORED_KEYWORDS` | Tiered keyword weights (3/2/1) — core of the scoring |
| `GATE_KEYWORDS` | Derived from weight-2/3; job must match at least one |
| `BLACKLIST_TITLE` | Excluded if match is in the job *title* |
| `BLACKLIST_FULL` | Excluded if match appears anywhere in text |
| `SENIOR_TITLE_PATTERNS` | Regex patterns for seniority signals in title |
| `TARGET_COMPANIES` | Flagged with `*` and sorted to top |
| `RESULTS_PER_QUERY` | Results per query per site (default 30) |
| `HOURS_OLD` | Only jobs posted within this many hours (default 168 = 7 days) |
