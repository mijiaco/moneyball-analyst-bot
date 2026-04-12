# Agent guide — moneyball-discord-trade

Instructions for AI assistants and developers working in this repository.

## What this project does

- Polls a **MyFantasyLeague (MFL)** league via JSON **export** HTTP APIs.
- Posts **Discord** messages (embeds) for **trades** and optional **trade bait** updates.
- Persists **dedupe state** in `data/seen_trades.json` (and optional weekly report cursor in `data/reports_state.json`) so repeats are not announced.
- **Primary runtime:** GitHub Actions workflow `scheduled-export` running `python -m src.run_once` (no long-lived server required).
- **Optional:** `python -m src.bot` is a Discord gateway client for local/long-poll use; the maintainer typically relies on Actions instead.

Do **not** commit secrets, `.env`, or API keys. Never overwrite `.env` without explicit user approval.

---

## Repository layout (source)

| Path | Role |
|------|------|
| `src/run_once.py` | Single-shot poll + Discord REST posts + save dedupe; Actions entrypoint. |
| `src/trade_poll_core.py` | Shared fetch + build payloads for trades and trade bait. |
| `src/trade_notify.py` | Fingerprints, formatting, dry-run CLI (`python -m src.trade_notify`). |
| `src/mfl_client.py` | Async HTTP client for `…/export` endpoints; players cache; **`INCLUDE_DRAFT_PICKS`** on trade bait fetch so `willGiveUp` includes `DP_` / `FP_` tokens. |
| `src/mfl_env.py` | Requires `MFL_HOST`, `MFL_YEAR`, `MFL_LEAGUE_ID` from env (no baked-in league defaults). |
| `src/bot.py` | Optional Discord.py bot. |
| `.github/workflows/scheduled-export.yml` | Actions workflow: dispatch-only triggers, env wiring, Contents API commit for state files. |
| `tests/test_trade_notify.py` | Unit tests (no network). |

---

## Configuration: environment variables

### Required for any poll (`run_once`, `dry_run`, bot)

- `MFL_HOST` — host only (no `https://`), e.g. `www45.myfantasyleague.com`
- `MFL_YEAR` — calendar year segment used in export URLs
- `MFL_LEAGUE_ID` — numeric league id

Resolved in `src/mfl_env.py`.

### Discord (run_once / bot)

- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ID`

### MFL API

- `MFL_API_KEY` — optional in theory but required for private leagues / Actions `Require` step
- `MFL_USER_AGENT` — optional; sent as `User-Agent` when set

### Common optional toggles (defaults exist in code)

- `MFL_TRADE_LOOKBACK_DAYS`
- `MFL_ANNOUNCE_MAX_AGE_HOURS` — blank/unset in Actions → treated as `48` in `run_once`
- `MFL_ANNOUNCE_PENDING_TRADES`, `MFL_NOTIFY_ONCE_PER_TRADE`, `MFL_ANNOUNCE_TRADE_BAIT` — boolean-style env (see `env_bool` in `trade_notify.py`)

Local: use a `.env` file (loaded by `python-dotenv` where used). **`.env` is gitignored.**

---

## GitHub Actions

### Workflow file

- **Name:** `scheduled-export`
- **File:** `.github/workflows/scheduled-export.yml`

### Triggers (no `schedule` cron)

GitHub’s native `schedule` is unreliable (delays, minimum practical interval). The workflow is intended to run via:

1. **`workflow_dispatch`** — e.g. external cron POST (see comment block at top of YAML).
2. **`repository_dispatch`** with `event_type: mfl-poll`.

### Secrets (repository)

Required by the workflow’s guard step:

- `DISCORD_BOT_TOKEN`
- `DISCORD_CHANNEL_ID`
- `MFL_API_KEY`

Also passed when set:

- `MFL_USER_AGENT`

### Variables (repository)

Required by the workflow’s guard step:

- `MFL_HOST`
- `MFL_YEAR`
- `MFL_LEAGUE_ID`

Optional:

- `MFL_ANNOUNCE_MAX_AGE_HOURS`

### Concurrency

- `group: scheduled-export`, `cancel-in-progress: false` — at most one job at a time; queued runs are not cancelled (avoids inconsistent dedupe / partial posts).

### Post-run: state on `main`

The workflow uses `actions/github-script` to commit, via the Contents API (with retries):

- `data/seen_trades.json`
- `data/reports_state.json` (if present)

So each **clone** of this repo on GitHub has **its own** dedupe history unless you share or sync files manually.

---

## External cron (e.g. cron-job.org)

- **Do not** store the GitHub PAT in the repo.
- **POST** to either endpoint documented in the workflow YAML header (`workflow_dispatch` or `repository_dispatch`).
- Typical headers: `Accept: application/vnd.github+json`, `Authorization: Bearer <PAT>`, `Content-Type: application/json`, optional `X-GitHub-Api-Version: 2022-11-28`.
- **204** from workflow dispatch is success.

Fine-grained PAT needs **Actions: Read and write** on each repo you dispatch (for `workflow_dispatch`). For `repository_dispatch`, GitHub documents **Contents: Write** on the repo.

---

## Two-repo pattern (test vs production)

Common setup for this maintainer:

| Remote | Typical GitHub repo | Role |
|--------|---------------------|------|
| `origin` | `moneyball-discord-trade` | Test / staging |
| `production` | `moneyball-analyst-bot` | Production |

Each repo has **its own** Actions secrets/variables and **its own** cron-job.org job URL (`OWNER/REPO` in the dispatch path).

**Push commands:**

```bash
git push origin main          # test repo
git push production main      # prod repo
git push origin main && git push production main   # both
```

Keep `main` tracking `origin/main` unless the user prefers otherwise (`git branch -u origin/main main`).

---

## Commands

```bash
pip install -r requirements.txt
python3 -m pytest -q
python3 -m src.run_once              # one poll (needs env / .env)
python3 -m src.trade_notify --dry-run ...
python3 -m src.bot                   # optional; needs env
```

---

## Debugging notes for agents

1. **Trade bait shows “(nothing listed)” for picks** — ensure `fetch_trade_baits` uses **`INCLUDE_DRAFT_PICKS=1`** (`mfl_client.py`). Without it, MFL may omit `DP_`/`FP_` from `willGiveUp` while the website still shows picks.

2. **`Iâve`-style text in “Looking for”** — usually UTF-8 mojibake from upstream or display; investigate `inExchangeFor` raw value before changing encoding logic.

3. **Dedupe / duplicate announcements** — fingerprinting lives in `trade_notify.py`; `seen_trades.json` keys must stay stable across pending/processed (do not key solely on MFL transaction id for dedupe).

4. **Discord custom text** — not implemented in-repo; use Discord API `POST /channels/{id}/messages` with `{"content":"..."}` and `Authorization: Bot <token>` for one-offs.

---

## Constraints

- Prefer **minimal, task-scoped** changes; match existing style and patterns.
- Do **not** add fake/stub data paths that affect prod.
- Do **not** commit league-identifying defaults into code; league connection is env-driven.

When in doubt, read the workflow header in `scheduled-export.yml` and `src/mfl_env.py` before changing env contracts.
