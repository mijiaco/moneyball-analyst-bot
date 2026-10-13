# Agent guide — moneyball-discord-trade

Instructions for AI assistants and developers working in this repository.

## What this project does

- Polls a **MyFantasyLeague (MFL)** league via JSON **export** HTTP APIs.
- Posts **Discord** messages (embeds) for **trades** and optional **trade bait** updates.
- Posts optional **Restricted Free Agent (RFA)** report embeds (list changes + Saturday weekly) and **invalid RFA claim** alerts; syncs the active RFA list to Google Sheets.
- Posts optional Saturday weekly embeds (Top Traders, draft picks, roster breakdown, **taxi-cut cap refunds pending**).
- Posts optional **Roster Violations** embeds on Thu/Sun/Mon ET slots (Thu/Mon 7:30 PM; Sun 10:00 AM, 12:15 / 3:30 / 7:30 PM) plus Tue/Wed/Fri/Sat at 3:00 PM ET and Saturday 9:00 PM ET.
- Posts optional Sunday **Active Roster: Can Be Demoted** embed (11:00 AM ET) when enabled; default off — use local dry-run preview.
- Posts optional **Top Scorers** embeds after NFL slates (Wed/TNF, Sun 4:15 / 8:00 / 11:45 PM ET; Tue 12:30 AM ET for MNF + week cumulative).
- Posts optional **Points Leaderboard**, **Lottery Balls**, **Top Rookie by Round**, and **Positional Averages** (team QB, punter, and kicker; bye teams omitted) embeds every Tuesday at 3:00 AM CT (4:00 AM ET).
- Posts optional **Weekly Playoff Picture** every Tuesday at 9:00 AM CT (10:00 AM ET).
- Posts immediate **taxi squad cut** alerts when a taxi player is dropped and dead-money hits the cap (commish refund needed).
- Persists **dedupe state** in `data/seen_trades.json`, optional weekly report cursor in `data/reports_state.json`, and RFA state in `data/rfa_state.json` so repeats are not announced.
- **Primary runtime:** GitHub Actions workflow `scheduled-export` running `python -m src.run_once` (no long-lived server required).
- **Optional:** `python -m src.bot` is a Discord gateway client for local/long-poll use; the maintainer typically relies on Actions instead.

Do **not** commit secrets, `.env`, or API keys. Never overwrite `.env` without explicit user approval.

---

## Repository layout (source)

| Path | Role |
|------|------|
| `src/run_once.py` | Single-shot poll + Discord REST posts + save dedupe; Actions entrypoint. |
| `src/trade_poll_core.py` | Shared fetch + build payloads for trades and trade bait. |
| `src/trade_roster_check.py` | Post-trade roster simulation + invalid-roster section for trade embeds. |
| `src/trade_notify.py` | Fingerprints, formatting, dry-run CLI (`python -m src.trade_notify`). |
| `src/mfl_client.py` | Async HTTP client for `…/export` endpoints; players cache; **`INCLUDE_DRAFT_PICKS`** on trade bait fetch so `willGiveUp` includes `DP_` / `FP_` tokens. |
| `src/mfl_env.py` | Requires `MFL_HOST`, `MFL_YEAR`, `MFL_LEAGUE_ID` from env (no baked-in league defaults). |
| `src/rfa_state.py` | RFA roster-diff state machine; FREE_AGENT / BBID_WAIVER parsers. |
| `src/rfa_report.py` | RFA Discord formatters (and re-exports from `rfa_state`). |
| `src/roster_violations.py` | IR eligibility, roster/taxi/IR slot limits, salary cap, starter-depth violations, and Active Roster: Can Be Demoted formatting. |
| `src/taxi_cut_report.py` | Taxi-squad cut dead-money detection, refund matching, immediate + weekly Discord formatters. |
| `src/top_scorers_report.py` | NFL-slate and weekly-cumulative top-5 scorers by position (league scoring). |
| `src/points_leaderboard_report.py` | Tuesday season points leaderboard (#1–#32) and lottery balls for the worst 8 teams. |
| `src/positional_average_report.py` | Tuesday averages for team QB, punter, and kicker, excluding bye teams. |
| `src/playoff_picture_report.py` | Tuesday playoff picture: seeds 1–12 from standings. |
| `src/top_rookie_report.py` | Tuesday top scorer from each round of the current-year draft, with that round's average. |
| `src/google_sheets.py` | Service-account Sheets read (top 32) + write (RFA tab). |
| `src/bot.py` | Optional Discord.py bot. |
| `.github/workflows/scheduled-export.yml` | Actions workflow: dispatch-only triggers, env wiring, Contents API commit for state files. |
| `tests/test_trade_notify.py` | Unit tests (no network). |
| `tests/test_rfa_report.py` | RFA unit tests (no network). |

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

### Google Sheets (RFA report)

- `GOOGLE_SERVICE_ACCOUNT_JSON` — service account JSON string **or** path to a JSON file
- `GOOGLE_SERVICE_ACCOUNT_FILE` — alternate path to the JSON file
- `GOOGLE_SHEETS_SPREADSHEET_ID` — spreadsheet id (defaults to the league RFA workbook id in code)
- `GOOGLE_SHEETS_TOP32_TAB` — eligibility tab (default `Top 32 At Position`)
- `GOOGLE_SHEETS_RFA_TAB` — output tab (default `Restricted Free Agents`)

Share the spreadsheet with the service account email (Editor).

### Common optional toggles (defaults exist in code)

- `MFL_TRADE_LOOKBACK_DAYS`
- `MFL_ANNOUNCE_MAX_AGE_HOURS` — blank/unset in Actions → treated as `48` in `run_once`
- `MFL_ANNOUNCE_PENDING_TRADES`, `MFL_NOTIFY_ONCE_PER_TRADE`, `MFL_ANNOUNCE_TRADE_BAIT` — boolean-style env (see `env_bool` in `trade_notify.py`)
- `MFL_RFA_REPORT_ENABLED` — default true
- `MFL_RFA_INVALID_CLAIM_ALERTS_ENABLED` — default true
- `MFL_RFA_LOOKBACK_DAYS` — defaults to `MFL_TRADE_LOOKBACK_DAYS`
- `MFL_DAILY_ROSTER_VIOLATIONS_ENABLED` — default true (Thu/Sun/Mon slot times ET, Tue/Wed/Fri/Sat 3:00 PM ET, and Saturday 9:00 PM ET; falls back to legacy `MFL_WEEKLY_REPORTS_INCLUDE_ROSTER_VIOLATIONS` when unset)
- `MFL_SUNDAY_ACTIVE_ROSTER_DEMOTE_REPORT_ENABLED` — default false (Sunday 11:00 AM ET Active Roster: Can Be Demoted; keep off for public Discord; local preview via `python -m src.trade_notify --dry-run --active-roster-demote-report`)
- `MFL_TOP_SCORERS_REPORT_ENABLED` — default true (slate top-5 by position; preview via `python -m src.trade_notify --dry-run --top-scorers-report`)
- `MFL_POINTS_LEADERBOARD_REPORT_ENABLED` — default true (Tuesday 3:00 AM CT points leaderboard, then lottery balls for the worst 8 by points)
- `MFL_POSITIONAL_AVERAGES_REPORT_ENABLED` — default true (same Tuesday slot; average team QB, punter, and kicker scores for NFL teams that played, bye teams omitted)
- `MFL_TOP_ROOKIE_BY_ROUND_REPORT_ENABLED` — default true (same Tuesday slot; top scorer in each current-year draft round, using completed NFL weeks only)
- `MFL_PLAYOFF_PICTURE_REPORT_ENABLED` — default true (Tuesday 9:00 AM CT playoff picture: seeds 1–4 division-winner byes, 5–11 by record, 12 by points)
- `MFL_WEEKLY_REPORTS_INCLUDE_TAXI_CUT_REFUNDS` — default true (Saturday list of unreimbursed taxi-cut dead money)
- `MFL_TAXI_CUT_ALERTS_ENABLED` — default true (immediate Discord alert on taxi cut)
- `MFL_TAXI_CUT_LOOKBACK_DAYS` — defaults to `MFL_TRADE_LOOKBACK_DAYS`
- `MFL_IR_ELIGIBLE_STATUSES` — comma/pipe list of NFL injury statuses allowed on IR (default: IR/Out/Doubtful + Suspended/Holdout/Retired/etc.; Questionable excluded)

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
- `GOOGLE_SERVICE_ACCOUNT_JSON` — required for RFA sheet read/write

### Variables (repository)

Required by the workflow’s guard step:

- `MFL_HOST`
- `MFL_YEAR`
- `MFL_LEAGUE_ID`

Optional:

- `MFL_ANNOUNCE_MAX_AGE_HOURS`
- `GOOGLE_SHEETS_SPREADSHEET_ID`
- `GOOGLE_SHEETS_TOP32_TAB`
- `GOOGLE_SHEETS_RFA_TAB`

### Concurrency

- `group: scheduled-export`, `cancel-in-progress: false` — at most one job at a time; queued runs are not cancelled (avoids inconsistent dedupe / partial posts).

### Post-run: state on `main`

The workflow uses `actions/github-script` to commit, via the Contents API (with retries):

- `data/seen_trades.json`
- `data/reports_state.json` (if present)
- `data/rfa_state.json` (if present)
- `data/taxi_cut_state.json` (if present)

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

5. **RFA list empty / skipped** — confirm Sheets service account can read tab `top 32 by position`, and that rows include MFL player ids or unambiguous player names. Without credentials, RFA is skipped (trades still run).

6. **Invalid RFA claim alerts** — triggered when an RFA player is claimed with winning bid / new roster salary below last cut salary (`BBID_WAIVER` bid when present, else roster salary after add).

7. **Taxi-cut refund still listed after commish cleared cap** — refund matching accepts a franchise **−$amount** adjustment equal to dead money, **or** removal of the originating `Dropped …` salary adjustment. Deleting the charge (with no negative line) should clear `pending_cuts` on the next poll.
7. **Roster violations / injuries** — `TYPE=injuries` must use **`api.myfantasyleague.com`** (league host returns an error). Roster Violations posts on Thu/Sun/Mon ET slots (Thu/Mon 7:30 PM; Sun 10:00 AM / 12:15 / 3:30 / 7:30 PM) plus Tue/Wed/Fri/Sat 3:00 PM ET and Saturday 9:00 PM ET (Sunday 9:00 AM CT and Saturday 8:00 PM CT), and does not list active-roster IR/Suspended players; empty violation lists are not posted. **Active Roster: Can Be Demoted** is built for Sundays at 11:00 AM ET but defaults off (`MFL_SUNDAY_ACTIVE_ROSTER_DEMOTE_REPORT_ENABLED`); preview locally with `--active-roster-demote-report`. IR eligibility defaults exclude Questionable; override with `MFL_IR_ELIGIBLE_STATUSES` if the league’s IR setup is broader/narrower. Salary-cap checks use `leagueStandings.salary` vs franchise `salaryCapAmount`. Starting-roster checks compare active (non-IR/taxi) depth to `league.starters` minimums.
8. **Empty weekly/Sunday lists** — Taxi Cut Cap Refunds Pending and Unpaid Owners / Traded Picks skip Discord when there is nothing to list (schedule cursors still advance).
9. **Weekly playoff picture** — Tuesday at/after 9:00 AM CT (10:00 AM ET), separate from the 3:00 AM CT points leaderboard. Uses `TYPE=standings` (`pf`, `pa`, `h2hw` / `h2hl` / `h2ht`) plus each franchise `division` on `TYPE=league`. Seeds 1–4 are division winners by win percentage (tie-break: points for, then fewest points against) and are marked first-round bye. Seeds 5–11 are the next-best records with the same tie-break. Seed 12 is the highest points for among the teams still left (tie-break: fewest points against). The embed lists seeds 1–12 only. Dedupe key is `PLAYOFF_PICTURE|{YYYY-MM-DD}`. It posts to the same `DISCORD_CHANNEL_ID` as the other scheduled reports.
10. **Points leaderboard / lottery balls / top rookie by round** — Tuesday at/after 3:00 AM CT (4:00 AM ET). Season points and head-to-head record come from `TYPE=leagueStandings` (`pf`, `h2hw` / `h2hl` / `h2ht`). Best-to-worst is points, then record, then team name. The worst 8 get 8 balls down to 1. Dedupe keys are `POINTS_LEADERBOARD|{YYYY-MM-DD}` and `LOTTERY_BALLS|{YYYY-MM-DD}` for that Tuesday. Top Rookie by Round uses the current-year `draftResults` and sums `playerScores` for weeks 1 through the last fully final NFL week (an in-progress week is left out). Each line is that round's top scorer and the average of every pick in the round, including zeros. Dedupe key is `TOP_ROOKIE_BY_ROUND|{YYYY-MM-DD}`. Positional Averages uses that same Tuesday slot. It averages `TMQB`, `TMPN`, and `TMPK` for NFL teams on that week's schedule (a bye team is left out, including a zero). The week is the last fully final NFL week. It waits until every team that played has a score at each of those positions. Each fantasy team that has one of those bye players on the active roster is listed with the position average added once per player, the official week score, and the adjusted total. IR and taxi copies are not adjusted. Dedupe key is `POSITIONAL_AVERAGES|{year}-W{week}`.
11. **Top scorers / slates** — `TYPE=nflSchedule` must use **`api.myfantasyleague.com`**. Sunday windows are kickoff-based (early = before 3:00 PM ET, i.e. 1:00 slates). Week points come from `TYPE=playerScores` merged with `TYPE=liveScoring` (`DETAILS=1`) because playerScores often omits Sunday players for hours after games are final. A slate posts only after every game in that window is final **and** at least one matching player has points. Dedupe keys are `TOP_SCORERS|{year}-W{week}|{slate}`; week 1 keys must not suppress week 2+. Cumulative waits for MNF (or posts at the Tuesday slot when there is no MNF). Local preview: `--top-scorers-report`.
12. **Invalid roster on trade embeds** — pending trades simulate player moves (and salary totals) before IR/slot/cap/starter checks; salary is only shifted for players still on the sender so already-updated `leagueStandings` totals are not double-counted during the veto window. Processed trades use current rosters and standings. The section is omitted when neither side is in violation.

---

## Constraints

- Prefer **minimal, task-scoped** changes; match existing style and patterns.
- Do **not** add fake/stub data paths that affect prod.
- Do **not** commit league-identifying defaults into code; league connection is env-driven.

When in doubt, read the workflow header in `scheduled-export.yml` and `src/mfl_env.py` before changing env contracts.
