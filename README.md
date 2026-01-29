# Email Intelligence MVP (Local) — Gmail + Daily Digest

This starter kit is a **local-first** MVP for:
- Gmail inbox monitoring (polling)
- Thread-level analysis
- LLM-based triage + extraction (strict JSON)
- Daily digest email back to you
- SQLite state store + audit log

## Quick start
1) Put Google OAuth Desktop credentials at `secrets/client_secret.json`
2) `python -m venv .venv && source .venv/bin/activate`
3) `pip install -r requirements.txt`
4) `cp .env.example .env` and edit required feilds.
5) `python src/app.py --init`
6) `python src/app.py --run-once` (or `--poll --interval-min 10`)
7) `python src/app.py --list`
8) `python src/app.py --done task-id`

## Notes
- Default LLM mode is `simulate` so you can test without keys.
- Switch to a real model via `LLM_MODE=openai_compatible` and set `LLM_BASE_URL`, `LLM_API_KEY`, `LLM_MODEL`.
- Stores extracted facts + tasks in SQLite under `data/state.sqlite`.
- To hard resest the state, remove the database file `rm -f data/state.sqlite`.
