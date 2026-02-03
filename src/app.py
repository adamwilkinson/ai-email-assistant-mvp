import argparse, os, time, datetime as dt
import store
import json
from pathlib import Path
from dotenv import load_dotenv
from gmail_connector import init_oauth, gmail_service, fetch_recent_threads, fetch_thread_messages_text
from triage import load_schema_dynamic, triage_thread
from digest import render_digest, send_digest_via_gmail_api

def header_value(msg, name: str) -> str:
    headers = (msg.get("payload", {}) or {}).get("headers", []) or []
    for h in headers:
        if (h.get("name", "") or "").lower() == name.lower():
            return h.get("value", "") or ""
    return ""

def now_iso():
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"

def extract_subject_and_history_id(th: dict) -> tuple[str, str | None]:
    """
    Extract subject + latest_history_id from a Gmail thread response.

    - Prefers Subject header from the first message (most reliable).
    - Falls back to thread snippet if messages metadata is missing.
    - latest_history_id is taken from the newest message in the thread.
    """
    subject = "(no subject)"
    msgs_meta = th.get("messages", []) or []
    latest_history_id = None

    if msgs_meta:
        # Usually subject is on the first message in the thread
        subject = header_value(msgs_meta[0], "Subject") or "(no subject)"
        latest_history_id = msgs_meta[-1].get("historyId")  # newest message
    else:
        # fallback if messages metadata missing for some reason
        subject = th.get("snippet") or "(no subject)"

    return subject, (str(latest_history_id) if latest_history_id is not None else None)

def _parse_iso_dt(s: str):
    if not s:
        return None
    try:
        # handle Z
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        return dt.datetime.fromisoformat(s)
    except Exception:
        return None
    
# Basic waiting on response follow up detector.
# TODO: use LLM to determine if the thread was resolved and doesnt require a response.
def waiting_on_reply(messages: list[dict], my_email: str, stale_days: int) -> tuple[bool, dt.datetime | None]:
    """
    Returns (is_stale, last_outbound_dt) if:
      - last outbound msg from me exists
      - no inbound msg from others after that outbound
      - outbound is older than stale_days
    """
    my_email = (my_email or "").lower().strip()
    if not my_email:
        return (False, None)

    # Sort messages by parsed date (fallback keeps original order)
    items = []
    for m in messages:
        d = _parse_iso_dt(m.get("date", ""))
        items.append((d, m))
    items.sort(key=lambda x: x[0] or dt.datetime.min.replace(tzinfo=dt.timezone.utc))

    last_out = None
    last_in = None

    for d, m in items:
        if d is None:
            continue
        frm = (m.get("from") or "").lower()
        is_me = my_email in frm  # simple containment works with "Name <email>"
        if is_me:
            last_out = d
        else:
            last_in = d

    if last_out is None:
        return (False, None)

    # if there is an inbound after my last outbound, I'm not waiting
    if last_in is not None and last_in > last_out:
        return (False, last_out)

    now = dt.datetime.now(dt.timezone.utc)
    age_days = (now - last_out).days
    return (age_days >= stale_days, last_out)

def main():
    load_dotenv()
    p = argparse.ArgumentParser("Email Intelligence MVP — Gmail + Daily Digest (Local)")
    p.add_argument("--init", action="store_true")
    p.add_argument("--run-once", action="store_true")
    p.add_argument("--poll", action="store_true")
    p.add_argument("--interval-min", type=int, default=10)
    p.add_argument("--done", type=int, help="Mark a task id as done")
    p.add_argument("--list", action="store_true", help="List open tasks")
    p.add_argument("--demo", type=str, help="Run using demo JSON threads instead of Gmail")
    p.add_argument("--preview-html", type=str, default="demo/digest_preview.html", help="Where to write the digest HTML in demo mode")
    args = p.parse_args()

    os.makedirs("secrets", exist_ok=True)
    client_secret = os.path.join("secrets","client_secret.json")
    token_path = os.path.join("secrets","token.json")

    if args.init:
        init_oauth(client_secret, token_path)
        print("OAuth complete. Token stored at secrets/token.json")
        return

    user = os.getenv("GMAIL_USER","").strip()
    if not user:
        raise RuntimeError("Set GMAIL_USER in .env")
    digest_to_email = os.getenv("DIGEST_TO_EMAIL", "").strip()
    if not digest_to_email:
        raise RuntimeError("Set DIGEST_TO_EMAIL in .env")
    digest_subject_prefix = os.getenv("DIGEST_SUBJECT_PREFIX", "EIMVP DIGEST")

    creds = init_oauth(client_secret, token_path)
    svc = gmail_service(creds)

    os.makedirs("data", exist_ok=True)
    conn = store.connect(os.path.join("data","state.sqlite"))
    schema = load_schema_dynamic("schema.json")

    if args.done:
        store.mark_task_done(conn, args.done)
        print(f"Marked task {args.done} as done.")
        return
    
    if args.list:
        tasks = store.fetch_open_tasks(conn)
        if not tasks:
            print("No open tasks ✅")
            return

        print("\nOpen tasks:")
        for t in tasks[:200]:
            due = t.get("due_date") or "—"
            bucket = t.get("bucket") or "—"
            subj = (t.get("thread_subject") or "—").strip()
            title = (t.get("title") or "").strip()
            pr = (t.get("priority") or "").strip()

            print(f"#{t['id']:>4}  [{pr:<6}]  due {due:<10}  {bucket:<18}  {title}")
            print(f"      subj: {subj}")
        return

    fup_lookback_days = int(os.getenv("FOLLOWUP_LOOKBACK_DAYS", "90"))
    fup_max_threads = int(os.getenv("FOLLOWUP_MAX_THREADS", "300"))
    fup_enabled = os.getenv("FOLLOWUP_ENABLED", "true").lower() == "true"

    lookback_days = int(os.getenv("LOOKBACK_DAYS","2"))
    max_threads = int(os.getenv("MAX_THREADS_PER_RUN","50"))
    conf_thr = float(os.getenv("CONFIDENCE_THRESHOLD","0.55"))
    send_digest = os.getenv("SEND_DIGEST","true").lower() == "true"

    def run_demo(conn, schema, demo_path: str, preview_html_path: str):
        with open(demo_path, "r", encoding="utf-8") as f:
            demo = json.load(f)

        threads = demo.get("threads", [])
        print(f"[DEMO] loaded {len(threads)} demo threads")

        now = now_iso()

        for th in threads:
            tid = th["thread_id"]
            subject = th.get("subject", "")
            latest_history_id = th.get("latest_history_id", tid)
            msgs = th.get("messages", [])

            # mimic the real pipeline storage
            store.upsert_thread(conn, "demo", tid, subject, now, str(latest_history_id))

            # triage using your existing LLM pipeline
            out = triage_thread(subject, msgs, schema)

            # record + tasks
            store.record_triage(conn, "demo", tid, now, os.getenv("LLM_MODEL",""), float(out.get("confidence", 0.5)), str(latest_history_id), out)
            store.create_tasks_from_actions(conn, "demo", tid, now, out)

        tasks = store.fetch_open_tasks(conn)
        html = render_digest(tasks)

        Path(preview_html_path).parent.mkdir(parents=True, exist_ok=True)
        Path(preview_html_path).write_text(html, encoding="utf-8")

        print(f"[DEMO] open tasks: {len(tasks)}")
        print(f"[DEMO] wrote digest preview: {preview_html_path}")

    if args.demo:
        run_demo(conn, schema, args.demo, args.preview_html)
        return

    def cycle():
        if fup_enabled:
            lookback_threads = fetch_recent_threads(svc, user, lookback_days=fup_lookback_days, max_threads=fup_max_threads)
            for th in lookback_threads:
                tid = th["id"]

                msgs = fetch_thread_messages_text(svc, user, tid, max_messages=6)
                subject, latest_history_id = extract_subject_and_history_id(th)
                
                # --- Follow-up scan: waiting on replies ---
                my_email = os.getenv("MY_EMAIL", "").strip()
                stale_days = int(os.getenv("FOLLOWUP_STALE_DAYS", "14"))
                followup_domain = (os.getenv("FOLLOWUP_DOMAIN", "followups") or "followups").strip().lower()
                followup_priority = (os.getenv("FOLLOWUP_PRIORITY", "normal") or "normal").strip().lower()

                is_stale, last_out_dt = waiting_on_reply(msgs, my_email, stale_days)
                if is_stale and last_out_dt:
                    # ensure the thread bucket reflects followups (so it shows in digest buckets)
                    # (this updates threads.digest_bucket; safe even if LLM also sets it later)
                    conn.execute("""
                    UPDATE threads SET digest_bucket=? WHERE provider=? AND thread_id=?
                    """, (followup_domain, "gmail", tid))
                    conn.commit()

                    title = f"Follow up: {subject or 'Email thread'}"
                    notes = f"Waiting on reply. Last outbound from {my_email} was {last_out_dt.date().isoformat()}."
                    # due_date can be today; or last_out + stale_days
                    due_date = dt.date.today().isoformat()

                    store.create_followup_task(conn, "gmail", tid, now_iso(), followup_priority, title, due_date, notes)

        threads = fetch_recent_threads(svc, user, lookback_days=lookback_days, max_threads=max_threads)
        print(f"[{now_iso()}] fetched {len(threads)} threads")
        for th in threads:
            tid = th["id"]

            subject, latest_history_id = extract_subject_and_history_id(th)

            # Check if the thread has changed since last poll, and skip triage if not
            if latest_history_id and (not store.should_analyze_thread(conn, "gmail", tid, latest_history_id)):
                continue

            store.upsert_thread(conn, "gmail", tid, subject, now_iso(), latest_history_id or "")
            msgs = fetch_thread_messages_text(svc, user, tid, max_messages=6)
            out = triage_thread(subject, msgs, schema)
            store.record_triage(conn, "gmail", tid, now_iso(), os.getenv("LLM_MODEL","simulate"), float(out["confidence"]), latest_history_id or "", out)
            if out.get("priority") == "ignore" and out.get("confidence",0) >= conf_thr:
                continue
            store.create_tasks_from_actions(conn, "gmail", tid, now_iso(), out)
            
        tasks = store.fetch_open_tasks(conn)
        print(f"[{now_iso()}] open tasks: {len(tasks)}")
        if send_digest:
            html = render_digest(tasks)
            send_digest_via_gmail_api(svc, user, digest_to_email, f"[{digest_subject_prefix}] Daily Action Digest", html)
            print(f"[{now_iso()}] digest sent to {digest_to_email}")

    if args.run_once:
        cycle(); return
    if args.poll:
        while True:
            cycle()
            time.sleep(max(60, args.interval_min*60))
    else:
        p.print_help()

if __name__ == "__main__":
    main()
