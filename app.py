import argparse, os, time, datetime as dt
from dotenv import load_dotenv
from gmail_connector import init_oauth, gmail_service, fetch_recent_threads, fetch_thread_messages_text
import store
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

def main():
    load_dotenv()
    p = argparse.ArgumentParser("Email Intelligence MVP — Gmail + Daily Digest (Local)")
    p.add_argument("--init", action="store_true")
    p.add_argument("--run-once", action="store_true")
    p.add_argument("--poll", action="store_true")
    p.add_argument("--interval-min", type=int, default=10)
    p.add_argument("--done", type=int, help="Mark a task id as done")
    p.add_argument("--list", action="store_true", help="List open tasks")
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

    lookback_days = int(os.getenv("LOOKBACK_DAYS","2"))
    max_threads = int(os.getenv("MAX_THREADS_PER_RUN","50"))
    conf_thr = float(os.getenv("CONFIDENCE_THRESHOLD","0.55"))
    send_digest = os.getenv("SEND_DIGEST","true").lower() == "true"

    def cycle():
        threads = fetch_recent_threads(svc, user, lookback_days=lookback_days, max_threads=max_threads)
        print(f"[{now_iso()}] fetched {len(threads)} threads")
        for th in threads:
            tid = th["id"]

            # Subject logic to account for snippet missing subject
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
            send_digest_via_gmail_api(svc, user, user, "[EIMVP DIGEST] Daily Action Digest — Email Intelligence MVP", html)
            print(f"[{now_iso()}] digest sent to {user}")

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
