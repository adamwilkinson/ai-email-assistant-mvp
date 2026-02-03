import sqlite3, json
from typing import Any, Dict, List
import hashlib

SCHEMA = """
PRAGMA journal_mode=WAL;

CREATE TABLE IF NOT EXISTS threads (
  provider TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  subject TEXT,
  last_seen_at TEXT,
  last_analyzed_at TEXT,
  digest_bucket TEXT,
  last_seen_history_id TEXT,
  last_analyzed_history_id TEXT,
  PRIMARY KEY (provider, thread_id)
);

CREATE TABLE IF NOT EXISTS triage_runs (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  run_at TEXT NOT NULL,
  model TEXT,
  confidence REAL,
  output_json TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS tasks (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  provider TEXT NOT NULL,
  thread_id TEXT NOT NULL,
  created_at TEXT NOT NULL,
  status TEXT NOT NULL DEFAULT 'open',
  priority TEXT NOT NULL,
  title TEXT NOT NULL,
  due_date TEXT,
  notes TEXT,
  task_key TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_tasks_status ON tasks(status);
CREATE INDEX IF NOT EXISTS idx_tasks_due ON tasks(due_date);
CREATE UNIQUE INDEX IF NOT EXISTS ux_tasks_task_key ON tasks(task_key);
"""

def _task_key(provider: str, thread_id: str, title: str, due: str | None) -> str:
    base = f"{provider}|{thread_id}|{title.strip().lower()}|{(due or '').strip()}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()

def connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys=ON;")
    conn.executescript(SCHEMA)
    return conn

def mark_task_done(conn, task_id: int):
    conn.execute("UPDATE tasks SET status='done' WHERE id=?", (task_id,))
    conn.commit()

def create_followup_task(conn, provider: str, thread_id: str, created_at: str,
                        priority: str, title: str, due_date: str | None, notes: str):
  key = _task_key(provider, thread_id, title, due_date)
  conn.execute("""
    INSERT OR IGNORE INTO tasks(provider, thread_id, created_at, priority, title, due_date, notes, task_key)
    VALUES(?,?,?,?,?,?,?,?)
  """, (provider, thread_id, created_at, priority, title, due_date, notes, key))
  conn.commit()

# Helper to decide whether thread needs re-analysis
def should_analyze_thread(conn, provider: str, thread_id: str, latest_history_id: str) -> bool:
    cur = conn.execute("""
      SELECT last_analyzed_history_id
      FROM threads
      WHERE provider=? AND thread_id=?
    """, (provider, thread_id))
    row = cur.fetchone()
    if not row:
        return True
    last_analyzed = row[0]
    return (last_analyzed is None) or (str(last_analyzed) != str(latest_history_id))

def upsert_thread(conn, provider: str, thread_id: str, subject: str, last_seen_at: str, last_seen_history_id: str):
    conn.execute("""
      INSERT INTO threads(provider, thread_id, subject, last_seen_at, last_seen_history_id)
      VALUES(?,?,?,?,?)
      ON CONFLICT(provider, thread_id) DO UPDATE SET
        subject=excluded.subject,
        last_seen_at=excluded.last_seen_at,
        last_seen_history_id=excluded.last_seen_history_id
    """, (provider, thread_id, subject, last_seen_at, last_seen_history_id))
    conn.commit()

def record_triage(conn, provider: str, thread_id: str, run_at: str, model: str, confidence: float, latest_history_id: str, output: Dict[str, Any]):
    conn.execute("""
      INSERT INTO triage_runs(provider, thread_id, run_at, model, confidence, output_json)
      VALUES(?,?,?,?,?,?)
    """, (provider, thread_id, run_at, model, confidence, json.dumps(output, ensure_ascii=False)))
    conn.execute("""
      UPDATE threads SET last_analyzed_at=?, digest_bucket=?, last_analyzed_history_id=? WHERE provider=? AND thread_id=?
    """, (run_at, output.get("domain","other"),latest_history_id, provider, thread_id))
    conn.commit()

def create_tasks_from_actions(conn, provider: str, thread_id: str, created_at: str, triage_output: Dict[str, Any]):
    pr = triage_output.get("priority","normal")
    for a in triage_output.get("recommended_actions", []):
        if a.get("action") not in ("create_task","send_reminder","review_needed"):
            continue
        title = a.get("title") or "Follow up"
        notes = a.get("notes") or triage_output.get("rationale","")
        due = a.get("due_date")
        key = _task_key(provider, thread_id, title, due)
        conn.execute("""
          INSERT OR IGNORE INTO tasks(provider, thread_id, created_at, priority, title, due_date, notes, task_key)
          VALUES(?,?,?,?,?,?,?,?)
        """, (provider, thread_id, created_at, pr, title, due, notes, key))
    conn.commit()

def fetch_open_tasks(conn) -> List[Dict[str, Any]]:
    cur = conn.execute("""
      SELECT 
        t.id, 
        t.provider, 
        t.thread_id, 
        t.created_at, 
        t.priority, 
        t.title, 
        t.due_date, 
        t.notes, 
        th.subject AS thread_subject,
        th.digest_bucket AS bucket
      FROM tasks t
      LEFT JOIN threads th
      ON th.provider = t.provider
      AND th.thread_id = t.thread_id
      WHERE status='open'
      ORDER BY
        CASE priority WHEN 'urgent' THEN 0 WHEN 'high' THEN 1 WHEN 'normal' THEN 2 ELSE 3 END,
        COALESCE(due_date,'9999-12-31') ASC
    """)
    cols = [c[0] for c in cur.description]
    return [dict(zip(cols, row)) for row in cur.fetchall()]
