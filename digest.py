import base64, datetime as dt
from email.mime.text import MIMEText
from typing import Dict, Any, List

def _esc(s: str) -> str:
    return (s or "").replace("&","&amp;").replace("<","&lt;").replace(">","&gt;")

def _parse_date(s: str):
    try:
        return dt.date.fromisoformat(s)
    except Exception:
        return None

def _days_until(d: dt.date):
    return (d - dt.date.today()).days

def _bucket_label(key: str) -> str:
    labels = {
        "urgent": "Urgent (next 72h)",
        "expiry": "Expiries / Deadlines",
        "audit": "Audit / Evidence Requests",
        "followup": "Follow-ups You Owe",
        "payment": "Payments / Receivables",
        "review": "Review Needed",
        "other": "Other",
        "noise": "Suppressed / Noise (should be empty)"
    }
    return labels.get(key, key.title())

def _compute_bucket(t: Dict[str, Any]) -> str:
    # Priority override: urgent bucket if due soon or marked urgent
    due = _parse_date(t.get("due_date") or "")
    if t.get("priority") == "urgent":
        return "urgent"
    if due is not None and _days_until(due) <= 3:
        return "urgent"

    # If triage thought it's ambiguous, you may have tasks titled "Review..."
    title = (t.get("title") or "").lower()
    if "review" in title or "unclear" in (t.get("notes") or "").lower():
        return "review"

    # Otherwise bucket by triage domain stored on thread
    b = (t.get("bucket") or "other").lower()
    if b in ("expiry","audit","followup","payment","other","noise"):
        return b
    return "other"

def render_digest(tasks: List[Dict[str, Any]]) -> str:
    # Group tasks into buckets
    buckets_order = ["urgent", "expiry", "audit", "followup", "payment", "review", "other"]
    groups: Dict[str, List[Dict[str, Any]]] = {k: [] for k in buckets_order}
    extras: List[Dict[str, Any]] = []

    for t in tasks:
        k = _compute_bucket(t)
        if k in groups:
            groups[k].append(t)
        else:
            extras.append(t)

    # Helper to build table rows
    def rows_for(items: List[Dict[str, Any]]) -> str:
        out = []
        for t in items[:80]:
            due = _esc(t.get("due_date") or "—")
            subj = _esc(t.get("thread_subject") or "—")
            thr = t.get("thread_id")
            link = f"https://mail.google.com/mail/u/0/#inbox/{thr}" if thr else "#"
            out.append(
                "<tr>"
                f"<td>{_esc(str(t.get('id') or ''))}</td>"
                f"<td>{_esc(t.get('priority') or '')}</td>"
                f"<td>{_esc(t.get('title') or '')}<div style='color:#6b7280;font-size:12px;margin-top:2px'>{subj}</div></td>"
                f"<td>{due}</td>"
                f"<td><a href='{link}'>Open</a></td>"
                "</tr>"
            )
        return "".join(out) if out else "<tr><td colspan='4'>No items ✅</td></tr>"

    # Summary counts for reviewing bucket quality
    counts_html = "".join([
        f"<span style='display:inline-block;margin:0 8px 8px 0;padding:6px 10px;border:1px solid #e5e7eb;border-radius:999px;background:#f8fafc'>"
        f"<b>{_esc(_bucket_label(k))}:</b> {len(groups[k])}"
        f"</span>"
        for k in buckets_order
    ])

    sections = []
    for k in buckets_order:
        label = _bucket_label(k)
        n = len(groups[k])
        # Open urgent + review by default so you can “review the buckets” quickly
        open_attr = " open" if k in ("urgent","review") else ""
        sections.append(f"""
        <details{open_attr} style="margin:10px 0;border:1px solid #e5e7eb;border-radius:14px;background:#ffffff;">
          <summary style="padding:10px 12px;cursor:pointer;background:#f8fafc;border-radius:14px;">
            <b>{_esc(label)}</b> <span style="color:#6b7280">({n})</span>
          </summary>
          <div style="padding:10px 12px;">
            <table cellpadding="8" cellspacing="0" style="border-collapse:collapse;width:100%;border:1px solid #e5e7eb">
              <thead>
                <tr style="background:#f8fafc">
                  <th align="left">ID</th>
                  <th align="left">Priority</th>
                  <th align="left">Task + Subject</th>
                  <th align="left">Due</th>
                  <th align="left">Link</th>
                </tr>
              </thead>
              <tbody>
                {rows_for(groups[k])}
              </tbody>
            </table>
          </div>
        </details>
        """)

    return f"""
    <div style="font-family:ui-sans-serif,system-ui; line-height:1.35; color:#111827">
      <h2 style="margin:0 0 6px">Daily Action Digest</h2>
      <div style="color:#6b7280;margin-bottom:10px">{dt.date.today().strftime('%B %d, %Y')}</div>

      <div style="margin:10px 0 4px;color:#111827"><b>Bucket review</b> <span style="color:#6b7280">(counts)</span></div>
      <div>{counts_html}</div>

      {"".join(sections)}

      <p style="color:#6b7280;margin-top:10px;font-size:12px">
        Generated locally. Click “Open” to jump directly to the Gmail thread.
      </p>
    </div>
    """

def send_digest_via_gmail_api(service, user_id: str, to_email: str, subject: str, html: str):
    msg = MIMEText(html, "html", "utf-8")
    msg["to"] = to_email
    msg["from"] = to_email
    msg["subject"] = subject
    raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
    service.users().messages().send(userId=user_id, body={"raw": raw}).execute()



# def render_digest(tasks: List[Dict[str, Any]]) -> str:
#     rows = []
#     for t in tasks[:60]:
#         due = _esc(t.get("due_date") or "—")
#         thread_id = t.get('thread_id')
#         subj = t.get("thread_subject") or "—"
#         link = f"https://mail.google.com/mail/u/0/#inbox/{thread_id}"
#         rows.append(f"<tr>"
#                     f"<td>{_esc(t.get('priority'))}</td>"
#                     f"<td>{_esc(t.get('title'))}</td>"
#                     f"<td>{_esc(subj)}</td>"
#                     f"<td>{due}</td>"
#                     f"<td style='color:#6b7280'>{_esc(thread_id)}</td>"
#                     f"<td><a href='{link}'>Open</a></td>"
#                     f"</tr>")
#     return f"""
#     <div style="font-family:ui-sans-serif,system-ui; line-height:1.4">
#       <h2 style="margin:0 0 8px">Daily Action Digest</h2>
#       <div style="color:#6b7280;margin-bottom:10px">{dt.date.today().strftime('%B %d, %Y')}</div>
#       <table cellpadding="8" cellspacing="0" style="border-collapse:collapse;width:100%;border:1px solid #e5e7eb">
#         <thead><tr style="background:#f8fafc">
#           <th align="left">Priority</th><th align="left">Task</th><th align="left">Subject</th><th align="left">Due</th><th align="left">Thread</th><th align="left">Link</th>
#         </tr></thead>
#         <tbody>{''.join(rows) if rows else '<tr><td colspan="4">No open tasks 🎉</td></tr>'}</tbody>
#       </table>
#       <p style="color:#6b7280;margin-top:12px;font-size:12px">Generated by your local Email Intelligence MVP.</p>
#     </div>
#     """

# def send_digest_via_gmail_api(service, user_id: str, to_email: str, subject: str, html: str):
#     msg = MIMEText(html, "html", "utf-8")
#     msg["to"] = to_email
#     msg["from"] = to_email
#     msg["subject"] = subject
#     raw = base64.urlsafe_b64encode(msg.as_bytes()).decode("utf-8")
#     service.users().messages().send(userId=user_id, body={"raw": raw}).execute()
