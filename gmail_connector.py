import base64
import datetime as dt
from typing import List, Dict, Any
from googleapiclient.discovery import build
from google.oauth2.credentials import Credentials
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request

SCOPES = [
    "https://www.googleapis.com/auth/gmail.readonly",
    "https://www.googleapis.com/auth/gmail.send",
]

def init_oauth(client_secret_path: str, token_path: str) -> Credentials:
    creds = None
    try:
        creds = Credentials.from_authorized_user_file(token_path, SCOPES)
    except Exception:
        creds = None
    if creds and creds.valid:
        return creds
    if creds and creds.expired and creds.refresh_token:
        creds.refresh(Request())
    else:
        flow = InstalledAppFlow.from_client_secrets_file(client_secret_path, SCOPES)
        creds = flow.run_local_server(port=0)
    with open(token_path, "w", encoding="utf-8") as f:
        f.write(creds.to_json())
    return creds

def gmail_service(creds: Credentials):
    return build("gmail", "v1", credentials=creds)

def _iso_from_ms(ms: str) -> str:
    ts = int(ms) / 1000.0
    return dt.datetime.fromtimestamp(ts).isoformat()

def fetch_recent_threads(service, user_id: str, lookback_days: int = 2, max_threads: int = 50) -> List[Dict[str, Any]]:
    q = f"newer_than:{lookback_days}d -category:promotions -category:social"
    res = service.users().threads().list(userId=user_id, q=q, maxResults=max_threads).execute()
    threads = res.get("threads", [])
    out = []
    for t in threads:
        tid = t["id"]
        th = service.users().threads().get(
            userId=user_id, id=tid, format="metadata",
            metadataHeaders=["From","To","Cc","Subject","Date","Message-ID"]
        ).execute()
        out.append(th)
    return out

def fetch_thread_messages_text(service, user_id: str, thread_id: str, max_messages: int = 6) -> List[Dict[str, Any]]:
    th = service.users().threads().get(userId=user_id, id=thread_id, format="full").execute()
    msgs = th.get("messages", [])[-max_messages:]

    def get_header(headers, name):
        for h in headers:
            if h.get("name","").lower() == name.lower():
                return h.get("value","")
        return ""

    def decode_part(part):
        data = (part.get("body", {}) or {}).get("data")
        if data:
            return base64.urlsafe_b64decode(data.encode("utf-8")).decode("utf-8", errors="ignore")
        return ""

    def walk(payload):
        if not payload:
            return ""
        if payload.get("mimeType") == "text/plain":
            return decode_part(payload)
        for p in payload.get("parts", []) or []:
            txt = walk(p)
            if txt.strip():
                return txt
        if (payload.get("body", {}) or {}).get("data"):
            return decode_part(payload)
        return ""

    out = []
    for m in msgs:
        payload = m.get("payload", {}) or {}
        headers = payload.get("headers", []) or []
        out.append({
            "message_id": m.get("id"),
            "internal_date": _iso_from_ms(m.get("internalDate","0")),
            "from": get_header(headers, "From"),
            "to": get_header(headers, "To"),
            "subject": get_header(headers, "Subject"),
            "date": get_header(headers, "Date"),
            "text": walk(payload),
        })
    return out
