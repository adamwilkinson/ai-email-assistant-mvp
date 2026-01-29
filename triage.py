import os, json, re, copy
from typing import Any, Dict, List
import requests
from jsonschema import validate

def load_schema_dynamic(path: str) -> dict:
    with open(path, "r", encoding="utf-8") as f:
        schema = json.load(f)

    schema = copy.deepcopy(schema)

    domains_env = os.getenv("DOMAINS", "").strip()
    if domains_env:
        domains = [d.strip() for d in domains_env.split(",") if d.strip()]
        # enforce at least 2 domains to avoid weirdness
        if len(domains) >= 2:
            schema["properties"]["domain"]["enum"] = domains

    default_domain = os.getenv("DOMAIN_DEFAULT", "").strip()
    if default_domain:
        schema.setdefault("properties", {}).setdefault("domain", {}).setdefault("enum", [])
        # (optional) you can ensure the default is in the enum list
        if "enum" in schema["properties"]["domain"] and default_domain not in schema["properties"]["domain"]["enum"]:
            schema["properties"]["domain"]["enum"].append(default_domain)

    return schema

def strip_quotes_and_signatures(text: str) -> str:
    lines = text.splitlines()
    cleaned = []
    for ln in lines:
        if ln.strip().startswith(">"):
            continue
        if re.match(r"^On .* wrote:$", ln.strip()):
            continue
        cleaned.append(ln)
    out = "\n".join(cleaned).strip()
    for marker in ["--", "Sent from my", "Kind regards", "Best regards", "Regards,"]:
        idx = out.find(marker)
        if idx != -1 and idx > 80:
            out = out[:idx].strip()
            break
    return out

def build_prompt(thread_subject: str, messages: List[Dict[str, Any]], schema: dict) -> str:
    domains = schema["properties"]["domain"]["enum"]
    my_email = os.getenv("MY_EMAIL","").strip().lower()
    parts = []
    parts.append(f"You are an AI email assistant with the goal of triaging emails to save time.")
    parts.append(f"Return ONLY valid JSON that matches the provided schema. Be EXACT when referencing the schema for allowed values.")
    parts.append(f"Be conservative: if low-impact or informational, set priority='ignore'. If 'recommended_actions' is not empty then ")
    parts.append(f"and priority='ignore, then set priority='normal")
    parts.append(f"Allowed domain values are ONLY: {domains}")
    parts.append(f"If message is from {my_email}, then treat it as sent by me.")
    parts.append(f"Thread subject: {thread_subject}")
    parts.append("Messages (newest last):")
    for m in messages:
        body = strip_quotes_and_signatures(m.get("text","") or "")[:2500]
        parts.append(f"- From: {m.get('from','')} | Date: {m.get('date','')}")
        parts.append(f"  Body: {body}")
    parts.append("Return JSON now. No markdown. No extra keys.")
    return "\\n".join(parts)

def simulate_llm(thread_subject: str, messages: List[Dict[str, Any]]) -> Dict[str, Any]:
    text = (" ".join([(m.get("text","") or "") for m in messages]) + " " + thread_subject).lower()
    def has(*w): return any(x in text for x in w)
    domain, intent, priority = "other", "other", "normal"
    actions, extractions = [], []
    rationale = "Simulated triage."
    if has("invoice","remittance","payment","paid","wire","ach"):
        domain, intent, priority = "payment", "payment_commitment", "high"
        actions.append({"action":"create_task","title":"Payment follow-up / confirm remittance","notes":"Check promised payment status.","due_date":None,"urgency_window":"7d"})
        extractions.append({"type":"payment","summary":"Payment-related conversation detected.","due_date":None,"amount":None,"currency":None,"invoice_id":None,"counterparty":None,"confidence":0.65})
    if has("expires","expiry","renewal","expiring"):
        domain, intent, priority = "expiry", "deadline", "urgent"
        actions.append({"action":"create_task","title":"Track upcoming expiry / renewal","notes":"Confirm expiry date and renewal owner.","due_date":None,"urgency_window":"72h"})
        extractions.append({"type":"expiry","summary":"Expiry/renewal signal detected.","due_date":None,"amount":None,"currency":None,"invoice_id":None,"counterparty":None,"confidence":0.65})
    if has("soc","audit","evidence","pbc","controls","request"):
        domain, intent, priority = "audit", "request", "high"
        actions.append({"action":"create_task","title":"Audit request: respond / provide evidence","notes":"Identify requested items and due date.","due_date":None,"urgency_window":"7d"})
        extractions.append({"type":"document_request","summary":"Audit/evidence request detected.","due_date":None,"amount":None,"currency":None,"invoice_id":None,"counterparty":None,"confidence":0.62})
    if not actions and has("fyi","newsletter","promo","update","thank you"):
        domain, intent, priority = "noise", "fyi", "ignore"
        actions.append({"action":"suppress","title":"Ignore low-impact email","notes":"No action required.","due_date":None,"urgency_window":None})
        rationale = "Simulated: informational/noise."
    if not actions:
        actions.append({"action":"review_needed","title":"Review thread","notes":"Unclear intent; needs quick scan.","due_date":None,"urgency_window":"7d"})
        rationale = "Simulated: ambiguous thread."
    return {"domain":domain,"intent":intent,"priority":priority,"confidence":0.62 if domain!="noise" else 0.75,"rationale":rationale,"extractions":extractions,"recommended_actions":actions}

def call_openai_compatible(prompt: str, base_url: str, api_key: str, model: str, schema: dict) -> str:
    url = base_url.rstrip("/") + "/chat/completions"
    headers = {"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"}

    payload = {
        "model": model,
        "messages": [
            {"role": "system", "content": "Return a response that matches the provided JSON schema."},
            {"role": "user", "content": prompt},
        ],
        "temperature": 0.2,
        "response_format": {
            "type": "json_schema",
            "json_schema": {
                "name": "EmailTriageResult",
                "schema": schema,
                "strict": True
            }
        }
    }

    r = requests.post(url, headers=headers, json=payload, timeout=60)
    if not r.ok:
        raise RuntimeError(f"OpenAI {r.status_code}: {r.text}")
    data = r.json()
    return data["choices"][0]["message"]["content"]

def triage_thread(thread_subject: str, messages: List[Dict[str, Any]], schema: Dict[str, Any]) -> Dict[str, Any]:
    mode = os.getenv("LLM_MODE","simulate").strip().lower()
    if mode == "simulate":
        out = simulate_llm(thread_subject, messages)
        validate(instance=out, schema=schema)
        return out
    base_url = os.getenv("LLM_BASE_URL","").strip()
    api_key = os.getenv("LLM_API_KEY","").strip()
    model = os.getenv("LLM_MODEL","gpt-4o-mini").strip()
    if not (base_url and api_key):
        raise RuntimeError("LLM_MODE=openai_compatible but LLM_BASE_URL/LLM_API_KEY not set")
    prompt = build_prompt(thread_subject, messages, schema)
    raw = call_openai_compatible(prompt, base_url, api_key, model, schema)
    out = json.loads(raw)
    validate(instance=out, schema=schema)
    return out
