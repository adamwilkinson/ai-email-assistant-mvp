import os, json, re
from typing import Any, Dict, List
import requests
from jsonschema import validate

def apply_defaults(out: dict) -> dict:
    base = {
        "domain": "other",
        "intent": "other",
        "priority": "normal",
        "confidence": 0.5,
        "rationale": "",
        "extractions": [],
        "recommended_actions": [],
    }
    base.update(out or {})

    # If it's ignore/noise, enforce a safe suppress action
    if base["priority"] == "ignore" and not base["recommended_actions"]:
        base["recommended_actions"] = [{
            "action": "suppress",
            "title": "Ignore low-impact email",
            "notes": "No action required.",
            "due_date": None,
            "urgency_window": None
        }]
    return base

def normalize_output(out: dict) -> dict:
    # Defaults
    base = {
        "domain": "other",
        "intent": "other",
        "priority": "normal",
        "confidence": 0.5,
        "rationale": "",
        "extractions": [],
        "recommended_actions": [],
    }
    base.update(out or {})

    # Normalize priority aliases
    p = (base.get("priority") or "").strip().lower()
    priority_map = {
        "medium": "normal",
        "med": "normal",
        "low": "ignore",
        "none": "ignore",
        "informational": "ignore",
    }
    base["priority"] = priority_map.get(p, p) or "normal"

    # Normalize domain aliases (optional hardening)
    d = (base.get("domain") or "").strip().lower()
    domain_map = {
        "receivables": "payment",
        "invoice": "payment",
        "billing": "payment",
        "renewals": "expiry",
    }
    base["domain"] = domain_map.get(d, d) or "other"

    # Normalize intent aliases (optional hardening)
    i = (base.get("intent") or "").strip().lower()
    intent_map = {
        "follow_up": "followup_needed",
        "followup": "followup_needed",
        "pay": "payment_commitment",
        "payment": "payment_commitment",
    }
    base["intent"] = intent_map.get(i, i) or "other"

    # If ignore/noise but no actions, add suppress so schema passes
    if base["priority"] == "ignore" and not base["recommended_actions"]:
        base["recommended_actions"] = [{
            "action": "suppress",
            "title": "Ignore low-impact email",
            "notes": "No action required.",
            "due_date": None,
            "urgency_window": None
        }]

    return base

def load_schema(path: str) -> Dict[str, Any]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)

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

def build_prompt(thread_subject: str, messages: List[Dict[str, Any]]) -> str:
    parts = []
    parts.append("You are an AI assistant for a financial analyst and auditor.")
    parts.append("Return ONLY valid JSON that matches the provided schema. Be EXACT when referencing the schema for allowed values AND ensure EXACTLY these top-level keys:")
    parts.append(" domain, intent, priority, confidence, rationale, extractions, recommended_actions. Always include all keys.")
    parts.append("Be conservative: if low-impact or informational, set priority='ignore' and domain='noise'.")
    parts.append("If no action is needed: domain=\"noise\", intent=\"fyi\", priority=\"ignore\", confidence between 0 and 1, ")
    parts.append("extractions=[], recommended_actions=[{\"action\":\"suppress\",\"title\":\"Ignore low-impact email\",")
    parts.append("\"notes\":\"No action required.\",\"due_date\":null,\"urgency_window\":null}]")
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
    model = os.getenv("LLM_MODEL","gpt-4.1-mini").strip()
    if not (base_url and api_key):
        raise RuntimeError("LLM_MODE=openai_compatible but LLM_BASE_URL/LLM_API_KEY not set")
    prompt = build_prompt(thread_subject, messages)
    raw = call_openai_compatible(prompt, base_url, api_key, model, schema)
    out = json.loads(raw)
    validate(instance=out, schema=schema)
    return out
