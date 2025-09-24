# backend/chat.py
from __future__ import annotations
from fastapi import APIRouter, Depends
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from typing import List, Dict, Any
import json, os, time
from .cache import redis_client, key
from .settings import settings
import httpx

router = APIRouter()

# ---- config ----
MODEL = os.getenv("LLM_MODEL", "gpt-4o-mini")
OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")

class ChatMessage(BaseModel):
    role: str   # 'system' | 'user' | 'assistant'
    content: str

class ChatRequest(BaseModel):
    session_id: str
    user_message: str

# Simple helpers
def _hist_key(session_id: str) -> str:
    return key("chat", "hist", session_id)  # e.g. demo:chat:hist:{session}

def _cache_key(session_id: str, user_msg: str) -> str:
    return key("chat", "cache", session_id, str(abs(hash(user_msg)) % (10**10)))

async def _append_history(r, session_id: str, msg: Dict[str, Any]):
    # Use JSONL in a Redis list for easy streaming append
    r.rpush(_hist_key(session_id), json.dumps(msg))
    r.expire(_hist_key(session_id), 60 * 60)  # 1h TTL

async def _load_history(r, session_id: str, limit: int = 20) -> List[Dict[str, Any]]:
    vals = r.lrange(_hist_key(session_id), max(-limit, -1000), -1) or []
    return [json.loads(v) for v in vals]

@router.post("/chat")
async def chat(req: ChatRequest):
    r = redis_client()

    # quick response cache (avoid repeat identical prompts)
    ck = _cache_key(req.session_id, req.user_message)
    cached = r.get(ck)
    if cached:
        msg = {"role": "assistant", "content": cached}
        await _append_history(r, req.session_id, {"role":"user","content":req.user_message})
        await _append_history(r, req.session_id, msg)
        def _emit():
            yield msg["content"]
        return StreamingResponse(_emit(), media_type="text/plain")

    # load last N history turns
    history = await _load_history(r, req.session_id)
    # system primer – inject app-specific guidance (tools, tone, guardrails)
    system_msg = {
        "role": "system",
        "content": (
            "You are a helpful assistant for a retail analytics demo. "
            "When users ask for reports, consider calling the server tools. "
            "Be concise, and prefer markdown lists/tables."
        )
    }
    messages = [system_msg] + history + [{"role": "user", "content": req.user_message}]

    # ---- call OpenAI with streaming ----
    async def stream_llm():
        # record user input
        await _append_history(r, req.session_id, {"role":"user","content":req.user_message})

        # example OpenAI Chat Completions streaming
        url = "https://api.openai.com/v1/chat/completions"
        headers = {"Authorization": f"Bearer {OPENAI_API_KEY}", "Content-Type":"application/json"}
        payload = {
            "model": MODEL,
            "stream": True,
            "temperature": 0.2,
            "messages": messages,
        }
        assistant_text = []
        async with httpx.AsyncClient(timeout=30.0) as client:
            async with client.stream("POST", url, headers=headers, json=payload) as resp:
                async for line in resp.aiter_lines():
                    if not line or not line.startswith("data:"):
                        continue
                    if line.strip() == "data: [DONE]":
                        break
                    try:
                        chunk = json.loads(line[len("data:"):].strip())
                        delta = chunk["choices"][0]["delta"].get("content")
                        if delta:
                            assistant_text.append(delta)
                            yield delta
                    except Exception:
                        continue

        full_answer = "".join(assistant_text).strip()
        # cache final answer briefly (e.g., 30s)
        if full_answer:
            r.setex(ck, 30, full_answer)
            await _append_history(r, req.session_id, {"role":"assistant","content":full_answer})

    return StreamingResponse(stream_llm(), media_type="text/plain")