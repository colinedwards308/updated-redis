# backend/cache.py (only if you need it)
import json
from redis import Redis
from .settings import settings

def redis_client() -> Redis:
    return Redis.from_url(settings.REDIS_URL, decode_responses=True)

def key(*parts: str) -> str:
    # namespaced keys, e.g., demo:report:top:30:10
    return f"{settings.CACHE_PREFIX}:" + ":".join(p.strip(":") for p in parts if p is not None)

def get_json(r: Redis, k: str):
    v = r.get(k)
    return json.loads(v) if v else None

def set_json(r: Redis, k: str, value, ttl: int | None = None):
    data = json.dumps(value, separators=(",", ":"))
    if ttl:
        r.setex(k, ttl, data)
    else:
        r.set(k, data)

def delete_prefix(r: Redis, prefix: str):
    patt = prefix + "*"
    pipe = r.pipeline()
    for kk in r.scan_iter(match=patt, count=1000):
        pipe.delete(kk)
    pipe.execute()