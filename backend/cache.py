# backend/cache.py
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
    if v:
        try:
            obj = json.loads(v)
            preview = str(obj) if len(str(obj)) < 500 else str(obj)[:500] + "...(truncated)"
            print(f"[get_json] HIT key={k!r} -> {preview}")
            return obj
        except Exception as e:
            print(f"[get_json] ERROR decoding key={k!r}: {e}")
            return None
    else:
        print(f"[get_json] MISS key={k!r}")
        return None

def set_json(r: Redis, k: str, value, ttl: int | None = None):
    data = json.dumps(value, separators=(",", ":"))
    preview = data if len(data) < 500 else data[:500] + "...(truncated)"
    print(f"[set_json] key={k!r} ttl={ttl} value_preview={preview}")
    if ttl:
        r.setex(k, ttl, data)
    else:
        r.set(k, data)

def delete_prefix(r: Redis, prefix: str):
    patt = prefix + "*"
    count = 0
    pipe = r.pipeline()
    for kk in r.scan_iter(match=patt, count=1000):
        print(f"[delete_prefix] deleting key={kk!r}")
        pipe.delete(kk)
        count += 1
    pipe.execute()
    print(f"[delete_prefix] done prefix={prefix!r}, deleted={count}")