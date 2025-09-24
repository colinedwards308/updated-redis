from dotenv import load_dotenv
load_dotenv()
from pydantic import BaseModel
import os

class Settings(BaseModel):
    DATABASE_URL: str = os.getenv("DATABASE_URL", "postgresql+psycopg2://redisdemo:redisdemo@localhost:5432/redisdemo")
    #REDIS_URL: str = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    REDIS_URL: str = os.getenv("REDIS_URL", "redis://redis-12345.cbrown-lab.redisdemo.com:12345")
    CACHE_PREFIX: str = os.getenv("CACHE_PREFIX", "demo")
    # synthetic sizes (can also be overridden in POST /load-sample-data body)
    PRODUCTS: int = int(os.getenv("PRODUCTS", "50000"))
    CLIENTS: int = int(os.getenv("CLIENTS", "20000"))
    CARTS: int = int(os.getenv("CARTS", "15000"))
    MAX_ITEMS_PER_CART: int = int(os.getenv("MAX_ITEMS_PER_CART", "7"))

settings = Settings()