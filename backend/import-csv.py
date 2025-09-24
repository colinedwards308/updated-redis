import csv
import json
import logging
from pathlib import Path
from redis import Redis
from redis.exceptions import RedisError
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(name)s - %(message)s',
    handlers=[
        logging.FileHandler('import-csv.log'),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger('CustomerLoader')

# Redis connection settings
REDIS_URL = "redis://redis-12000.cbrown-lab.redisdemo.com:12000"
CACHE_PREFIX = "demo"

def redis_client():
    """Create a Redis client for standalone mode."""
    logger.debug(f"Attempting to connect to Redis at {REDIS_URL}")
    try:
        client = Redis.from_url(REDIS_URL, decode_responses=True)
        # Test connection with a PING
        if client.ping():
            logger.info("Successfully connected to Redis and received PONG")
        else:
            logger.error("Connected to Redis but PING failed")
            raise RedisError("PING failed")
        # Log server info
        server_info = client.info('server')
        logger.debug(f"Redis server info: {server_info}")
        return client
    except RedisError as e:
        logger.error(f"Failed to connect to Redis: {str(e)}")
        raise

def key(category, suffix):
    """Generate a Redis key with the given category and suffix."""
    k = f"{CACHE_PREFIX}:{category}:{suffix.lstrip(':')}"
    logger.debug(f"Generated key: {k}")
    return k

def set_json(redis, key, value, ttl=None):
    """Store a JSON-serialized value in Redis with optional TTL."""
    try:
        logger.debug(f"Setting key {key} with value {json.dumps(value, indent=None)}")
        redis.set(key, json.dumps(value))
        if ttl is not None:
            logger.debug(f"Setting TTL {ttl} seconds for key {key}")
            redis.expire(key, ttl)
        logger.info(f"Successfully set key {key}")
    except RedisError as e:
        logger.error(f"Failed to set key {key}: {str(e)}")
        raise

def delete_prefix(redis, prefix):
    """Delete all keys matching the given prefix."""
    try:
        logger.info(f"Scanning for keys with prefix {prefix}")
        count = 0
        cursor = '0'
        while cursor != '0':
            cursor, keys = redis.scan(cursor=cursor, match=prefix + '*', count=100)
            logger.debug(f"SCAN cursor={cursor} found {len(keys)} keys: {keys}")
            if keys:
                count += redis.delete(*keys)
                logger.debug(f"Deleted {len(keys)} keys")
        logger.info(f"Deleted {count} keys with prefix {prefix}")
        return count
    except RedisError as e:
        logger.error(f"Failed to delete keys with prefix {prefix}: {str(e)}")
        raise

def load_customers_to_redis(csv_path):
    """Load customer data from CSV into Redis."""
    logger.info(f"Starting to load customers from {csv_path}")
    redis = redis_client()
    customers_loaded = 0
    customer_set_key = key("customers", "set")

    # Clear existing customer data (optional, comment out if not desired)
    try:
        logger.info(f"Clearing existing customer data at {customer_set_key}")
        redis.delete(customer_set_key)
        logger.debug(f"Deleted customer set key {customer_set_key}")
        deleted_count = delete_prefix(redis, key("customer", ""))
        logger.info(f"Cleared {deleted_count} existing customer keys")
    except RedisError as e:
        logger.error(f"Failed to clear existing data: {str(e)}")
        raise

    try:
        csv_path = Path(csv_path)
        logger.info(f"Opening CSV file: {csv_path}")
        with csv_path.open('r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            expected_columns = {'id', 'first_name', 'last_name', 'email', 'address', 'city', 'state', 'zip4', 'age'}
            if not expected_columns.issubset(reader.fieldnames):
                logger.error(f"CSV missing required columns. Expected: {expected_columns}, Got: {reader.fieldnames}")
                raise ValueError(f"CSV missing required columns. Expected: {expected_columns}, Got: {reader.fieldnames}")

            for row_num, row in enumerate(reader, start=1):
                logger.debug(f"Processing row {row_num}: {row}")
                # Skip invalid rows (e.g., 'All Others')
                if row['id'].lower() == 'all others' or not row['id']:
                    logger.warning(f"Skipping invalid row {row_num} with id: {row['id']}")
                    continue

                # Create customer dictionary
                try:
                    customer = {
                        "id": row["id"],
                        "first_name": row.get("first_name", "").strip(),
                        "last_name": row.get("last_name", "").strip(),
                        "name": f"{row.get('first_name', '').strip()} {row.get('last_name', '').strip()}".strip(),
                        "email": row.get("email", "").strip(),
                        "address": row.get("address", "").strip(),
                        "city": row.get("city", "").strip(),
                        "state": row.get("state", "").strip(),
                        "zip4": row.get("zip4", "").strip(),
                        "age": int(row.get("age", 0)) if row.get("age") and row["age"].isdigit() else None
                    }
                    logger.debug(f"Created customer object for id {row['id']}: {customer}")
                except Exception as e:
                    logger.error(f"Failed to process customer row {row_num} (id: {row.get('id', 'unknown')}): {str(e)}")
                    continue

                # Store customer in Redis
                customer_key = key("customer", row["id"])
                try:
                    set_json(redis, customer_key, customer, ttl=None)  # No TTL for persistence
                    redis.sadd(customer_set_key, row["id"])
                    customers_loaded += 1
                    logger.info(f"Loaded customer {row['id']} into Redis under key {customer_key}")
                except RedisError as e:
                    logger.error(f"Failed to load customer {row['id']} into Redis under key {customer_key}: {str(e)}")
                    continue

        # Set TTL for the customer set
        try:
            logger.debug(f"Setting TTL 86400 seconds for customer set {customer_set_key}")
            redis.expire(customer_set_key, 86400)  # 24 hours
            logger.info(f"Set TTL for {customer_set_key}")
        except RedisError as e:
            logger.error(f"Failed to set TTL for {customer_set_key}: {str(e)}")

        logger.info(f"Successfully loaded {customers_loaded} customers into Redis from {csv_path}")
        return customers_loaded

    except FileNotFoundError:
        logger.error(f"CSV file not found at {csv_path}")
        return 0
    except ValueError as e:
        logger.error(f"CSV validation error: {str(e)}")
        return 0
    except Exception as e:
        logger.error(f"Unexpected error loading customers: {str(e)}")
        return 0

if __name__ == "__main__":
    csv_path = "/Users/colin.brown/Dropbox/Colin/Projects/redis/updated-redis/datafiles/customers_expanded.csv"  # Adjust path as needed
    logger.info("Starting customer data load script")
    try:
        customers_loaded = load_customers_to_redis(csv_path)
        logger.info(f"Completed. Loaded {customers_loaded} customers.")
    except Exception as e:
        logger.error(f"Script failed: {str(e)}")