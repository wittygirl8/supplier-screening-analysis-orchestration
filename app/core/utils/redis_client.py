import redis
import os
from dotenv import load_dotenv

load_dotenv()

REDIS_URL = "redis://redis:6379/0"
SESSION_SET_KEY = "queued_session_ids"
SESSION_VALIDATION_SET_KEY = "queued_name_validation_ids"

rdb = redis.Redis.from_url(REDIS_URL, decode_responses=True)
