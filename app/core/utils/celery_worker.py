import asyncio
import nest_asyncio
from celery.schedules import crontab
from celery.utils.log import get_task_logger
from celery.app.control import Inspect
from sqlalchemy.ext.asyncio import create_async_engine, async_sessionmaker

from app.core.utils.celery_app import celery_app
from app.core.config import get_settings
from app.core.analysis.analysis import run_analysis, run_supplier_name_validation
from app.core.analysis.fallback import (
    trigger_from_db_if_needed,
    trigger_from_db_if_needed_validation
)
from app.core.utils.redis_client import rdb, SESSION_SET_KEY, SESSION_VALIDATION_SET_KEY

logger = get_task_logger(__name__)

# Apply nest_asyncio globally
nest_asyncio.apply()

# Helper for safely running async tasks in sync context
def safe_async_run(coro):
    try:
        loop = asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop = asyncio.get_event_loop()
    return loop.run_until_complete(coro)

# Set up Celery Beat schedule
celery_app.conf.beat_schedule = {
    "check-db-and-trigger-every-2-mins": {
        "task": "fallback_trigger",
        "schedule": crontab(minute="*/2"),
    },
    "check-db-and-trigger-validation-every-2-mins": {
        "task": "fallback_validation_trigger",
        "schedule": crontab(minute="*/2"),
    },
}

# Task 1: Process screening session
@celery_app.task(name="process_session", queue="screening_queue")
def process_session(session_id: str):
    logger.info(f"[Screening] Processing session_id: {session_id}")
    before = rdb.smembers(SESSION_SET_KEY)
    logger.info(f"Redis SET BEFORE: {before}")

    async def async_task():
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            await run_analysis({"session_id": session_id}, session)

    try:
        safe_async_run(async_task())
    except Exception as e:
        logger.error(f"Error in process_session: {e}")
        raise

    removed = rdb.srem(SESSION_SET_KEY, session_id)
    after = rdb.smembers(SESSION_SET_KEY)
    logger.info(f"Redis removal success: {removed}")
    logger.info(f"Redis SET AFTER: {after}")
    return f"Done: {session_id}"


# Task 2: Process validation session
@celery_app.task(name="validate_name", queue="name_validation_queue")
def validate_name(session_id: str):
    logger.info(f"[Validation] Processing session_id: {session_id}")
    before = rdb.smembers(SESSION_VALIDATION_SET_KEY)
    logger.info(f"Redis SET BEFORE: {before}")

    async def async_task():
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            await run_supplier_name_validation({"session_id": session_id}, session)

    try:
        safe_async_run(async_task())
    except Exception as e:
        logger.error(f"Error in validate_name: {e}")
        raise

    removed = rdb.srem(SESSION_VALIDATION_SET_KEY, session_id)
    after = rdb.smembers(SESSION_VALIDATION_SET_KEY)
    logger.info(f"Redis removal success: {removed}")
    logger.info(f"Redis SET AFTER: {after}")
    return f"Done: {session_id}"


# Task 3: Fallback trigger for screening_queue
@celery_app.task(name="fallback_trigger", queue="screening_queue")
def fallback_trigger():
    logger.info("[Screening] Checking if queue is empty...")

    inspector: Inspect = celery_app.control.inspect()
    active = inspector.active() or {}
    reserved = inspector.reserved() or {}

    active_count = sum(len(tasks) for tasks in active.values())
    reserved_count = sum(len(tasks) for tasks in reserved.values())
    total_tasks = active_count + reserved_count

    if total_tasks == 0:
        logger.info("[Screening] Queue is empty. Running fallback...")
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)

        async def run_trigger():
            async with async_session() as session:
                await trigger_from_db_if_needed(session)

        try:
            safe_async_run(run_trigger())
        except Exception as e:
            logger.error(f"Fallback trigger error: {e}")
    else:
        logger.info("[Screening] Queue not empty. Skipping fallback.")


# Task 4: Process trigger for name_validation_queue
@celery_app.task(name="process_session_validation", queue="name_validation_queue")
def process_session_validation(session_id: str):
    logger.info(f"[Validation] Processing session_id: {session_id}")
    before = rdb.smembers(SESSION_VALIDATION_SET_KEY)
    logger.info(f"Redis SET BEFORE: {before}")

    async def async_task():
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)
        async with async_session() as session:
            await run_supplier_name_validation({"session_id": session_id}, session)

    try:
        safe_async_run(async_task())
    except Exception as e:
        logger.error(f"Error in process_session_validation: {e}")
        raise

    removed = rdb.srem(SESSION_VALIDATION_SET_KEY, session_id)
    after = rdb.smembers(SESSION_VALIDATION_SET_KEY)
    logger.info(f"Redis removal success: {removed}")
    logger.info(f"Redis SET AFTER: {after}")
    return f"Done: {session_id}"


# Task 5: Fallback trigger for name_validation_queue
@celery_app.task(name="fallback_validation_trigger", queue="name_validation_queue")
def fallback_validation_trigger():
    logger.info("[Validation] Checking if queue is empty...")

    inspector: Inspect = celery_app.control.inspect()
    active = inspector.active() or {}
    reserved = inspector.reserved() or {}

    active_count = sum(len(tasks) for tasks in active.values())
    reserved_count = sum(len(tasks) for tasks in reserved.values())
    total_tasks = active_count + reserved_count

    if total_tasks == 0:
        logger.info("[Validation] Queue is empty. Running fallback...")
        settings = get_settings()
        engine = create_async_engine(settings.sqlalchemy_database_uri, echo=True)
        async_session = async_sessionmaker(engine, expire_on_commit=False)

        async def run_trigger():
            async with async_session() as session:
                await trigger_from_db_if_needed_validation(session)

        try:
            safe_async_run(run_trigger())
        except Exception as e:
            logger.error(f"Fallback validation trigger error: {e}")
    else:
        logger.info("[Validation] Queue not empty. Skipping fallback.")
