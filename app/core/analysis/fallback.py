from sqlalchemy import select
from sqlalchemy.exc import SQLAlchemyError
from app.models import STATUS, Base
from app.core.utils.redis_client import (
    rdb,
    SESSION_SET_KEY,
    SESSION_VALIDATION_SET_KEY
)
from app.schemas.logger import logger

# === SCREENING fallback ===
async def trigger_from_db_if_needed(session):
    try:
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist.")

        query = select(table_class.c.session_id).where(
            table_class.c.screening_analysis_status == STATUS.QUEUED.value
        )
        result = await session.execute(query)
        session_ids = [row[0] for row in result.fetchall()]

        requeued = []
        for sid in session_ids:
            if not rdb.sismember(SESSION_SET_KEY, sid):
                rdb.sadd(SESSION_SET_KEY, sid)

                # Lazy import to avoid circular import
                from app.core.utils.celery_worker import process_session
                process_session.delay(sid)

                logger.info(f"🔁 Requeued to screening_queue: {sid}")
                requeued.append(sid)

        return requeued

    except (ValueError, SQLAlchemyError, Exception) as e:
        logger.error(f"❌ Screening fallback error: {e}")
        return []


# === VALIDATION fallback ===
async def trigger_from_db_if_needed_validation(session):
    try:
        table_class = Base.metadata.tables.get("session_screening_status")
        if table_class is None:
            raise ValueError("Table 'session_screening_status' does not exist.")

        query = select(table_class.c.session_id).where(
            table_class.c.supplier_name_validation_status == STATUS.QUEUED.value
        )
        result = await session.execute(query)
        session_ids = [row[0] for row in result.fetchall()]

        requeued = []
        for sid in session_ids:
            if not rdb.sismember(SESSION_VALIDATION_SET_KEY, sid):
                rdb.sadd(SESSION_VALIDATION_SET_KEY, sid)

                # Lazy import to avoid circular import
                from app.core.utils.celery_worker import validate_name
                validate_name.delay(sid)

                logger.info(f"🔁 Requeued to name_validation_queue: {sid}")
                requeued.append(sid)

        return requeued

    except (ValueError, SQLAlchemyError, Exception) as e:
        logger.error(f"❌ Validation fallback error: {e}")
        return []
