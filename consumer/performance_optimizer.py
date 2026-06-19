import logging
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from functools import wraps
from typing import Callable, List

logger = logging.getLogger(__name__)


def timer_decorator(func: Callable) -> Callable:
    @wraps(func)
    def wrapper(*args, **kwargs):
        start = time.time()
        result = func(*args, **kwargs)
        duration = time.time() - start
        logger.info("Function %s completed in %.4fs", func.__name__, duration)
        return result

    return wrapper


def parallel_process_events(
    events: List[dict], process_func: Callable, max_workers: int = 5
) -> List[dict]:
    results = []
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {executor.submit(process_func, event): event for event in events}
        for future in as_completed(futures):
            try:
                results.append(future.result())
            except Exception as exc:
                logger.warning("parallel_process_events worker failed: %s", exc)
    return results


def batch_insert_postgres(rows: list, insert_sql: str, conn, batch_size: int = 100) -> int:
    inserted = 0
    cursor = conn.cursor()
    try:
        for i in range(0, len(rows), batch_size):
            batch = rows[i : i + batch_size]
            cursor.executemany(insert_sql, batch)
            conn.commit()
            inserted += len(batch)
            logger.info("Inserted batch %d rows (total %d)", len(batch), inserted)
    except Exception as exc:
        conn.rollback()
        logger.error("batch_insert_postgres failed: %s", exc)
    finally:
        cursor.close()
    return inserted


def run_benchmark(events: List[dict], process_func: Callable) -> dict:
    start_seq = time.time()
    for event in events:
        process_func(event)
    sequential_seconds = time.time() - start_seq

    start_par = time.time()
    parallel_process_events(events, process_func)
    parallel_seconds = time.time() - start_par

    speedup = sequential_seconds / parallel_seconds if parallel_seconds > 0 else 0.0
    return {
        "sequential_seconds": round(sequential_seconds, 4),
        "parallel_seconds": round(parallel_seconds, 4),
        "speedup": round(speedup, 2),
    }
