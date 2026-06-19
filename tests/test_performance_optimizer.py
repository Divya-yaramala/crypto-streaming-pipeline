import time
from unittest.mock import MagicMock

from consumer.performance_optimizer import (
    batch_insert_postgres,
    parallel_process_events,
    run_benchmark,
    timer_decorator,
)


def test_timer_decorator_executes_function():
    @timer_decorator
    def add(a, b):
        return a + b

    assert add(2, 3) == 5


def test_parallel_process_events_returns_all():
    events = [{"id": i} for i in range(10)]

    def identity(event):
        return event

    results = parallel_process_events(events, identity, max_workers=3)
    assert len(results) == 10


def test_batch_insert_postgres_inserts_rows():
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor
    rows = [("bitcoin", i * 100.0) for i in range(5)]
    sql = "INSERT INTO crypto_prices (crypto_id, price_usd) VALUES (%s, %s)"
    inserted = batch_insert_postgres(rows, sql, mock_conn, batch_size=3)
    assert inserted == 5
    assert mock_cursor.executemany.call_count == 2


def test_run_benchmark_returns_structure():
    def fast_func(event):
        return event

    events = [{"id": i} for i in range(5)]
    result = run_benchmark(events, fast_func)
    assert "sequential_seconds" in result
    assert "parallel_seconds" in result
    assert "speedup" in result


def test_parallel_faster_than_sequential_for_slow_tasks():
    def slow_func(event):
        time.sleep(0.05)
        return event

    events = [{"id": i} for i in range(6)]
    result = run_benchmark(events, slow_func)
    assert result["parallel_seconds"] < result["sequential_seconds"]
