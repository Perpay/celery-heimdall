import time

import pytest
from celery import shared_task

from celery_heimdall import HeimdallTask, RateLimit, HeimdallConfig



@shared_task(
    base=HeimdallTask,
    heimdall=HeimdallConfig(
        rate_limit=RateLimit((2, 10))
    )
)
def tuple_rate_limit_task():
    pass


@shared_task(
    base=HeimdallTask,
    heimdall=HeimdallConfig(
        rate_limit=RateLimit(lambda *args, **kwargs: (2, 10))
    )
)
def callable_rate_limit_task():
    pass


@shared_task(
    base=HeimdallTask,
    heimdall=HeimdallConfig(
        rate_limit=[
            RateLimit((2, 30), key="global"),
            RateLimit((1, 10))
        ]
    )
)
def multiple_rate_limit_task(key: str):
    return key


@pytest.mark.parametrize('func', [
    tuple_rate_limit_task,
    callable_rate_limit_task
])
def test_default_rate_limit(celery_session_worker, func):
    """
    Ensure that rate limiting works as expected.
    """
    start = time.time()
    # Immediate
    task1 = func.apply_async()
    # Immediate
    task2 = func.apply_async()
    # After at least 10 seconds
    task3 = func.apply_async()
    # After at least 10 seconds
    task4 = func.apply_async()
    # After at least 20 seconds
    task5 = func.apply_async()
    # After at least 20 seconds
    task6 = func.apply_async()

    task1.get()
    task2.get()

    elapsed = time.time() - start
    assert elapsed < 2

    task3.get()
    task4.get()

    elapsed = time.time() - start
    assert 10 < elapsed < 20

    task5.get()
    task6.get()

    elapsed = time.time() - start
    assert 20 < elapsed < 30


def test_multiple_rate_limit(celery_session_worker):
    """
    Ensure that rate limiting works as expected when multiple rate limits
    are configured.
    """
    start = time.time()

    # Since both task1 and task2 use distinct arguments and are using the
    # default key, they will run immediately.
    task1 = multiple_rate_limit_task.delay("t1")
    task2 = multiple_rate_limit_task.delay("t2")
    # ... but task3 will be delayed by the global rate limit.
    task3 = multiple_rate_limit_task.delay("t3")
    # ... and task4 will be delayed by the global rate limit.
    task4 = multiple_rate_limit_task.delay("t2")

    task1.get()
    task2.get()

    elapsed = time.time() - start
    assert elapsed < 5

    task3.get()
    elapsed = time.time() - start
    assert 30 < elapsed < 40

    task4.get()
    elapsed = time.time() - start
    assert 30 < elapsed < 40