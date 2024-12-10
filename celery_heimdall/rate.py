from typing import List, Optional, Tuple

from redis import Redis
from celery import Task


def check_rate_limits(
    redis_client: Redis,
    rate_limits: List[Tuple[bytes, int, int]],
) -> Optional[float]:

    pipeline = redis_client.pipeline()
    max_delay = 0.0

    # Check each rate limit
    for rate_key, count, period in rate_limits:
        # Get current count and timestamp
        pipeline.get(rate_key)
        pipeline.ttl(rate_key)
        value, ttl = pipeline.execute()

        if value is None:
            # First execution within this period
            pipeline.set(rate_key, 1, ex=period)
            pipeline.execute()
            continue

        count_used = int(value)
        if count_used >= count:
            # Rate limit exceeded, calculate delay
            if ttl > 0:
                delay = float(ttl)
                max_delay = max(max_delay, delay)
            continue

        # Increment counter
        pipeline.incr(rate_key)
        if ttl <= 0:
            pipeline.expire(rate_key, period)
        pipeline.execute()

    return max_delay


def get_rate_limits(
        task: Task, config, args, kwargs
) -> List[Tuple[bytes, int, int]]:
    """
    Convert rate limit configuration into a list of (key, count, period) tuples.

    Args:
        task: The Celery task being rate limited
        config: Task configuration containing rate limit settings
        args: Task arguments
        kwargs: Task keyword arguments

    Returns:
        List of (key, count, period) tuples defining the rate limits
    """
    if config.rate_limit is None:
        return []

    if not isinstance(config.rate_limit, list):
        rate_limits_config = [config.rate_limit]
    else:
        rate_limits_config = config.rate_limit

    rate_limits = []
    for limit_config in rate_limits_config:
        # Get the rate limit values
        if callable(limit_config.rate_limit):
            count, period = limit_config.rate_limit()
        else:
            count, period = limit_config.rate_limit

        # Get the key for this rate limit
        if limit_config.key:
            # Use provided key or key function
            if callable(limit_config.key):
                key = limit_config.key(args, kwargs)
            else:
                key = limit_config.key
        else:
            # Fall back to task's unique key
            key = config.get_key(task, args, kwargs).decode('utf-8')

        # Build the full Redis key with prefix
        full_key = f"{config.get_rate_limit_prefix()}:{key}".encode('utf-8')
        rate_limits.append((full_key, count, period))

    return rate_limits