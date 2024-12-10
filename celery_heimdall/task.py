import hashlib
from abc import ABC
from dataclasses import dataclass
from enum import Enum
from functools import cache, cached_property
from typing import Callable

import celery
import redis
from celery import Celery
from kombu import serialization
from kombu.utils import uuid

from . import lock
from .errors import AlreadyQueuedError
from .rate import get_rate_limits, check_rate_limits


class RateLimitStrategy(Enum):
    """
    Strategies for rate limiting tasks.
    """

    DEFAULT = 10


@dataclass
class RateLimit:
    """
    A rate limit configuration for a HeimdallTask.
    """

    #: The rate limit to apply to the task. Can be a tuple in the form of
    #: (times, per) or a callable that returns the tuple.
    rate_limit: tuple[int, int] | Callable[..., tuple[int, int]]
    #: The key to use for rate limiting. If not provided, the key will be
    #: taken from the unique key of the task.
    key: str | None = None
    #: The strategy to use for rate limiting.
    strategy: RateLimitStrategy = RateLimitStrategy.DEFAULT


@cache
def _cached_redis(app: Celery) -> redis.Redis | None:
    backend = app.conf.get("result_backend", "")
    if backend.startswith("redis://"):
        return redis.Redis.from_url(backend)

    broker = app.conf.get("broker_url", "")
    if broker.startswith("redis://"):
        return redis.Redis.from_url(broker)


@dataclass
class HeimdallConfig:
    """
    Configuration options for a HeimdallTask.
    """

    #: If True, the task will be globally unique, allowing only one instance
    #: to run or be queued at a time.
    unique: bool = False
    #: If True, the lock will be acquired before the task is queued.
    unique_early: bool = True
    #: If True, the lock will be acquired when the task is started.
    unique_late: bool = True
    #: If True, the task will raise an exception if it's already queued.
    unique_raises: bool = False
    #: The amount of time to wait before allowing the task lock to expire,
    #: even if the task has not yet completed.
    unique_expiry: int = 60 * 100
    #: If True, the task will wait for the lock to expire instead of releasing
    #: it, even if the task has already completed. This can be used to easily
    #: implement tasks that should only run once per interval.
    unique_wait_for_expiry: bool = False
    #: A user-provided unique key for the task. If not specified, a unique
    #: key will be generated from the task's arguments.
    key: str | Callable[..., str] = None
    #: The rate limit to apply to the task. Can optionally be a list of
    #: rate limits which will be applied in order.
    rate_limit: RateLimit | list[RateLimit] | None = None

    #: The default prefix to use for the task lock key.
    lock_prefix: str = "h-lock"
    #: The default prefix to use for the rate limit key.
    rate_limit_prefix: str = "h-rate"

    def get_redis(self, app: Celery) -> redis.Redis:
        """
        Get a Redis instance for the task.

        The resulting instance will be used to store task locks and rate
        limiting information. It's important that the instance is shared
        across all workers to ensure that the locks and rate limiting are
        effective.

        If not implemented by the user, the default implementation will
        try to get the Redis instance from the Celery app, if available.
        """
        if (client := _cached_redis(app)) is not None:
            return client

        raise NotImplementedError(
            "No Redis instance available from the Celery configuration and no"
            " get_redis() implemented."
        )

    def get_lock_prefix(self) -> str:
        """
        Get the prefix to use for the task lock key.

        The prefix is used to namespace the lock key to avoid conflicts with
        other locks that may be sharing the same Redis instance.
        """
        return self.lock_prefix

    def get_rate_limit_prefix(self) -> str:
        """
        Get the prefix to use for the rate limit key.

        The prefix is used to namespace the rate limit key to avoid conflicts
        with other rate limits that may be sharing the same Redis instance.
        """
        return self.rate_limit_prefix

    def get_key(self, task: celery.Task, args, kwargs) -> bytes:
        """
        Get the unique key for the task.

        If the key is a callable, it will be called with the task's arguments
        and keyword arguments to generate the key.
        """
        # When Celery deserializes the arguments for a job, args and kwargs will
        # be `[]` or `{}`, even if they were `None` when serialized. Ensure we
        # do the same here or the hashes will never match when arguments are
        # empty.
        args = args or []
        kwargs = kwargs or {}

        if not self.key:
            # No key was provided, so we'll generate a reasonably-unique key
            # from the task and its arguments.
            _, _, data = serialization.dumps((args, kwargs), serializer="json")

            h = hashlib.md5()
            h.update(task.name.encode("utf-8"))
            h.update(data.encode("utf-8"))
            k = h.hexdigest()
        elif callable(self.key):
            # If the key is a callable, we'll call it with the task's arguments
            # and keyword arguments to allow the user to generate their own
            # keys.
            k = self.key(args, kwargs)
        else:
            # Otherwise it should just be a simple string.
            k = self.key

        return f"{self.get_lock_prefix()}:{k}".encode("utf-8")


class HeimdallTask(celery.Task, ABC):
    """
    A base task for Celery that adds helpful features such as global rate
    limiting and unique task execution.

    These features are built on top of Redis and are designed to be
    distributed across multiple workers using a shared Redis instance.
    """

    abstract = True

    def __call__(self, *args, **kwargs):
        if self.h_config.rate_limit is not None:
            rate_limits = get_rate_limits(self, self.h_config, args, kwargs)
            if rate_limits:
                delay = check_rate_limits(self.h_redis, rate_limits)
                if delay > 0:
                    # We don't want our rescheduling retry to count against
                    # any normal retry limits the user might have set on the
                    # task or globally.
                    self.request.retries -= 1
                    # Max retries needs to be set to None _before_ calling
                    # retry(). This value will not propagate, allowing the user's
                    # normal retry behaviour to apply on the next call.
                    self.max_retries = None
                    # Retrying with a future ETA is flawed in Celery. Celery
                    # will schedule these by pulling them into worker memory
                    # which can cause massive problems on busy queues.
                    raise self.retry(countdown=delay)

        # Acquire a globally unique lock for this task at the time it's
        # executed.
        if self.h_config.unique and self.h_config.unique_late:
            key = self.h_config.get_key(self, args, kwargs)

            token = lock.lock(
                self.h_redis,
                key,
                token=self.request.id.encode("utf-8"),
                expiry=self.h_config.unique_expiry,
            ).decode("utf-8")

            if not token == self.request.id:
                raise AlreadyQueuedError(token)

        return self.run(*args, **kwargs)

    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        if self.h_config.unique and self.h_config.unique_early:
            # Acquire a globally unique lock for this task before it's queued.
            # In some cases, this function may not be called, such as by
            # send_task() or non-standard task execution.
            task_id: str = task_id or uuid()
            key = self.h_config.get_key(self, args, kwargs)

            token = lock.lock(
                self.h_redis,
                key,
                token=task_id.encode("utf-8"),
                expiry=self.h_config.unique_expiry,
            ).decode("utf-8")

            if not token == task_id:
                if not self.h_config.unique_raises:
                    # If the task is not configured to raise an exception when
                    # it's already queued, we'll just return the task ID of the
                    # task that already holds the lock.
                    if token is not None:
                        return self.AsyncResult(token)

                raise AlreadyQueuedError(token)

        return super().apply_async(
            args=args, kwargs=kwargs, task_id=task_id, **options
        )

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        # Handles post-task cleanup, when a task exits cleanly. This will be
        # called if a task raises an exception (stored in `einfo`), but not
        # if a worker straight up dies (say, because of running out of memory)

        if self.h_config.unique and not self.h_config.unique_wait_for_expiry:
            key = self.h_config.get_key(self, args, kwargs)
            # It's not an error for our lock to have already been cleared by
            # another token, because our token may have expired.
            lock.release(self.h_redis, key, token=task_id.encode("utf-8"))

        super().after_return(status, retval, task_id, args, kwargs, einfo)

    @cached_property
    def h_config(self) -> HeimdallConfig:
        """
        Heimdall's configuration for the current task.
        """
        return getattr(self, "heimdall", HeimdallConfig())

    @cached_property
    def h_redis(self) -> redis.Redis:
        """
        Heimdall's cached redis connection.
        """
        return self.h_config.get_redis(self.app)

    def h_only_after(self, key: str, seconds: int) -> bool:
        """
        Only returns true if either the key has expired in the cache or if
        the current task already holds the key.

        This can be used to gate parts of a task that should only be run
        once per time period.

        .. code-block:: python

        @shared_task(bind=True, task=HeimdallTask)
        def my_task(self, *args, **kwargs):
            if self.only_after("my_task", 60 * 60):
                print(
                    "It's been at least an hour since this task was"
                    " last run."
                )
        """

        token = self.request.id.encode("utf-8")
        return lock.lock(
            self.h_redis,
            key.encode("utf-8"),
            token=token,
            expiry=seconds,
        ) == token

    def h_extend_lock(self, milliseconds: int):
        """
        Extends the expiry on the lock for the current task by the given number
        of milliseconds.
        """
        if not self.h_config.unique:
            raise ValueError("Task is not configured to have a unique lock")

        key = self.h_config.get_key(
            self, self.request.args, self.request.kwargs
        )

        return lock.extend(
            self.h_redis,
            key,
            self.request.id.encode("utf-8"),
            milliseconds,
            replace=False,
        )
