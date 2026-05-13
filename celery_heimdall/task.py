import dataclasses
import enum
import hashlib
import inspect
import random
import time
from abc import ABC
from typing import Union, Tuple, Callable

import redis
import redis.lock
import celery
from kombu import serialization
from kombu.utils import uuid

from celery_heimdall.config import Config
from celery_heimdall.errors import AlreadyQueuedError

# Sliding window rate limiter using a sorted set. Each allowed request is
# stored as a scored entry (score = arrival timestamp in ms). On every call:
#   1. Expired entries outside the window are pruned atomically.
#   2. If count < limit the request is recorded and 0 is returned (allowed).
#   3. Otherwise the expiry timestamp of the oldest entry is returned so the
#      caller can compute an exact retry countdown without a second round-trip.
#
# KEYS[1]  - rate limit key
# ARGV[1]  - current time in milliseconds
# ARGV[2]  - window size in milliseconds
# ARGV[3]  - max requests per window
_MOVING_WINDOW_LUA = """
local now    = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit  = tonumber(ARGV[3])

redis.call('ZREMRANGEBYSCORE', KEYS[1], 0, now - window)
local count = redis.call('ZCARD', KEYS[1])

if count < limit then
    redis.call('ZADD', KEYS[1], now, tostring(now) .. '-' .. tostring(count))
    redis.call('PEXPIRE', KEYS[1], window)
    return 0
end

local oldest = redis.call('ZRANGE', KEYS[1], 0, 0, 'WITHSCORES')
return tonumber(oldest[2]) + window
"""


class Strategy(enum.Enum):
    DEFAULT = 10


@dataclasses.dataclass
class RateLimit:
    rate_limit: Union[Tuple, Callable]
    strategy: Strategy = Strategy.DEFAULT


def acquire_lock(task: "HeimdallTask", key: str, timeout: int, *, task_id: str):
    acquired = redis.lock.Lock(
        task.heimdall_redis,
        key,
        timeout=timeout,
        blocking=task.heimdall_config.unique_lock_blocking,
        blocking_timeout=task.heimdall_config.unique_lock_timeout,
    ).acquire(token=task_id)

    if not acquired:
        pipe = task.heimdall_redis.pipeline()
        pipe.get(key)
        pipe.ttl(key)
        task_id, ttl = pipe.execute()

        raise AlreadyQueuedError(
            # TTL may be -1 or -2 if the key didn't exist, depending on the
            # version of Redis.
            expires_in=max(0, ttl),
            likely_culprit=task_id.decode("utf-8") if task_id else None,
        )

    return acquired


def release_lock(task: "HeimdallTask", key: str):
    task.heimdall_redis.delete(key)


def unique_key_for_task(
    task: "HeimdallTask", args, kwargs, *, prefix=""
) -> str:
    """
    Given a task and its arguments, generate a unique key which can be used
    to identify it.
    """
    h = getattr(task, "heimdall", {})

    # When Celery deserializes the arguments for a job, args and kwargs will
    # be `[]` or `{}`, even if they were `None` when serialized. Ensure we
    # do the same here or the hashes will never match when arguments are empty.
    args = args or []
    kwargs = kwargs or {}

    # User specified an explicit key function.
    if "key" in h:
        if callable(h["key"]):
            return prefix + h["key"](args, kwargs)
        return prefix + h["key"]

    # Try to generate a unique key from the arguments given to the task.
    # Most of the cases where this will fail are also cases where Celery
    # will be unable to serialize the job, so we're not too concerned with
    # validation.
    _, _, data = serialization.dumps(
        (args, kwargs),
        # TODO: We should _probably_ use the same serializer as the task.
        "json",
    )

    h = hashlib.md5()
    h.update(task.name.encode("utf-8"))
    h.update(data.encode("utf-8"))
    return f"{prefix}{h.hexdigest()}"


def rate_limited_countdown(task: "HeimdallTask", key, args, kwargs):
    # Based on improvements to Vigrond's original implementation by mlissner
    # on stack overflow.
    h = getattr(task, "heimdall", {})

    if "rate_limit" in h:
        config = h["rate_limit"]

        # Plain callable: (args, kwargs) → None | (key, times, per).
        # None means no rate limit for this invocation. The (key, times, per)
        # form allows dynamic per-invocation limits with custom Redis key scoping.
        if callable(config) and not isinstance(config, RateLimit):
            resolved = config(args, kwargs)
            if resolved is None:
                return 0
            key, times, per = resolved

        else:
            try:
                times, per = config.rate_limit
            except TypeError:
                f = config.rate_limit

                rate_limit_args = {}
                signature = inspect.signature(f)
                if "key" in signature.parameters:
                    rate_limit_args["key"] = key
                if "task" in signature.parameters:
                    rate_limit_args["task"] = task
                if "args" in signature.parameters:
                    rate_limit_args["args"] = args
                if "kwargs" in signature.parameters:
                    rate_limit_args["kwargs"] = kwargs

                times, per = config.rate_limit(**rate_limit_args)
    else:
        times, per = h["times"], h["per"]

    now_ms = int(time.time() * 1000)
    window_ms = per * 1000
    reset_ms = task.heimdall_redis.register_script(_MOVING_WINDOW_LUA)(
        keys=[key],
        args=[now_ms, window_ms, times],
    )

    if reset_ms == 0:
        return 0

    reset_seconds = max(0.0, (reset_ms - now_ms) / 1000.0)
    # Float division instead of per // times. Integer division produces 0 when
    # per < times (e.g. 10 tasks/30s => 10 // 30 = 0), collapsing all excess
    # tasks onto the same retry moment and causing an ever-growing burst each
    # window. Float division + jitter spreads them evenly across the window.
    per_task_spacing = per / times
    if reset_seconds <= 0:
        return random.uniform(1e-3, per_task_spacing)
    return reset_seconds + random.uniform(0, per_task_spacing)


class HeimdallTask(celery.Task, ABC):
    """
    An all-seeing base task for Celery, it provides useful global utilities
    for common Celery behaviors, such as global rate limiting and singleton
    (only one at a time) tasks.
    """

    abstract = True

    def __init__(self):
        super().__init__()
        self._heimdall_config = None
        self._heimdall_redis = None

    @property
    def heimdall_config(self) -> Config:
        if not self._heimdall_config:
            self._heimdall_config = Config(self.app, task=self)
        return self._heimdall_config

    @property
    def heimdall_redis(self) -> redis.Redis:
        if not self._heimdall_redis:
            self._heimdall_redis = self.setup_redis()
        return self._heimdall_redis

    def setup_redis(self) -> redis.Redis:
        """
        Sets up the Redis connection. By default, it'll use any Redis instance
        it can find (in order):

            - the Celery result backend
            - the Celery broker

        If nothing can be found, or if you want to explicitly specify a Redis
        connection you'll need to implement this method yourself, ex:

        .. code::

            from redis import Redis
            from celery_heimdall import HeimdallTask

            class MyHeimdallTask(HeimdallTask):
                def setup_redis(self):
                    return Redis.from_url('redis://')
        """
        # Try to use the Celery result backend, if it's configured for redis.
        backend = self.app.conf.get("result_backend") or ""
        if backend.startswith("redis://"):
            return redis.Redis.from_url(backend)

        # If not the backend, try the broker....
        broker = self.app.conf.get("broker_url") or ""
        if broker.startswith("redis://"):
            return redis.Redis.from_url(broker)

        # Nope, we can't find a usable redis, user will need to implement
        # setup_redis() themselves.
        raise NotImplementedError()

    def apply_async(self, args=None, kwargs=None, task_id=None, **options):
        h = getattr(self, "heimdall", {})
        if h and "unique" in h:
            task_id = task_id or uuid()

            # Task has been configured to be globally unique, so we check for
            # the presence of a global lock before allowing it to be queued.
            try:
                acquire_lock(
                    self,
                    unique_key_for_task(
                        self,
                        args,
                        kwargs,
                        prefix=self.heimdall_config.lock_prefix,
                    ),
                    h.get(
                        "unique_timeout", self.heimdall_config.unique_timeout
                    ),
                    task_id=task_id,
                )
            except AlreadyQueuedError as exc:
                if not self.heimdall_config.unique_raises:
                    # If we were unable to get the task ID for whatever reason,
                    # we just fall through and raise anyway.
                    if exc.likely_culprit is not None:
                        return self.AsyncResult(exc.likely_culprit)

                raise

        # TODO: If we kept track of queued, but not running, tasks, we should
        #       be able to estimate _when_ it would be okay to run a
        #       rate-limited task, rather then just checking when it runs.

        return super().apply_async(
            args=args, kwargs=kwargs, task_id=task_id, **options
        )

    def __call__(self, *args, **kwargs):
        h = getattr(self, "heimdall", {})
        if h and ("per" in h and "times" in h) or "rate_limit" in h:
            delay = rate_limited_countdown(
                self,
                unique_key_for_task(
                    self,
                    args,
                    kwargs,
                    prefix=self.heimdall_config.rate_limit_prefix,
                ),
                args,
                kwargs,
            )
            if delay > 0:
                # Release the unique lock before retrying so the retry's
                # apply_async can re-acquire it. Without this, apply_async sees
                # the lock as still held and silently no-ops the dispatch — the
                # task enters RETRY state but never actually runs again.
                if h and "unique" in h:
                    release_lock(
                        self,
                        unique_key_for_task(
                            self,
                            args,
                            kwargs,
                            prefix=self.heimdall_config.lock_prefix,
                        ),
                    )
                # We don't want our rescheduling retry to count against
                # any normal retry limits the user might have set on the
                # task or globally.
                self.request.retries -= 1
                # Save and restore max_retries around the retry call. Celery
                # reuses one task instance per worker; without restoration,
                # setting max_retries=None here permanently shadows the
                # class-level value for all future invocations on this worker.
                _had_instance_max_retries = "max_retries" in self.__dict__
                _saved_max_retries = self.__dict__.get("max_retries")
                self.max_retries = None
                try:
                    raise self.retry(countdown=delay)
                finally:
                    if _had_instance_max_retries:
                        self.max_retries = _saved_max_retries
                    else:
                        del self.max_retries

        # Normally, we check for uniqueness before calling the task, but if
        # celery beat is being used, it appears to bypass the apply_async
        # method, so we need to check again at run time.
        if h and "unique" in h:
            task_id = self.request.id
            try:
                acquire_lock(
                    self,
                    unique_key_for_task(
                        self,
                        args,
                        kwargs,
                        prefix=self.heimdall_config.lock_prefix,
                    ),
                    h.get(
                        "unique_timeout", self.heimdall_config.unique_timeout
                    ),
                    task_id=task_id,
                )
            except AlreadyQueuedError as exc:
                # If this task is the one holding the lock, we can just
                # continue on and run it.
                if exc.likely_culprit != task_id:
                    # We can't raise an exception here because it breaks
                    # celery's funky custom tracing if an exception occurs
                    # outside of self.run().
                    return

        return self.run(*args, **kwargs)

    def after_return(self, status, retval, task_id, args, kwargs, einfo):
        # Handles post-task cleanup, when a task exits cleanly. This will be
        # called if a task raises an exception (stored in `einfo`), but not
        # if a worker straight up dies (say, because of running out of memory)
        h = getattr(self, "heimdall", {})

        # Cleanup the unique task lock when the task finishes, unless the user
        # told us to wait for the remaining interval.
        if h and "unique" in h and not h.get("unique_wait_for_expiry"):
            release_lock(
                self,
                unique_key_for_task(
                    self, args, kwargs, prefix=self.heimdall_config.lock_prefix
                ),
            )

        super().after_return(status, retval, task_id, args, kwargs, einfo)

    def only_after(self, key: str, seconds: int) -> bool:
        """
        A utility for writing sub-blocks in tasks that only execute if
        `seconds` has passed since the last time it was run.

        Imagine you have a task that runs every 5 minutes, but there's one line
        in that task you only want to run after at least an hour. You'd use
        `only_after` to accomplish that.
        """
        task_id = getattr(self.request, "id", uuid())
        return bool(
            redis.lock.Lock(
                self.heimdall_redis,
                key,
                timeout=seconds,
                blocking=self.heimdall_config.unique_lock_blocking,
                blocking_timeout=self.heimdall_config.unique_lock_timeout,
            ).acquire(token=task_id)
        )
