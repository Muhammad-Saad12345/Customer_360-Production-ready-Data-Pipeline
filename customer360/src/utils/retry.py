from __future__ import annotations

from typing import Any, Callable

from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)


def retry_on_exception(max_attempts: int = 5):
    def decorator(fn: Callable[..., Any]) -> Callable[..., Any]:
        return retry(
            reraise=True,
            stop=stop_after_attempt(max_attempts),
            wait=wait_exponential(multiplier=1, min=1, max=60),
            retry=retry_if_exception_type(Exception),
        )(fn)

    return decorator
