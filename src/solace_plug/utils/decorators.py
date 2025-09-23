import functools
import asyncio
import time
import random
import typing as t
import inspect
import logging

log = logging.getLogger("solace_plug")

def retry_on_failure(
    func: t.Callable | None = None,
    *,
    max_attempts: int = 3,
    backoff_factor: float = 5.0,
    max_backoff: float = 60.0,
    exceptions: t.Tuple[t.Type[Exception], ...] = (Exception,),
    jitter: bool = True
):
    """
    Decorator that adds retry logic for both SYNC and ASYNC functions.

    Args:
        max_attempts: How many times to try the function (including the first attempt).
                     
        backoff_factor: How much to multiply the delay after each failure.
                       Example: backoff_factor=2.0 means double the wait time each retry.
                       Attempt 1 fails → wait 1 second
                       Attempt 2 fails → wait 2 seconds  
                       Attempt 3 fails → wait 4 seconds
                       
        max_backoff: Maximum seconds to wait between retries (prevents delays from growing too large).
                    
        exceptions: Tuple of exception types that should trigger a retry.
                   
        jitter: Add randomness to delay times to prevent thundering herd.
               Example: Instead of all services waiting exactly 4 seconds,
               they'll wait between 2-4 seconds (spread out).
        
    Example:
        @retry_on_failure(max_attempts=3, backoff_factor=2.0)
        def connect_to_database():
            # This function will retry up to 3 times with exponential backoff
            return psycopg2.connect("postgresql://...")
            
        # If connection fails:
        # Try 1: Immediate attempt
        # Try 2: Wait ~1 second, then try again  
        # Try 3: Wait ~2 seconds, then try again
        # If all fail: Give up and raise the last exception
    """
    def decorator(inner_func: t.Callable) -> t.Callable:
        if inspect.iscoroutinefunction(inner_func):
            # Async wrapper
            @functools.wraps(inner_func)
            async def async_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(max_attempts):
                    try:
                        return await inner_func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt == max_attempts - 1:
                            break
                        delay = min(backoff_factor ** attempt, max_backoff)
                        if jitter:
                            delay *= (0.5 + random.random() * 0.5)
                        log.warning(
                            "Attempt %d/%d failed for %s: %s. Retrying in %.2fs...",
                            attempt + 1, max_attempts, inner_func.__name__, str(e), delay
                        )
                        await asyncio.sleep(delay)
                log.error("All %d attempts failed for %s", max_attempts, inner_func.__name__)
                raise last_exception
            return async_wrapper
            
        else:
            # Sync wrapper
            @functools.wraps(inner_func)
            def sync_wrapper(*args, **kwargs):
                last_exception = None
                for attempt in range(max_attempts):
                    try:
                        return inner_func(*args, **kwargs)
                    except exceptions as e:
                        last_exception = e
                        if attempt == max_attempts - 1:
                            break
                        delay = min(backoff_factor ** attempt, max_backoff)
                        if jitter:
                            delay *= (0.5 + random.random() * 0.5)
                        log.warning(
                            "Attempt %d/%d failed for %s: %s. Retrying in %.2fs...",
                            attempt + 1, max_attempts, inner_func.__name__, str(e), delay
                        )
                        time.sleep(delay)
                log.error("All %d attempts failed for %s", max_attempts, inner_func.__name__)
                raise last_exception
            return sync_wrapper

    if func is not None and callable(func):
        return decorator(func)
    return decorator