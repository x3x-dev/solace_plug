import functools
import asyncio
import time
import random
import typing as t
from solace_plug.log import log


def retry_on_failure(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    max_backoff: float = 60.0,
    exceptions: t.Tuple[t.Type[Exception], ...] = (Exception,),
    jitter: bool = True
):
    """
    Decorator that adds retry logic for SYNC functions.
    
    When a function fails, this decorator will automatically retry it multiple times
    with increasing delays between attempts (exponential backoff).
    
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
    def decorator(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    # Don't wait after the last attempt
                    if attempt == max_attempts - 1:
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = min(backoff_factor ** attempt, max_backoff)
                    if jitter:
                        delay *= (0.5 + random.random() * 0.5)
                    
                    log.warning(
                        "Attempt %d/%d failed for %s: %s. Retrying in %.2fs...",
                        attempt + 1, max_attempts, func.__name__, str(e), delay
                    )
                    
                    time.sleep(delay)  # Sync sleep
            
            # All attempts failed
            log.error("All %d attempts failed for %s", max_attempts, func.__name__)
            raise last_exception
        
        return wrapper
    return decorator


def retry_on_failure_async(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    max_backoff: float = 60.0,
    exceptions: t.Tuple[t.Type[Exception], ...] = (Exception,),
    jitter: bool = True
):
    """
    Decorator that adds retry logic for ASYNC functions.
    
    When an async function fails, this decorator will automatically retry it multiple times
    with increasing delays between attempts (exponential backoff).
    
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
        @retry_on_failure_async(max_attempts=3, backoff_factor=2.0)
        async def connect_to_solace():
            # This async function will retry up to 3 times with exponential backoff
            client = AsyncSolaceClient()
            await client.connect()
            return client
            
        # If connection fails:
        # Try 1: Immediate attempt
        # Try 2: Wait ~1 second, then try again  
        # Try 3: Wait ~2 seconds, then try again
        # If all fail: Give up and raise the last exception
    """
    def decorator(func: t.Callable) -> t.Callable:
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            last_exception = None
            
            for attempt in range(max_attempts):
                try:
                    return await func(*args, **kwargs)
                    
                except exceptions as e:
                    last_exception = e
                    
                    # Don't wait after the last attempt
                    if attempt == max_attempts - 1:
                        break
                    
                    # Calculate delay with exponential backoff
                    delay = min(backoff_factor ** attempt, max_backoff)
                    if jitter:
                        delay *= (0.5 + random.random() * 0.5)
                    
                    log.warning(
                        "Attempt %d/%d failed for %s: %s. Retrying in %.2fs...",
                        attempt + 1, max_attempts, func.__name__, str(e), delay
                    )
                    
                    await asyncio.sleep(delay)  # Async sleep
            
            # All attempts failed
            log.error("All %d attempts failed for %s", max_attempts, func.__name__)
            raise last_exception
        
        return async_wrapper
    return decorator
