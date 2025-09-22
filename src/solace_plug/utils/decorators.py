import functools
import asyncio
import time
import random
from typing import Callable, Any, Type, Tuple
from solace_plug.log import log


def retry_on_failure(
    max_attempts: int = 3,
    backoff_factor: float = 2.0,
    max_backoff: float = 60.0,
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True
):
    """
    Decorator that adds retry logic for SYNC functions.
    
    Args:
        max_attempts: Maximum number of attempts (including first try)
        backoff_factor: Multiplier for exponential backoff  
        max_backoff: Maximum delay between retries in seconds
        exceptions: Tuple of exception types to retry on
        jitter: Add random jitter to prevent thundering herd
        
    Example:
        @retry_on_failure(max_attempts=3, backoff_factor=2.0)
        def connect_to_database():
            # This function will retry up to 3 times
            return psycopg2.connect("postgresql://...")
    """
    def decorator(func: Callable) -> Callable:
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
    exceptions: Tuple[Type[Exception], ...] = (Exception,),
    jitter: bool = True
):
    """
    Decorator that adds retry logic for ASYNC functions.
    
    Args:
        max_attempts: Maximum number of attempts (including first try)
        backoff_factor: Multiplier for exponential backoff  
        max_backoff: Maximum delay between retries in seconds
        exceptions: Tuple of exception types to retry on
        jitter: Add random jitter to prevent thundering herd
        
    Example:
        @retry_on_failure_async(max_attempts=3, backoff_factor=2.0)
        async def connect_to_solace():
            # This async function will retry up to 3 times
            client = AsyncSolaceClient()
            await client.connect()
            return client
    """
    def decorator(func: Callable) -> Callable:
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
