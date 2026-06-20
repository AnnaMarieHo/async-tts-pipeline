import functools
import time

def async_logging(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        t1 = time.perf_counter()
        result = await func(*args, **kwargs)
        t2 = time.perf_counter()
        print(f"Total time: {t2-t1}")
    return wrapper

def batch_logger(func):
    @functools.wraps(func)
    async def wrapper(*args, **kwargs):
        result = await func(*args, **kwargs)
        print(f"Batch ID: {args}")
    return wrapper
