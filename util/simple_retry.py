import functools
import logging


def simple_retry(max_retries: int):
    def decorator_simple_retry(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for _ in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except RuntimeError:
                    logging.error(
                        f'Error occurred in {func.__name__}. Beginning retry #{_ + 1}...'
                    )
                    continue
            raise RuntimeError(f'Ran out of retries after {max_retries} attempts')

        return wrapper

    return decorator_simple_retry
