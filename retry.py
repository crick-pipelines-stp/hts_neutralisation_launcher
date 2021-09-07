"""
stolen from:
https://gist.github.com/FBosler/be10229aba491a8c912e3a1543bbc74e
"""

from functools import wraps
import time


def retry(exceptions, total_tries=3, initial_wait=3.0, backoff_factor=2):
    """
    calling the decorated function applying an exponential backoff.
    Arguments
    ----------
        exceptions: Exception(s) that trigger a retry, can be a tuple
        total_tries: Total tries
        initial_wait: Time to first retry
        backoff_factor: Backoff multiplier (e.g. value of 2 will double the delay each retry).
        logger: logger to be used, if none specified print
    """
    def retry_decorator(f):
        @wraps(f)
        def func_with_retries(*args, **kwargs):
            tries = total_tries + 1
            wait = initial_wait
            while tries > 1:
                try:
                    return f(*args, **kwargs)
                except exceptions as e:
                    tries -= 1
                    print_args = args if args else "no args"
                    if tries == 1:
                        msg = str(
                            f"Function: {f.__name__}\n"
                            f"Failed despite best efforts after {total_tries} tries.\n"
                            f"args: {print_args}, kwargs: {kwargs}"
                        )
                        print(msg)
                        raise
                    msg = str(
                        f"Function: {f.__name__}\n"
                        f"Exception: {e}\n"
                        f"Retrying in {wait} seconds!, args: {print_args}, kwargs: {kwargs}\n"
                    )
                    print(msg)
                    time.sleep(wait)
                    wait *= backoff_factor

        return func_with_retries
    return retry_decorator
