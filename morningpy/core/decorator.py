import asyncio
import time
import functools
import logging
from functools import wraps
from pathlib import Path
import json
from morningpy.core.config import EXTRACTOR_CLASS_FUNC

def retry(
    max_retries: int = 3,
    backoff_factor: float = 2,
    exceptions: tuple = (Exception,),
):
    """
    Retry decorator supporting both synchronous and asynchronous functions,
    with exponential backoff.

    Parameters
    ----------
    max_retries : int, optional
        Maximum number of attempts before the exception is raised.
        Defaults to 3.
    backoff_factor : float, optional
        Base multiplier for exponential backoff. The waiting time is computed as::

            wait_time = backoff_factor ** attempt

        Defaults to 2.
    exceptions : tuple of Exception types, optional
        Tuple of exception classes that should trigger a retry.
        Defaults to ``(Exception,)``.

    Returns
    -------
    callable
        A decorator that wraps the target function and applies retry logic.

    Notes
    -----
    - Works for both sync and async functions.
    - Logs retry attempts at WARNING level.
    - The function sleeps using ``time.sleep`` for synchronous functions
      and ``asyncio.sleep`` for asynchronous ones.

    Examples
    --------
    >>> @retry(max_retries=5, backoff_factor=1.5)
    ... def fetch_data():
    ...     ...

    >>> @retry(exceptions=(ValueError,))
    ... async def fetch_async():
    ...     ...
    """
    def decorator(func):
        @functools.wraps(func)
        async def async_wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            for attempt in range(1, max_retries + 1):
                try:
                    return await func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    wait_time = backoff_factor ** attempt
                    logger.warning(
                        f"[ASYNC RETRY] {func.__name__} failed "
                        f"({attempt}/{max_retries}): {e}. Retrying in {wait_time}s..."
                    )
                    await asyncio.sleep(wait_time)

        @functools.wraps(func)
        def sync_wrapper(*args, **kwargs):
            logger = logging.getLogger(func.__module__)
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except exceptions as e:
                    if attempt == max_retries:
                        raise
                    wait_time = backoff_factor ** attempt
                    logger.warning(
                        f"[SYNC RETRY] {func.__name__} failed "
                        f"({attempt}/{max_retries}): {e}. Retrying in {wait_time}s..."
                    )
                    time.sleep(wait_time)

        return async_wrapper if asyncio.iscoroutinefunction(func) else sync_wrapper

    return decorator

def save_api_response(activate: bool = False):
    """
    Decorator to save raw API responses in BaseClient.fetch_all.

    Parameters
    ----------
    activate : bool
        If True, saving is enabled. Otherwise, does nothing.
    """
    if not activate:
        def noop_decorator(func):
            return func
        return noop_decorator

    package_dir = Path(__file__).resolve().parent.parent
    fixture_dir = package_dir / "data" / "fixture"
    fixture_dir.mkdir(parents=True, exist_ok=True)

    def decorator(func):
        @wraps(func)
        async def wrapper(self, session, requests, *args, **kwargs):
            responses = await func(self, session, requests, *args, **kwargs)

            # Save the last response only
            if responses:
                res = responses[0]
                cls_name = self.__class__.__name__
                func_name = EXTRACTOR_CLASS_FUNC[cls_name]
                try:
                    file_path = fixture_dir / f"{func_name}_response.json"
                    with open(file_path, "w", encoding="utf-8") as f:
                        json.dump(res, f, ensure_ascii=False, indent=2)
                except Exception as e:
                    self.logger.error(f"Failed to save API response: {e}")

            return responses
        return wrapper
    return decorator

def save_dataframe_mock(activate: bool = False):
    """
    Decorator for BaseExtractor._call_api to save the final DataFrame (first 5 rows) for testing.

    Parameters
    ----------
    activate : bool
        If True, saving is enabled. Otherwise, does nothing.
    """
    if not activate:
        def noop_decorator(func):
            return func
        return noop_decorator

    package_dir = Path(__file__).resolve().parent.parent
    mock_dir = package_dir / "data" / "mock"
    mock_dir.mkdir(parents=True, exist_ok=True)

    def decorator(func):
        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            df = await func(self, *args, **kwargs)

            if df is not None and not df.empty:
                try:
                    cls_name = self.__class__.__name__
                    func_name = EXTRACTOR_CLASS_FUNC[cls_name]
                    file_path = mock_dir / f"{func_name}_mock.csv"
                    df.head(5).to_csv(file_path, index=False)
                except Exception as e:
                    self.client.logger.error(f"Failed to save DataFrame: {e}")

            return df
        return wrapper
    return decorator

def save_api_request(activate: bool = False):
    if not activate:
        def noop_decorator(func):
            return func
        return noop_decorator

    package_dir = Path(__file__).resolve().parent.parent
    request_dir = package_dir / "data" / "request"
    request_dir.mkdir(parents=True, exist_ok=True)

    def decorator(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):

            requests = func(self, *args, **kwargs)

            if not requests:
                return requests

            first = requests[0]

            try:
                cls_name = self.__class__.__name__
                func_name = EXTRACTOR_CLASS_FUNC[cls_name]
                file_path = request_dir / f"{func_name}_request.json"

                request_payload = {
                    "url": first[0] if isinstance(first, (tuple, list)) else getattr(first, "url", None),
                    "params": first[1] if isinstance(first, (tuple, list)) else getattr(first, "params", None),
                    "method": getattr(first, "method", "GET"),
                }

                with open(file_path, "w", encoding="utf-8") as f:
                    json.dump(request_payload, f, ensure_ascii=False, indent=2)

            except Exception as e:
                logger = getattr(self, "logger", getattr(self, "client", None).logger)
                logger.error(f"Failed to save API request: {e}")

            return requests

        return wrapper
    return decorator