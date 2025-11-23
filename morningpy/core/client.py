import aiohttp
import requests
import logging
import asyncio
from typing import Any, Dict, List

from morningpy.core.auth import AuthManager
from morningpy.core.decorator import retry,save_api_response

class BaseClient:
    """
    Base network client handling low-level synchronous and asynchronous HTTP
    communication with retry support.

    This class centralizes authentication headers, session handling, and
    standardized GET operations for both sync and async workflows. It uses
    ``AuthManager`` to manage authentication and provides a consistent API for
    making requests.

    Attributes
    ----------
    logger : logging.Logger
        Logger instance for client-level logs.
    auth_type : str
        Type of authentication required (passed to ``AuthManager``).
    url : str or None
        Base URL or endpoint associated with the client.
    auth_manager : AuthManager
        Authentication handler that builds request headers.
    session : requests.Session
        Persistent session for synchronous HTTP communication.
    headers : dict
        Precomputed authentication headers.

    Notes
    -----
    - ``get_async`` is decorated with ``retry`` to automatically retry failed requests.
    - ``fetch_all`` dispatches async requests concurrently via ``asyncio.gather``.

    """

    DEFAULT_TIMEOUT = 20
    MAX_RETRIES = 1
    BACKOFF_FACTOR = 2

    def __init__(self, auth_type, url: str = None):
        """
        Initialize the BaseClient.

        Parameters
        ----------
        auth_type : str
            Type of authentication mechanism registered with ``AuthManager``.
        url : str, optional
            Base URL for the endpoint, used to build authentication headers.

        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth_type = auth_type
        self.url = url
        self.auth_manager = AuthManager()
        self.session = requests.Session()
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        """
        Build authentication headers using the configured ``AuthManager``.

        Returns
        -------
        dict
            Authentication headers including tokens, user agent, etc.

        """
        return self.auth_manager.get_headers(self.auth_type, self.url)

    @retry(max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
    async def get_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: dict | None = None,
    ) -> Dict[str, Any]:
        """
        Send an asynchronous GET request with retry logic applied.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used to send the request.
        url : str
            Full request URL.
        params : dict, optional
            Query parameters for the GET request.

        Returns
        -------
        dict
            Parsed JSON response from the server.

        Raises
        ------
        aiohttp.ClientResponseError
            If the request fails and exceeds the maximum retry attempts.
        aiohttp.ClientError
            For lower-level network errors.
        asyncio.TimeoutError
            If the request exceeds ``DEFAULT_TIMEOUT``.

        Notes
        -----
        - Retry behavior is controlled via the ``@retry`` decorator.
        - Headers are automatically included.
        - ``raise_for_status`` triggers retries for HTTP 4xx/5xx errors.

        """
        async with session.get(
            url,
            headers=self.headers,
            timeout=self.DEFAULT_TIMEOUT,
            params=params,
        ) as response:
            response.raise_for_status()
            return await response.json()

    async def fetch_all(
        self,
        session: aiohttp.ClientSession,
        requests: List[tuple[str, dict]],
    ) -> List[Any]:
        """
        Fetch multiple GET requests concurrently.

        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used for all requests.
        requests : list of (str, dict)
            A list of tuples ``(url, params)`` specifying endpoints and query
            parameters.

        Returns
        -------
        list
            A list of responses or exceptions. Exceptions are returned as-is
            (not raised) to allow consumers to handle them individually.

        Notes
        -----
        - The method uses ``asyncio.gather`` with ``return_exceptions=True``.
        - Each request internally uses the retry logic of ``get_async``.

        Examples
        --------
        >>> async with aiohttp.ClientSession() as session:
        ...     tasks = [
        ...         ("https://api.morningstar.com/a", {"q": 1}),
        ...         ("https://api.morningstar.com/b", {"q": 2}),
        ...     ]
        ...     results = await client.fetch_all(session, tasks)

        """
        tasks = [self.get_async(session, url, params=params) for url, params in requests]
        return await asyncio.gather(*tasks, return_exceptions=True)
