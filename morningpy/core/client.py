import aiohttp
import requests
import logging
import asyncio
from typing import Any, Dict, List, Tuple, Optional

from morningpy.core.auth import AuthManager
from morningpy.core.decorator import retry, save_api_response


class BaseClient:
    """
    Base network client for synchronous and asynchronous HTTP communication.
    
    Centralizes authentication headers, session handling, and standardized GET
    operations for both sync and async workflows. Uses AuthManager to manage
    authentication and provides retry logic for failed requests.
    
    Attributes
    ----------
    DEFAULT_TIMEOUT : int
        Default request timeout in seconds
    MAX_RETRIES : int
        Maximum number of retry attempts for failed requests
    BACKOFF_FACTOR : int
        Exponential backoff multiplier between retries
    logger : logging.Logger
        Logger instance for client-level logs
    auth_type : str
        Type of authentication required (passed to AuthManager)
    url : str or None
        Base URL or endpoint associated with the client
    auth_manager : AuthManager
        Authentication handler that builds request headers
    session : requests.Session
        Persistent session for synchronous HTTP communication
    headers : dict
        Precomputed authentication headers
    
    Notes
    -----
    - get_async is decorated with retry to automatically retry failed requests
    - fetch_all dispatches async requests concurrently via asyncio.gather
    """

    DEFAULT_TIMEOUT = 20
    MAX_RETRIES = 1
    BACKOFF_FACTOR = 2

    def __init__(self, auth_type: str, url: Optional[str] = None):
        """
        Initialize the BaseClient.
        
        Parameters
        ----------
        auth_type : str
            Type of authentication mechanism registered with AuthManager
        url : str, optional
            Base URL for the endpoint, used to build authentication headers
        """
        self.logger = logging.getLogger(self.__class__.__name__)
        self.auth_type = auth_type
        self.url = url
        self.auth_manager = AuthManager()
        self.session = requests.Session()
        self.headers = self._get_headers()

    def _get_headers(self) -> Dict[str, str]:
        """
        Build authentication headers using the configured AuthManager.
        
        Returns
        -------
        Dict[str, str]
            Authentication headers including tokens, user agent, etc.
        """
        return self.auth_manager.get_headers(self.auth_type, self.url)

    @retry(max_retries=MAX_RETRIES, backoff_factor=BACKOFF_FACTOR)
    async def get_async(
        self,
        session: aiohttp.ClientSession,
        url: str,
        params: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Send an asynchronous GET request with retry logic.
        
        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used to send the request
        url : str
            Full request URL
        params : dict, optional
            Query parameters for the GET request
        metadata : dict, optional
            Additional metadata to attach to the response (e.g., security_id).
            Will be added to response under 'metadata' key.
        
        Returns
        -------
        Dict[str, Any]
            Parsed JSON response from the server, with metadata injected
            if provided
        
        Raises
        ------
        aiohttp.ClientResponseError
            If the request fails and exceeds the maximum retry attempts
        aiohttp.ClientError
            For lower-level network errors
        asyncio.TimeoutError
            If the request exceeds DEFAULT_TIMEOUT
        
        Notes
        -----
        - Retry behavior is controlled via the @retry decorator
        - Headers are automatically included from self.headers
        - raise_for_status triggers retries for HTTP 4xx/5xx errors
        """
        async with session.get(
            url,
            headers=self.headers,
            timeout=aiohttp.ClientTimeout(total=self.DEFAULT_TIMEOUT),
            params=params,
        ) as response:
            response.raise_for_status()
            result = await response.json()
            
            if metadata:
                if isinstance(result, dict):
                    result.setdefault("metadata", metadata)
                elif isinstance(result, list) and result and isinstance(result[0], dict):
                    result[0].setdefault("metadata", metadata)
                        
            return result

    async def fetch_all(
        self,
        session: aiohttp.ClientSession,
        requests: List[Tuple[str, Optional[Dict[str, Any]], Optional[Dict[str, Any]]]],
    ) -> List[Any]:
        """
        Fetch multiple GET requests concurrently.
        
        Parameters
        ----------
        session : aiohttp.ClientSession
            Active aiohttp session used for all requests
        requests : List[Tuple[str, dict, dict]]
            List of tuples (url, params, metadata) specifying endpoints,
            query parameters, and metadata to attach to each response
        
        Returns
        -------
        List[Any]
            List of responses or exceptions. Exceptions are returned as-is
            (not raised) to allow consumers to handle them individually
        
        Notes
        -----
        - Uses asyncio.gather with return_exceptions=True
        - Each request internally uses the retry logic of get_async
        - Failed requests return Exception objects instead of raising
        
        Examples
        --------
        >>> async with aiohttp.ClientSession() as session:
        ...     tasks = [
        ...         ("https://api.example.com/a", {"q": 1}, {"id": "sec1"}),
        ...         ("https://api.example.com/b", {"q": 2}, {"id": "sec2"}),
        ...     ]
        ...     results = await client.fetch_all(session, tasks)
        """
        tasks = [
            self.get_async(
                session,
                req["url"],
                params=req.get("params"),
                metadata=req.get("metadata"),
            )
            for req in requests
        ]
        return await asyncio.gather(*tasks, return_exceptions=True)