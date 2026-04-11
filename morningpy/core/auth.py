import re
import requests
from selenium import webdriver
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.support.ui import WebDriverWait
from enum import Enum
from typing import Dict, Optional, Any

from .config import CoreConfig, _UA
from .cache import Cache


class AuthType(Enum):
    """
    Authentication types supported by the Morningstar API.
    
    Attributes
    ----------
    API_KEY : str
        Standard Apigee API key authentication
    BEARER_TOKEN : str
        MAAS bearer token authentication
    WAF_TOKEN : str
        AWS WAF browser token (requires Selenium)
    NONE : str
        No authentication required
    """
    API_KEY = "apikey"
    BEARER_TOKEN = "bearer"
    WAF_TOKEN = "waf"
    NONE = "none"


class AuthManager:
    """
    Manages authentication for Morningstar API interactions.
    
    Handles retrieval, caching, and refresh of various authentication tokens
    including API keys, MAAS bearer tokens, and AWS WAF tokens. Implements
    a fallback strategy using persistent cache when live retrieval fails.
    
    Attributes
    ----------
    cache : Cache
        Persistent cache for storing tokens between sessions
    
    Notes
    -----
    - Tokens are cached both in-memory and persistently
    - Automatic fallback to cached values on retrieval failure
    - Supports forced refresh for all token types
    """

    def __init__(self):
        """
        Initialize the AuthManager with default configuration.
        
        Sets up headers, URLs, and initializes token storage and cache.
        """
        self._headers = CoreConfig.DEFAULT_HEADERS
        self._urls = CoreConfig.URLS
        self._maas_token: Optional[str] = None
        self._api_key: Optional[str] = None
        self._token_real_time: Optional[str] = None
        self._waf_token: Optional[str] = None
        self.cache = Cache()

    def get_maas_token(self, force_refresh: bool = False) -> str:
        """
        Retrieve the MAAS bearer token with intelligent caching.
        
        Strategy:
        1. Return in-memory token unless force_refresh=True
        2. Attempt live HTTP retrieval from endpoint
        3. Fallback to persistent cache if server returns empty
        4. Raise ValueError if no valid token available
        
        Parameters
        ----------
        force_refresh : bool, default=False
            If True, bypasses in-memory cache and forces live retrieval
        
        Returns
        -------
        str
            Valid MAAS bearer token
        
        Raises
        ------
        ValueError
            If token cannot be retrieved and no cached value exists
        
        Notes
        -----
        Successfully retrieved tokens are cached both in-memory and persistently
        """
        cached = self.cache.get("maas_token")

        if self._maas_token and not force_refresh:
            return self._maas_token

        url = self._urls["maas_token"]
        try:
            response = self._fetch_url(url)
            token = response.text.strip()
        except Exception:
            token = ""

        if not token:
            if cached:
                print("⚠️ Empty MAAS token response, using cached token.")
                return cached
            raise ValueError("Empty MAAS token and no cached token available.")

        self._maas_token = token
        self.cache.set("maas_token", token)
        return token

    def get_api_key(self, force_refresh: bool = False) -> str:
        """
        Retrieve the Apigee API key by parsing JavaScript content.
        
        Extracts the API key from JavaScript using pattern:
            keyApigee: "XXXX"
        
        Parameters
        ----------
        force_refresh : bool, default=False
            If True, bypasses in-memory cache and forces live retrieval
        
        Returns
        -------
        str
            Valid Morningstar Apigee API key
        
        Raises
        ------
        ValueError
            If API key cannot be extracted and no cached value exists
        
        Notes
        -----
        - Parses JavaScript content using regex pattern matching
        - Falls back to cached value on extraction failure
        - Successfully retrieved keys are cached persistently
        """
        cached = self.cache.get("apikey")
        
        if self._api_key and not force_refresh:
            return self._api_key

        url = self._urls["key_api"]

        try:
            resp = self._fetch_url(url)
            content = resp.text
            pattern = r'keyApigee\s*[:=]\s*["\']([^"\']+)["\']'
            match = re.search(pattern, content)
            api_key = match.group(1) if match else ""

        except Exception as e:
            print(f"⚠️ Error while retrieving API key: {e}")
            api_key = ""

        if not api_key:
            if cached:
                print("⚠️ Empty API key, using cached value.")
                return cached
            raise ValueError("API key not found in response or cache.")

        self._api_key = api_key
        self.cache.set("apikey", api_key)
        return api_key

    def get_token_real_time(self, force_refresh: bool = False) -> str:
        """
        Retrieve the real-time data token from JavaScript payload.
        
        Extracts token from JavaScript using pattern:
            tokenRealtime: "XXXX"
        
        Parameters
        ----------
        force_refresh : bool, default=False
            If True, bypasses in-memory cache and forces live retrieval
        
        Returns
        -------
        str
            Valid real-time data token
        
        Raises
        ------
        ValueError
            If token cannot be extracted and no cached value exists
        
        Notes
        -----
        Uses same endpoint as API key but extracts different token field
        """
        cached = self.cache.get("token_real_time")

        if self._token_real_time and not force_refresh:
            return self._token_real_time

        url = self._urls["key_api"]
        try:
            response = self._fetch_url(url)
            match = re.search(r'tokenRealtime\s*[:=]\s*"([^"]+)"', response.text)
            token = match.group(1) if match else ""
        except Exception:
            token = ""

        if not token:
            if cached:
                print("⚠️ Empty real-time token, using cached value.")
                return cached
            raise ValueError("Real-time token not found in response or cache.")

        self._token_real_time = token
        self.cache.set("token_real_time", token)
        return token

    def get_waf_token(
        self,
        url: str = "https://www.morningstar.com/markets/calendar",
        force_refresh: bool = False
    ) -> str:
        """
        Retrieve AWS WAF token using headless Chrome browser automation.
        
        Strategy:
        1. Launch headless Chrome with anti-detection measures
        2. Load target page to trigger WAF token generation
        3. Extract cookies and identify WAF token cookie
        4. Fallback to persistent cache on failure
        
        Parameters
        ----------
        url : str, default="https://www.morningstar.com/markets/calendar"
            Target webpage URL to trigger WAF token generation
        force_refresh : bool, default=False
            If True, bypasses in-memory cache and launches browser
        
        Returns
        -------
        str
            Valid AWS WAF token extracted from browser cookies
        
        Raises
        ------
        ValueError
            If token cannot be extracted and no cached value exists
        
        Notes
        -----
        - Requires ChromeDriver to be installed and accessible
        - Uses headless mode to avoid opening visible browser window
        - Searches for cookies containing 'waf' or 'token' in name
        - Browser cleanup handled in finally block
        """
        cached = self.cache.get("waf_token")

        if self._waf_token and not force_refresh:
            return self._waf_token

        chrome_options = Options()
        chrome_options.add_argument("--headless")
        chrome_options.add_argument("--no-sandbox")
        chrome_options.add_argument("--disable-dev-shm-usage")
        chrome_options.add_argument("--disable-blink-features=AutomationControlled")
        chrome_options.add_experimental_option("excludeSwitches", ["enable-automation"])
        chrome_options.add_experimental_option("useAutomationExtension", False)
        chrome_options.add_argument(f"user-agent={_UA}")

        waf_token = ""
        driver = None

        try:
            driver = webdriver.Chrome(options=chrome_options)
            driver.get(url)

            WebDriverWait(driver, 10).until(
                lambda d: d.execute_script("return document.readyState") == "complete"
            )

            cookies = driver.get_cookies()
            cookies_dict = {c["name"]: c["value"] for c in cookies}

            for name, value in cookies_dict.items():
                if "waf" in name.lower() or "token" in name.lower():
                    waf_token = value
                    break

        except Exception as e:
            print(f"⚠️ Selenium WAF token fetch failed: {e}")
            waf_token = ""
        finally:
            if driver:
                driver.quit()

        if not waf_token:
            if cached:
                print("⚠️ Empty WAF token, using cached value.")
                return cached
            raise ValueError("WAF token not found in response or cache.")

        self._waf_token = waf_token
        self.cache.set("waf_token", waf_token)
        return waf_token

    def _fetch_url(self, url: str) -> requests.Response:
        """
        Perform authenticated GET request with error handling.
        
        Parameters
        ----------
        url : str
            Target URL to fetch
        
        Returns
        -------
        requests.Response
            Validated HTTP response object
        
        Raises
        ------
        requests.HTTPError
            If response status code is not 2xx
        requests.Timeout
            If request exceeds 20 second timeout
        
        Notes
        -----
        Uses default headers from CONFIG and 20 second timeout
        """
        response = requests.get(url, headers=self._headers, timeout=20)
        response.raise_for_status()
        return response

    def get_headers(
        self, 
        auth_type: AuthType, 
        url: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Build HTTP headers with appropriate authentication for API requests.
        
        Parameters
        ----------
        auth_type : AuthType
            Type of authentication to apply (API_KEY, BEARER_TOKEN, WAF_TOKEN, or NONE)
        url : str, optional
            URL required for WAF token generation (only used when auth_type is WAF_TOKEN)
        
        Returns
        -------
        Dict[str, Any]
            Complete headers dictionary with authentication fields injected
        
        Notes
        -----
        - Starts with default headers from CONFIG
        - Injects auth-specific fields based on auth_type:
            - API_KEY: Adds 'Apikey' header
            - BEARER_TOKEN: Adds 'authorization' header with Bearer prefix
            - WAF_TOKEN: Adds 'x-aws-waf-token' header
            - NONE: Returns default headers unchanged
        
        Examples
        --------
        >>> auth_mgr = AuthManager()
        >>> headers = auth_mgr.get_headers(AuthType.API_KEY)
        >>> headers = auth_mgr.get_headers(AuthType.WAF_TOKEN, url="https://example.com")
        """
        headers = self._headers.copy()

        if auth_type == AuthType.API_KEY:
            headers["Apikey"] = self.get_api_key()

        elif auth_type == AuthType.BEARER_TOKEN:
            headers["authorization"] = f"Bearer {self.get_maas_token()}"

        elif auth_type == AuthType.WAF_TOKEN:
            headers["x-aws-waf-token"] = self.get_waf_token(url)

        return headers