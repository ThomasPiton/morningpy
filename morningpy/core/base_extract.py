from abc import ABC, abstractmethod
import aiohttp
from typing import Any, List, Tuple, Dict, Optional, Union, Type
import pandas as pd

from morningpy.core.decorator import save_dataframe_mock,save_api_response,save_api_request
from morningpy.core.interchange import DataFrameInterchange


class BaseExtractor(ABC):
    """
    Abstract base class for async extractors.

    Responsibilities:
    - Validate input parameters
    - Build a request definition (url + params)
    - Make async calls (single or batch)
    - Process API responses individually
    - Normalize and validate resulting DataFrames using an optional schema
    """

    schema: Optional[Type] = None

    def __init__(self, client):
        self.client = client
        self.url: Union[str, List[str]] = ""
        self.params: Union[Dict[str, Any], List[Dict[str, Any]], None] = None

    @abstractmethod
    def _check_inputs(self) -> None:
        """Validate input parameters before request build."""
        raise NotImplementedError

    @abstractmethod
    def _build_request(self) -> None:
        """Set self.url and self.params."""
        raise NotImplementedError

    @abstractmethod
    def _process_response(self, response: Any) -> pd.DataFrame:
        """Convert API json/dict response → DataFrame."""
        raise NotImplementedError
    
    @save_dataframe_mock(activate=False)
    async def _call_api(self) -> pd.DataFrame:
        """
        Handle single or multiple async API calls.

        Automatically detects 3 cases:
        - self.url is a list  → multiple endpoints, same params
        - self.params is a list → same endpoint, different params
        - simple call → one request
        """

        timeout = aiohttp.ClientTimeout(total=self.client.DEFAULT_TIMEOUT)

        async with aiohttp.ClientSession(
            timeout=timeout,
            headers=self.client.headers
        ) as session:

            requests = self._prepare_requests()

            responses = await self._fetch_responses(session, requests)

            dfs = []
            for res in responses:
                if isinstance(res, Exception):
                    self.client.logger.error(f"API call failed: {res}")
                    continue
            
                df = self._process_response(res)
                if not isinstance(df, pd.DataFrame):
                    self.client.logger.error(
                        f"_process_response must return a DataFrame, got {type(df)}"
                    )
                    continue

                dfs.append(df)
                
            return pd.concat(dfs, ignore_index=True, sort=False) if dfs else pd.DataFrame()

    @save_api_response(activate=False)
    async def _fetch_responses(self, session, requests):
        """Fetch API responses using the client."""
        return await self.client.fetch_all(session, requests)
    
    @save_api_request(activate=False)
    def _prepare_requests(self) -> List[Tuple[str, Optional[Dict[str, Any]]]]:
        """Normalize request inputs into a uniform list of (url, params) pairs."""

        # Case 1: Multiple URLs
        if isinstance(self.url, list):
            return [(u, self.params) for u in self.url]

        # Case 2: Multiple params
        if isinstance(self.params, list):
            return [(self.url, p) for p in self.params]

        # Case 3: Single request
        return [(self.url, self.params)]

    def _validate_and_convert_types(self, df: pd.DataFrame) -> pd.DataFrame:
        """Validate and convert DataFrame types based on provided schema."""

        if self.schema is None:
            return df

        schema_instance = self.schema()
        dtype_map = schema_instance.to_dtype_dict()

        for col, dtype in dtype_map.items():
            if col not in df.columns:
                continue

            try:
                if dtype == "string":
                    df[col] = df[col].astype("string")

                elif dtype == "Int64":
                    df[col] = pd.to_numeric(df[col], errors="coerce").astype("Int64")

                elif dtype in ("float32", "float64"):
                    df[col] = pd.to_numeric(df[col], errors="coerce")

                elif dtype == "boolean":
                    df[col] = df[col].astype("boolean")

                else:
                    df[col] = df[col].astype(dtype)

            except Exception as e:
                self.client.logger.warning(
                    f"Failed to convert column '{col}' to {dtype}: {e}"
                )

        return df

    async def run(self) -> DataFrameInterchange:
        """Unified async extraction pipeline."""
        self._check_inputs()
        self._build_request()

        df = await self._call_api()
        df = self._validate_and_convert_types(df)

        return DataFrameInterchange(df)
