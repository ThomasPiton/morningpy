import pandas as pd
from typing import Any, Dict, Optional, List,Union
import warnings

from morningpy.core.security_convert import IdSecurityConverter
from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.timeseries import *
from morningpy.schema.timeseries import *

    
class IntradayTimeseriesExtractor(BaseExtractor):
    """
    Extracts intraday timeseries data for a single security from Morningstar.

    This extractor handles:
        - Validating inputs (dates, frequency, pre/post market flag)
        - Building API requests (splitting date ranges into 18-business-day chunks)
        - Processing API responses into a standardized pandas DataFrame

    Attributes:
        ticker (str | None): Ticker symbol of the security.
        isin (str | None): ISIN code of the security.
        security_id (str | None): Morningstar internal security ID.
        performance_id (str | None): Morningstar performance ID.
        start_date (str): Start date for extraction (YYYY-MM-DD).
        end_date (str): End date for extraction (YYYY-MM-DD).
        frequency (str): Frequency of intraday data (e.g., "5min").
        pre_after (bool): Include pre/post-market data if True.
        security_id (str): Converted single Morningstar security ID after validation.
    """

    config = IntradayTimeseriesConfig
    schema = IntradayTimeseriesSchema

    def __init__(self,
                 ticker: str = None,
                 isin: str = None,
                 security_id: str = None,
                 performance_id: str = None,
                 start_date: str = "1900-01-01",
                 end_date: str = "1900-01-01",
                 frequency: str = "5min",
                 pre_after: bool = False):

        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.pre_after = pre_after
        self.url = self.config.API_URL
        self.params = self.config.PARAMS.copy()
        self.mapping_frequency = self.config.MAPPING_FREQUENCY
        self.valid_frequency = self.config.VALID_FREQUENCY  # corrected typo
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        
        self.security_id = IdSecurityConverter(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).convert()

    def _check_inputs(self) -> None:
        """Validate user inputs and apply transformations."""

        if self.frequency not in self.valid_frequency:
            raise ValueError(
                f"Invalid frequency '{self.frequency}', must be one of {list(self.valid_frequency.keys())}"
            )

        if not isinstance(self.pre_after, bool):
            raise TypeError("Parameter 'pre_after' must be a boolean (True or False).")
        self.pre_after = "true" if self.pre_after else "false"

        if not isinstance(self.security_id, (list, tuple)):
            raise TypeError("security_id must be a list or tuple.")
        if len(self.security_id) != 1:
            raise ValueError("Exactly one security ID must be provided for intraday extraction.")
        self.security_id = self.security_id[0]
        
        try:
            self.start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            self.end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Dates must be in format YYYY-MM-DD (e.g., '2020-01-01').")

        if self.start_dt > self.end_dt:
            raise ValueError("start_date cannot be after end_date.")
        
        today = datetime.now()
        if (today - self.start_dt).days > 5 * 365:
            raise ValueError("Extraction period cannot exceed 5 years from today.")

    def _build_request(self) -> None:
        """
        Build one or several API requests per security ID.
        Morningstar API limits intraday data extraction to 18 business days per call.
        If the date range exceeds 18 business days, multiple requests (chunks) are created.
        """
        max_business_days = 18

        business_days = pd.bdate_range(start=self.start_dt, end=self.end_dt)
        total_days = len(business_days)

        if total_days == 0:
            raise ValueError("No business days found in the given date range.")

        chunks = [
            business_days[i:i + max_business_days]
            for i in range(0, total_days, max_business_days)]

        self.params = [
            {
                **self.config.PARAMS,  # use fresh copy of base params
                "query": f"{self.security_id}:open,high,low,close,volume,previousClose",
                "frequency": self.mapping_frequency[self.frequency],
                "preAfter": self.pre_after,
                "startDate": chunk[0].strftime("%Y-%m-%d"),
                "endDate": chunk[-1].strftime("%Y-%m-%d"),
            } for chunk in chunks
        ]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar intraday timeseries response."""
        if not isinstance(response, list) or not response:
            return pd.DataFrame()

        rows = []
        for security_block in response:
            security = security_block.get("queryKey")

            for daily_series in security_block.get("series", []):
                prev_close = daily_series.get("previousClose")
                for child in daily_series.get("children", []):
                    rows.append({
                        "security_id": security,
                        "date": child.get("date"),
                        "open": child.get("open"),
                        "high": child.get("high"),
                        "low": child.get("low"),
                        "close": child.get("close"),
                        "volume": child.get("volume"),
                        "previousClose": prev_close,
                    })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.sort_values(by=["security_id", "date"], inplace=True, ignore_index=True)
        return df
        
class HistoricalTimeseriesExtractor(BaseExtractor):
    """
    Extracts historical timeseries data for multiple securities from Morningstar.

    This extractor handles:
        - Validating inputs (dates, frequency, pre/post market flag, security limit)
        - Building API requests per security
        - Processing API responses into a standardized pandas DataFrame

    Attributes:
        ticker (str | List[str] | None): Ticker symbols of securities.
        isin (str | List[str] | None): ISIN codes of securities.
        security_id (str | List[str] | None): Morningstar internal security IDs.
        performance_id (str | List[str] | None): Morningstar performance IDs.
        start_date (str): Start date for extraction (YYYY-MM-DD).
        end_date (str): End date for extraction (YYYY-MM-DD).
        frequency (str): Frequency of historical data (e.g., "daily", "weekly").
        pre_after (bool): Include pre/post-market data if True.
        security_id (List[str]): Converted and validated list of Morningstar security IDs.
    
    Notes:
        - Maximum of 100 securities per request is enforced.
        - Dates are validated and must follow YYYY-MM-DD format.
    """

    config = HistoricalTimeseriesConfig
    schema = HistoricalTimeseriesSchema

    def __init__(self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id:Union[str, List[str]] = None,
        start_date: str = "1900-01-01",
        end_date: str = "2025-11-16",
        frequency: str = "daily",
        pre_after: bool = False):

        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        super().__init__(client)

        self.start_date = start_date
        self.end_date = end_date
        self.frequency = frequency
        self.pre_after = pre_after
        self.url = self.config.API_URL
        self.params = self.config.PARAMS.copy()
        self.mapping_frequency = self.config.MAPPING_FREQUENCY
        self.valid_frequency = self.config.VALID_FREQUENCY
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

        self.security_id = IdSecurityConverter(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).convert()

    def _check_inputs(self) -> None:
        """Validate user inputs and apply transformations."""
        
        if self.frequency not in self.valid_frequency:
            raise ValueError(
                f"Invalid frequency '{self.frequency}', must be one of {list(self.valid_frequency.keys())}"
            )

        if not isinstance(self.pre_after, bool):
            raise TypeError("Parameter 'pre_after' must be a boolean (True or False).")
        self.pre_after = "true" if self.pre_after else "false"

        if not isinstance(self.security_id, (list, tuple)):
            raise TypeError("Parameter 'security_id' must be a list or tuple.")

        if len(self.security_id) > 100:
            raise ValueError("A maximum of 100 securities can be requested for historical extraction.")
        
        try:
            self.start_dt = datetime.strptime(self.start_date, "%Y-%m-%d")
            self.end_dt = datetime.strptime(self.end_date, "%Y-%m-%d")
        except ValueError:
            raise ValueError("Dates must be in format YYYY-MM-DD (e.g., '2020-01-01').")

        if self.start_dt > self.end_dt:
            raise ValueError("start_date cannot be after end_date.")

    def _build_request(self) -> None:
        """Split request by date and security 
        """
        
        self.params = [
            {
                **self.params, 
                "query": f"{id}:open,high,low,close,volume,previousClose,marketTotalReturn", 
                "frequency": self.mapping_frequency[self.frequency],
                "preAfter":self.pre_after,
                "startDate":self.start_date,
                "endDate":self.end_date,
            } for id in self.security_id]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar Market Movers response based on selected mover_type."""
        if not isinstance(response, list) or not response:
            return pd.DataFrame()

        rows = []

        for security_block in response:
            security_id = security_block.get("queryKey")
            series = security_block.get("series", [])

            if not isinstance(series, list) or not series:
                continue

            for record in series:
                rows.append({
                    "security_id": security_id,
                    "date": record.get("date"),
                    "open": record.get("open"),
                    "high": record.get("high"),
                    "low": record.get("low"),
                    "close": record.get("close"),
                    "volume": record.get("volume"),
                    "previous_close": record.get("previousClose"),
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        df["date"] = pd.to_datetime(df["date"], errors="coerce")
        df.sort_values(by=["security_id","date"], inplace=True, ignore_index=True)
        return df
        
    
    