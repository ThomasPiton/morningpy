import pandas as pd
from typing import Optional, Literal, Union, List, Dict, Any
from functools import lru_cache
from importlib import resources

class TickerExtractor:
    """
    Extracts and converts financial security tickers from a Parquet dataset.

    This class allows filtering securities by various criteria and converting between
    ticker symbols, ISINs, performance IDs, and security IDs.

    Attributes
    ----------
    tickers : pd.DataFrame
        DataFrame containing all ticker data loaded from parquet file.
        Data is cached at the class level to avoid redundant file reads.
    
    Notes
    -----
    The parquet file is loaded once and cached for the lifetime of the application.
    Use `clear_cache()` to reload if the underlying file changes.
    """

    _cached_tickers: Optional[pd.DataFrame] = None  # Class-level cache

    def __init__(self):
        """
        Initialize a TickerExtractor.

        Notes
        -----
        Uses a class-level cache to load the ticker data only once across all instances.
        Subsequent instantiations will reuse the cached DataFrame.
        """
        if TickerExtractor._cached_tickers is None:
            TickerExtractor._cached_tickers = self._load_tickers()
        self.tickers = TickerExtractor._cached_tickers

    @staticmethod
    @lru_cache(maxsize=1)
    def _load_tickers() -> pd.DataFrame:
        """
        Load ticker data from a packaged parquet file.

        This method locates the `tickers.parquet` resource bundled within the
        `morningpy` package and loads it into a pandas DataFrame. The loaded
        data is cached to prevent repeated disk access.

        Parameters
        ----------
        None

        Returns
        -------
        pd.DataFrame
            DataFrame containing all ticker records.

        Raises
        ------
        FileNotFoundError
            If the parquet file cannot be found within the installed package.
        """
        try:
            parquet = resources.files("morningpy").joinpath("data/tickers.parquet")
            with resources.as_file(parquet) as parquet_path:
                return pd.read_parquet(parquet_path)
        except FileNotFoundError as e:
            raise FileNotFoundError(
                "Could not find 'data/tickers.parquet' inside the morningpy package. "
                "Ensure it is included via package-data."
            ) from e

    @classmethod
    def clear_cache(cls):
        """
        Clear the cached ticker data.

        Call this method when the underlying parquet file has been updated
        and needs to be reloaded.

        Examples
        --------
        >>> # Update the parquet file externally
        >>> # Then clear cache to reload
        >>> TickerExtractor.clear_cache()
        >>> extractor = TickerExtractor()  # Will reload from file
        """
        cls._cached_tickers = None
        cls._load_tickers.cache_clear()

    def search_tickers(
        self,
        filters: Optional[Dict[str, Any]] = None,
        exact_match: bool = False
    ) -> pd.DataFrame:
        """
        Retrieve tickers filtered by specified criteria.

        Parameters
        ----------
        filters : dict, optional
            Dictionary of column names and their filter values.
            Values can be single items or lists for multiple matches.
        exact_match : bool, default False
            If True, performs exact string matching for text fields.
            If False, performs case-insensitive partial matching.

        Returns
        -------
        pd.DataFrame
            DataFrame containing tickers matching the specified filters,
            or all tickers if filters is None or empty.

        Examples
        --------
        >>> extractor = TickerExtractor()
        >>> # Filter by single value
        >>> extractor.search_tickers({"security_type": "fund"})
        >>> # Filter by multiple values
        >>> extractor.search_tickers({"country": ["US", "GB"], "is_active": True})
        """
        if not filters:
            return self.tickers.copy()
        
        filters = {
            k: v for k, v in filters.items() 
            if k != 'exact_match' and v is not None and k in self.tickers.columns
        }

        if not filters:
            return self.tickers.copy()

        mask = pd.Series(True, index=self.tickers.index)

        for column, value in filters.items():
            mask &= self._apply_filter(column, value, exact_match)

        return self.tickers[mask].reset_index(drop=True)

    def _apply_filter(
        self, 
        column: str, 
        value: Union[Any, List[Any]], 
        exact_match: bool
    ) -> pd.Series:
        """
        Apply a single filter condition and return a boolean mask.

        Parameters
        ----------
        column : str
            Column name to filter on.
        value : Any or list
            Value(s) to filter by.
        exact_match : bool
            Whether to use exact matching for string comparisons.

        Returns
        -------
        pd.Series
            Boolean mask indicating which rows match the filter.
        """
        col_data = self.tickers[column]

        if isinstance(value, list):
            if not value: 
                return pd.Series(True, index=self.tickers.index)
            return col_data.isin(value)

        if isinstance(value, str) and col_data.dtype == 'object':
            if exact_match:
                return col_data == value
            else:
                return col_data.str.contains(value, case=False, na=False)

        return col_data == value

    def convert_to(
        self,
        ticker: Optional[str] = None,
        isin: Optional[str] = None,
        performance_id: Optional[str] = None,
        security_id: Optional[str] = None,
        convert_to: Literal["ticker", "isin", "performance_id", "security_id"] = None
    ) -> Optional[str]:
        """
        Convert between ticker, ISIN, performance_id, or security_id.

        Parameters
        ----------
        ticker : str, optional
            Ticker symbol to convert from.
        isin : str, optional
            ISIN code to convert from.
        performance_id : str, optional
            Performance ID to convert from.
        security_id : str, optional
            Security ID to convert from.
        convert_to : {"ticker", "isin", "performance_id", "security_id"}
            The target field to convert to.

        Returns
        -------
        str or None
            The corresponding value in the target column, or None if not found.

        Raises
        ------
        ValueError
            If no source identifier is provided or convert_to is not specified.

        Examples
        --------
        >>> extractor = TickerExtractor()
        >>> extractor.convert_to(ticker="AAPL", convert_to="isin")
        'US0378331005'
        >>> extractor.convert_to(isin="US0378331005", convert_to="security_id")
        '0P000000GY'
        """
        if convert_to is None:
            raise ValueError("convert_to parameter must be specified")

        if not any([ticker, isin, performance_id, security_id]):
            raise ValueError("At least one source identifier must be provided")

        df = self.tickers

        # Find matching row based on source identifier
        if ticker:
            row = df[df["ticker"] == ticker]
        elif isin:
            row = df[df["isin"] == isin]
        elif performance_id:
            row = df[df["performance_id"] == performance_id]
        elif security_id:
            row = df[df["security_id"] == security_id]
        else:
            return None

        if row.empty:
            return None

        return row.iloc[0][convert_to]

    def batch_convert(
        self,
        identifiers: List[str],
        from_field: Literal["ticker", "isin", "performance_id", "security_id"],
        to_field: Literal["ticker", "isin", "performance_id", "security_id"]
    ) -> pd.DataFrame:
        """
        Convert multiple identifiers in batch.

        Parameters
        ----------
        identifiers : list of str
            List of identifiers to convert.
        from_field : {"ticker", "isin", "performance_id", "security_id"}
            The source field type.
        to_field : {"ticker", "isin", "performance_id", "security_id"}
            The target field type.

        Returns
        -------
        pd.DataFrame
            DataFrame with columns for source identifier, target identifier,
            and security label.

        Examples
        --------
        >>> extractor = TickerExtractor()
        >>> df = extractor.batch_convert(
        ...     ["AAPL", "MSFT"], 
        ...     from_field="ticker", 
        ...     to_field="isin"
        ... )
        """
        df = self.tickers[self.tickers[from_field].isin(identifiers)].copy()
        return df[[from_field, to_field, "security_label"]].reset_index(drop=True)