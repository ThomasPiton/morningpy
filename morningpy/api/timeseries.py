import asyncio
from typing import Union, List, Literal

from morningpy.extractor.timeseries import *
from morningpy.core.interchange import DataFrameInterchange


def get_intraday_timeseries(
    ticker: str = None, 
    isin: str = None, 
    security_id: str = None, 
    performance_id: str = None,
    start_date: str = None,
    end_date: str = None,
    frequency: Literal["1min", "5min", "10min", "15min", "30min", "60min"] = None,
    pre_after: Literal[True, False] = False
) -> DataFrameInterchange:
    """
    Retrieve intraday time series data for a security.

    This function wraps the `IntradayTimeseriesExtractor` to provide 
    high-frequency market data between the specified start and end dates.
    Available intervals range from 1-minute to hourly. Extended-hours 
    trading sessions (pre-market and after-hours) can be included via 
    the `pre_after` flag.

    Parameters
    ----------
    ticker : str, optional
        Ticker symbol of the security.
    isin : str, optional
        ISIN code of the security.
    id_security : str, optional
        Internal Morningstar security identifier.
    performance_id : str, optional
        Morningstar performance identifier.
    start_date : str, optional
        Start date for the intraday data (ISO format, e.g. "2024-01-01").
    end_date : str, optional
        End date for the intraday data.
    frequency : {"1min", "5min", "10min", "15min", "30min", "60min"}, optional
        Intraday sampling frequency.
    pre_after : bool, default False
        Whether to include pre-market and after-market trading sessions.

    Returns
    -------
    DataFrameInterchange
        A standardized dataframe-like structure containing intraday OHLCV data.
    """
    extractor = IntradayTimeseriesExtractor(
        ticker=ticker,
        isin=isin,
        security_id=security_id,
        performance_id=performance_id,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        pre_after=pre_after
    )
    
    return asyncio.run(extractor.run())


def get_historical_timeseries(
    ticker: Union[str, List[str]] = None, 
    isin: Union[str, List[str]] = None, 
    security_id: Union[str, List[str]] = None, 
    performance_id: Union[str, List[str]] = None, 
    start_date: str = None,
    end_date: str = None,
    frequency: Literal["daily", "weekly", "monthly"] = None,
    pre_after: Literal[True, False] = False
) -> DataFrameInterchange:
    """
    Retrieve historical time series data for one or multiple securities.

    This function wraps the `HistoricalTimeseriesExtractor` to obtain 
    end-of-day (EOD) or aggregated historical data for the specified 
    frequency. Extended-hours data can be included when supported.

    Parameters
    ----------
    ticker : str or list of str, optional
        Ticker symbol(s) of the security.
    isin : str or list of str, optional
        ISIN code(s) of the security.
    security_id : str or list of str, optional
        Internal Morningstar security identifier(s).
    performance_id : str or list of str, optional
        Morningstar performance identifier(s).
    start_date : str, optional
        Start date for the historical series (ISO format).
    end_date : str, optional
        End date for the historical series. If None, the latest data is returned.
    frequency : {"daily", "weekly", "monthly"}, optional
        Sampling frequency for the time series.
    pre_after : bool, default False
        Whether to include pre-market and after-market sessions when available.

    Returns
    -------
    DataFrameInterchange
        A standardized dataframe-like structure containing historical OHLCV data.
    """
    extractor = HistoricalTimeseriesExtractor(
        ticker=ticker,
        isin=isin,
        security_id=security_id,
        performance_id=performance_id,
        start_date=start_date,
        end_date=end_date,
        frequency=frequency,
        pre_after=pre_after
    )
    
    return asyncio.run(extractor.run())
