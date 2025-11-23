from .api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value,
    get_market_info,
)

from .api.news import (
    get_headline_news,
)

from .api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info,
)

from .api.ticker import (
    get_all_etfs,
    get_all_funds,
    get_all_securities,
    get_all_stocks,
    convert
)

from .api.timeseries import (
    get_historical_timeseries,
    get_intraday_timeseries,
)

try:
    from importlib.metadata import version
    __version__ = version("morningpy")
except Exception:
    __version__ = "0.0.0"  # fallback for dev/test environment


__all__ = [
    "get_market_us_calendar_info",
    "get_market_commodities",
    "get_market_currencies",
    "get_market_movers",
    "get_market_indexes",
    "get_market_fair_value",
    "get_market_info",
    "get_headline_news",
    "get_financial_statement",
    "get_holding",
    "get_holding_info",
    "get_all_etfs",
    "get_all_funds",
    "get_all_securities",
    "get_all_stocks",
    "convert",
    "get_historical_timeseries",
    "get_intraday_timeseries",
]
