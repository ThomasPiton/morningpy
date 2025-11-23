import pytest
import inspect
from unittest.mock import patch
import pandas as pd
from morningpy import __all__ as api_functions_all
from morningpy import *  # import all functions
from morningpy.extractor import *
from unittest.mock import patch
import pytest
import pandas as pd

# Import all extractor classes explicitly

from morningpy.extractor.market import (MarketExtractor,MarketMoversExtractor,MarketIndexesExtractor,MarketFairValueExtractor,MarketCurrenciesExtractor,MarketCommoditiesExtractor,MarketCalendarUsInfoExtractor)
from morningpy.extractor.news import (HeadlineNewsExtractor)
from morningpy.extractor.timeseries import (IntradayTimeseriesExtractor,HistoricalTimeseriesExtractor)
from morningpy.extractor.security import (HoldingExtractor,HoldingInfoExtractor,FinancialStatementExtractor)
from morningpy.extractor.ticker import (TickerExtractor)

# Mapping extractor classes to wrapper functions
EXTRACTOR_CLASS_FUNC = {
    MarketCalendarUsInfoExtractor: "get_market_us_calendar_info",
    MarketCommoditiesExtractor: "get_market_commodities",
    MarketCurrenciesExtractor: "get_market_currencies",
    MarketMoversExtractor: "get_market_movers",
    MarketIndexesExtractor: "get_market_indexes",
    MarketFairValueExtractor: "get_market_fair_value",
    MarketExtractor: "get_market_info",
    HeadlineNewsExtractor: "get_headline_news",
    FinancialStatementExtractor: "get_financial_statement",
    HoldingExtractor: "get_holding",
    HoldingInfoExtractor: "get_holding_info",
    HistoricalTimeseriesExtractor: "get_historical_timeseries",
    IntradayTimeseriesExtractor: "get_intraday_timeseries",
    # Add more as needed
}


@pytest.mark.parametrize(
    "scenario_files,func_name",
    [(f, f) for f in api_functions_all],
    indirect=["scenario_files"]
)
def test_api_function(func_name, scenario_files, mocker, monkeypatch):
    request_data = scenario_files["request"] or {}
    expected_response = scenario_files["response"] or []
    expected_df = scenario_files["mock"]

    func = globals()[func_name]

    # Only keep keys that match the function signature
    sig = inspect.signature(func)
    func_params = {k: v for k, v in request_data.get("params", {}).items() if k in sig.parameters}

    # Find extractor class to mock
    extractor_class = next(
        (cls for cls, f in EXTRACTOR_CLASS_FUNC.items() if f == func_name), None
    )

    if extractor_class:
        # Build the target path for monkeypatching
        patch_target = f"{extractor_class.__module__}.{extractor_class.__name__}.run"

        # Use mocker to patch the run method
        mock_run = mocker.patch(patch_target, return_value=pd.DataFrame(expected_response))

        # Call the function
        result_df = func(**func_params) if func_params else func()

        # Assert the extractor's run method was called
        mock_run.assert_called_once()
    else:
        result_df = func(**func_params) if func_params else func()

    if expected_df is not None:
        pd.testing.assert_frame_equal(result_df, expected_df)