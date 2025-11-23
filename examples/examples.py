"""
Market Data Example for MorningPy
"""
from morningpy.api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value,
    get_market_info
)

from morningpy.api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info
)

from morningpy.api.timeseries import (
    get_intraday_timeseries,
    get_historical_timeseries
)

from morningpy.api.news import get_headline_news


def run():
    get_market_us_calendar_info(date=["2025-11-12"],info_type="earnings")
    get_market_commodities()
    get_market_currencies()
    get_market_movers(mover_type=["gainers", "losers", "actives"])
    get_market_indexes(index_type="americas")
    get_market_fair_value(value_type="overvaluated")
    get_market_info(info_type="commodities")
    get_headline_news(market="Spain",news="economy",edition="Central Europe")
    get_financial_statement(
        statement_type="Income Statement",
        report_frequency="Quarterly",
        security_id=["0P000115U4"])
    get_holding_info(performance_id=["0P0001PU03", "0P0001BG3E"])
    get_holding(performance_id=["0P0001PU03", "0P00013Z57"])
    get_intraday_timeseries(
            security_id=["0P0000OQN8"],
            start_date="2024-11-21",
            end_date="2025-11-21",
            frequency="1min",
            pre_after=False
        )
    get_historical_timeseries(
        security_id=["0P0000OQN8"],
        start_date="2010-11-05",
        end_date="2025-11-05",
        frequency="daily",
        pre_after=False
    )

if __name__ == "__main__":
    run()
