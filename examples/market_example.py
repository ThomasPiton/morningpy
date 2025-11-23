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

def run():
    # US earnings calendar
    calendar_info = get_market_us_calendar_info(
        date=["2025-11-12"],
        info_type="earnings"
    )
    print("US Calendar Info:")
    print(calendar_info.head())

    # Commodities
    commodities = get_market_commodities()
    print("\nCommodities:")
    print(commodities.head())

    # Currencies
    currencies = get_market_currencies()
    print("\nCurrencies:")
    print(currencies.head())

    # Market Movers
    movers = get_market_movers(mover_type=["gainers", "losers", "actives"])
    print("\nMarket Movers:")
    print(movers.head())

    # Market Indexes
    indexes = get_market_indexes()
    print("\nMarket Indexes:")
    print(indexes.head())

    # Market Fair Value
    fair_value = get_market_fair_value()
    print("\nMarket Fair Value:")
    print(fair_value.head())

    # General Market Info
    market_info = get_market_info()
    print("\nMarket Info:")
    print(market_info.head())


if __name__ == "__main__":
    run()
