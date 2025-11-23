"""
Ticker Data Example for MorningPy
"""
from morningpy.api.ticker import (
    get_all_etfs,
    get_all_funds,
    get_all_stocks,
    get_all_securities,
    convert
)

def run():
    # ETFs
    etfs = get_all_etfs()
    print("ETFs:")
    print(etfs.head())

    # Stocks
    stocks = get_all_stocks()
    print("\nStocks:")
    print(stocks.head())

    # Funds
    funds = get_all_funds()
    print("\nFunds:")
    print(funds.head())

    # All securities (stocks + ETFs + funds + others, depending on Morningstar)
    securities = get_all_securities()
    print("\nAll Securities:")
    print(securities.head())

    # ID Conversion (Ticker / ISIN / Morningstar ID)
    converted = convert(["US7181721090", "0P0001PU03"])
    print("\nConverted IDs:")
    print(converted.head())


if __name__ == "__main__":
    run()
