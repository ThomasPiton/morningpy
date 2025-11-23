DEFAULT_HEADERS = {
    "accept": "application/json, text/plain, */*",
    "accept-encoding": "gzip, deflate, br",
    "accept-language": "fr-FR,fr;q=0.9,en-US;q=0.8,en;q=0.7",
    "origin": "https://www.morningstar.com",
    "sec-ch-ua" : '"Chromium";v="142", "Google Chrome";v="142", "Not_A Brand";v="99"',
    "sec-ch-ua-mobile": "?0",
    "sec-ch-ua-platform": "Windows",
    "sec-fetch-dest": "empty",
    "sec-fetch-mode": "cors",
    "sec-fetch-site": "same-origin",
    "user-agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/142.0.0.0 Safari/537.36"
    ),
}

URLS = {
    # "key_api":"https://global.morningstar.com/assets/quotes/1.0.36/sal-components.umd.min.3594.js",
    "key_api":"https://global.morningstar.com/assets/quotes/1.0.41/sal-components.umd.min.7516.js",
    "maas_token":"https://www.morningstar.com/api/v2/stores/maas/token"
}

TICKERS_FILE = "tickers.parquet"

EXTRACTOR_CLASS_FUNC = {
    "MarketCalendarUsInfoExtractor":"get_market_us_calendar_info",
    "MarketCommoditiesExtractor":"get_market_commodities",
    "MarketCurrenciesExtractor":"get_market_currencies",
    "MarketMoversExtractor":"get_market_movers",
    "MarketIndexesExtractor":"get_market_indexes",
    "MarketFairValueExtractor":"get_market_fair_value",
    "MarketExtractor":"get_market_info",
    "HeadlineNewsExtractor":"get_headline_news",
    "FinancialStatementExtractor":"get_financial_statement",
    "HoldingExtractor":"get_holding",
    "HoldingInfoExtractor":"get_holding_info",
    "HistoricalTimeseriesExtractor":"get_historical_timeseries",
    "IntradayTimeseriesExtractor":"get_intraday_timeseries",
    # "TickerExtractor":"get_all_etfs",
    # "TickerExtractor":"get_all_funds",
    # "TickerExtractor":"get_all_securities",
    # "TickerExtractor":"get_all_stocks",
    # "TickerExtractor":"convert",

}