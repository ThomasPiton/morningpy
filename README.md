# MorningPy

*A modern Python library for financial data — Stocks, ETFs, Funds, Indices, Financial Statements, Timeseries, and News.*

|||
|----------|--------|
| Testing / CI | [![Tests](https://github.com/ThomasPiton/morningpy/actions/workflows/tests.yml/badge.svg)](https://github.com/ThomasPiton/morningpy/actions) [![Codecov](https://codecov.io/gh/ThomasPiton/morningpy/branch/main/graph/badge.svg?token=YOUR_CODECOV_TOKEN)](https://codecov.io/gh/ThomasPiton/morningpy) |
| Package | [![PyPI](https://img.shields.io/pypi/v/morningpy.svg)](https://pypi.org/project/morningpy/) [![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/) |
| Meta | [![Docs](https://img.shields.io/badge/docs-online-brightgreen.svg)](https://ThomasPiton.github.io/morningpy/) [![License](https://img.shields.io/badge/license-MIT-green.svg)](https://github.com/ThomasPiton/morningpy/blob/main/LICENSE)  [![Code Style](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black) [![Powered by NumFOCUS](https://img.shields.io/badge/NumFOCUS-powered-blue.svg)](https://numfocus.org/) |

## Overview

**MorningPy** provides a Pythonic and intuitive interface to access financial and market data from **Morningstar.com**.  
It is designed for students, practitioners, and researchers who need fast, consistent, and transparent access to high-quality market information.

MorningPy enables professional-grade data handling and covers a wide range of financial instruments, including:

- Stocks, ETFs, Funds, Indices  
- Financial Statements and Holdings  
- Historical and Intraday Timeseries  
- News and Market Updates  
- Ticker conversion  

## News

- First version of MorningPy released — excited to start the journey! 🎉

## Installation

Install from PyPI:

```bash
pip install morningpy
```

Install from source:

```bash
git clone https://github.com/ThomasPiton/morningpy.git
pip install -e morningpy
```

## Usage

Importing modules:

```python
# Market data
from morningpy.api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value,
    get_market_info
)

# Timeseries data
from morningpy.api.timeseries import (
    get_intraday_timeseries,
    get_historical_timeseries
)

# Security data
from morningpy.api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info
)

# Ticker info
from morningpy.api.ticker import (
    get_all_etfs,
    get_all_funds,
    get_all_stocks,
    get_all_securities,
    convert
)

# News
from morningpy.api.news import get_headline_news
```

Examples - Market Information: 

```python
from morningpy.api.market import (
    get_market_us_calendar_info,
    get_market_commodities,
    get_market_currencies,
    get_market_movers,
    get_market_indexes,
    get_market_fair_value,
    get_market_info
)

# US earnings calendar
calendar_info = get_market_us_calendar_info(date=["2025-11-12"], info_type="earnings")
print(calendar_info.head())

# Commodities and currencies
commodities = get_market_commodities()
currencies = get_market_currencies()

# Market movers
movers = get_market_movers(mover_type=["gainers", "losers", "actives"])

# Indexes and fair value
indexes = get_market_indexes(index_type=["americas", "us"])
fair_value = get_market_fair_value(value_type=["overvaluated", "undervaluated"])

# General market info
market_info = get_market_info(info_type=["global_barometer","commodities"])
```

Examples - Timeseries: 

```python
from morningpy.api.timeseries import (
    get_intraday_timeseries,
    get_historical_timeseries
)

# Intraday data
intraday = get_intraday_timeseries(
    security_id=["0P00009WL3"],
    start_date="2024-11-08",
    end_date="2025-11-07",
    frequency="1min",
    pre_after=False
)

# Historical daily data
historical = get_historical_timeseries(
    security_id=["0P0000OQN8","0P0001RWKZ"],
    start_date="2010-11-05",
    end_date="2025-11-05",
    frequency="daily",
    pre_after=False
)
```

Examples - Security: 

```python
from morningpy.api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info
)

# Financial statement
income_statement = get_financial_statement(
    statement_type="Income Statement",
    report_frequency="Quarterly",
    security_id=["0P000115U4"]
)

# Holdings
holding_info = get_holding_info(
    performance_id=["0P0001PU03", "0P0001BG3E"]
)
holding = get_holding(
    performance_id=["0P0001PU03", "0P00013Z57"]
)
```

Examples - Ticker: 

```python
from morningpy.api.ticker import (
    get_all_etfs,
    get_all_funds,
    get_all_stocks,
    get_all_securities,
    convert
)

# Retrieve all ETFs
etfs = get_all_etfs()

# Retrieve all stocks and funds
stocks = get_all_stocks()
funds = get_all_funds()

# Convert ticker or ISIN
converted = convert(["US7181721090", "0P0001PU03"])
```

Examples - News: 

```python
from morningpy.api.news import get_headline_news

# Get headline news
news = get_headline_news(
    market="Spain",
    news="economy",
    edition="Central Europe"
)
print(news.head())
```


## Documentation

Advanced documentation will be available at MorningPy Docs

## Contribution

Contributions are always welcome!
You can:
- Open an issue to discuss a feature or bug
- Submit a pull request
- Contact us to discuss the best way to contribute