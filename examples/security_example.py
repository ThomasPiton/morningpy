"""
Security Data Example for MorningPy
"""
from morningpy.api.security import (
    get_financial_statement,
    get_holding,
    get_holding_info
)

def run():
    
    # Financial statement
    income_statement = get_financial_statement(
        statement_type=["Cash Flow Statement","Balance Sheet","Income Statement"],
        report_frequency="Annualy",
        security_id=["0P000003RE"]
    ).to_pandas_dataframe()
    print("Income Statement:")
    print(income_statement.head())

    # Holdings
    holding_info = get_holding_info(
        performance_id=["0P0001PU03", "0P0001BG3E"]
    ).to_pandas_dataframe()
    print("\nHolding Info:")
    print(holding_info.head())

    holding = get_holding(
        performance_id=["0P0001PU03", "0P00013Z57"]
    )
    
    df = holding.to_pandas_dataframe()
    pl = holding.to_polars_dataframe()
    arr = holding.to_arrow_table()
    dk = holding.to_dask_dataframe()
    
    print("\nHolding Data:")
    print(holding.head())

    return "Correctly extracted !"
