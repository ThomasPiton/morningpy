import asyncio
from typing import Union, List, Literal

from morningpy.extractor.security import *
from morningpy.core.interchange import DataFrameInterchange

def get_financial_statement(
    ticker: Union[str, List[str]] = None, 
    isin: Union[str, List[str]] = None, 
    security_id: Union[str, List[str]] = None, 
    performance_id: Union[str, List[str]] = None, 
    statement_type: Literal["Balance Sheet", "Cash Flow Statement", "Income Statement"] = None,
    report_frequency: Literal["Annualy", "Quarterly"] = None
):
    """
    Retrieve financial statements for one or multiple securities.

    This function wraps the `FinancialStatementExtractor`, allowing the user
    to request balance sheets, cash flow statements, or income statements on
    an annual or quarterly basis. It supports queries by ticker, ISIN, security ID,
    or performance ID.

    Parameters
    ----------
    ticker : str or list of str, optional
        The ticker symbol(s) of the security.
    isin : str or list of str, optional
        The ISIN code(s) of the security.
    security_id : str or list of str, optional
        Internal Morningstar security identifier(s).
    performance_id : str or list of str, optional
        Morningstar performance identifier(s).
    statement_type : {"Balance Sheet", "Cash Flow Statement", "Income Statement"}, optional
        Type of financial statement to retrieve.
    report_frequency : {"Annualy", "Quarterly"}, optional
        Frequency of reporting for the statement.

    Returns
    -------
    DataFrameInterchange
        A standardized dataframe-like structure containing the requested
        financial statement data.
    """
    extractor = FinancialStatementExtractor(
        ticker=ticker,
        isin=isin,
        security_id=security_id,
        performance_id=performance_id,
        statement_type=statement_type,
        report_frequency=report_frequency
    )
    
    return asyncio.run(extractor.run())


def get_holding_info(
    ticker: Union[str, List[str]] = None, 
    isin: Union[str, List[str]] = None, 
    security_id: Union[str, List[str]] = None, 
    performance_id: Union[str, List[str]] = None
) -> DataFrameInterchange:
    """
    Retrieve holding metadata for one or more securities.

    This function returns high-level information about the holdings of a 
    security, such as issuer details, classification, and other descriptive data.

    Parameters
    ----------
    ticker : str or list of str, optional
        The ticker symbol(s) of the security.
    isin : str or list of str, optional
        The ISIN code(s) of the security.
    security_id : str or list of str, optional
        Internal Morningstar security identifier(s).
    performance_id : str or list of str, optional
        Morningstar performance identifier(s).

    Returns
    -------
    DataFrameInterchange
        A dataframe-like structure containing descriptive holding information.
    """
    extractor = HoldingInfoExtractor(
        ticker=ticker,
        isin=isin,
        security_id=security_id,
        performance_id=performance_id
    )
    
    return asyncio.run(extractor.run())


def get_holding(
    ticker: Union[str, List[str]] = None, 
    isin: Union[str, List[str]] = None, 
    security_id: Union[str, List[str]] = None, 
    performance_id: Union[str, List[str]] = None
) -> DataFrameInterchange:
    """
    Retrieve portfolio holdings for a given security.

    This function fetches the detailed list of holdings within a security,
    typically for funds or ETFs. It includes weights, positions, and security-level
    attributes.

    Parameters
    ----------
    ticker : str or list of str, optional
        The ticker symbol(s) of the security.
    isin : str or list of str, optional
        The ISIN code(s) of the security.
    security_id : str or list of str, optional
        Internal Morningstar security identifier(s).
    performance_id : str or list of str, optional
        Morningstar performance identifier(s).

    Returns
    -------
    DataFrameInterchange
        A dataframe-like structure containing detailed holdings data.
    """
    extractor = HoldingExtractor(
        ticker=ticker,
        isin=isin,
        security_id=security_id,
        performance_id=performance_id
    )
    
    return asyncio.run(extractor.run())
