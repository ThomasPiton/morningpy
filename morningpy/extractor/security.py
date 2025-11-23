import pandas as pd
from typing import Any, Dict, List,Union

from morningpy.core.security_convert import IdSecurityConverter
from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.security import *
from morningpy.schema.security import *
    
class FinancialStatementExtractor(BaseExtractor):
    """
    Extracts financial statement data (income statement, balance sheet, cash flow) from Morningstar.

    This extractor handles:
        - Validating user inputs (tickers, ISINs, security IDs, statement type, report frequency)
        - Building API requests per security and statement type
        - Processing the API response into a standardized pandas DataFrame with normalized sub-levels

    Attributes:
        ticker (str | List[str] | None): Ticker symbols of securities.
        isin (str | List[str] | None): ISIN codes of securities.
        security_id (str | List[str] | None): Morningstar internal security IDs.
        performance_id (str | List[str] | None): Morningstar performance IDs.
        statement_type (str): Type of statement to extract ("income", "balance", "cashflow").
        report_frequency (str): Report frequency ("annual", "quarterly", etc.).
        security_id (List[str]): Converted and validated list of Morningstar security IDs.
        url (str): Base API URL for financial statements.
        endpoint (dict): Endpoint mapping per statement type.
        valid_frequency (dict): Valid frequencies for reports.
        frequency_mapping (dict): Mapping from user-friendly frequency to API frequency.
    """
    config = FinancialStatementConfig
    schema = FinancialStatementSchema
    
    def __init__(
        self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,
        statement_type: str = None,
        report_frequency: str = None
        ):
     
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        
        super().__init__(client)

        self.url = self.config.API_URL
        self.endpoint = self.config.ENDPOINT
        self.valid_frequency = self.config.VALID_FREQUENCY
        self.frequency_mapping = self.config.MAPPING_FREQUENCY
        self.report_frequency = report_frequency 
        self.statement_type = statement_type
        self.filter_values = self.config.FILTER_VALUE
        self.params = self.config.PARAMS
        
        self.security_id = IdSecurityConverter(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).convert()      

    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        temp_url = self.url
        self.url = [temp_url + f"{sec_id}/{self.endpoint[self.statement_type]}/detail" for sec_id in self.security_id]
        self.params["dataType"] = self.frequency_mapping[self.report_frequency]
        
    def _process_response(self, response: dict) -> pd.DataFrame:

        if not isinstance(response, dict) or not response:
            return pd.DataFrame()

        column_defs = response.get("columnDefs", [])
        period_col = column_defs[5:]

        data_rows = []
        all_depths = []

        def clean_label(label: str) -> str:
            return " ".join(w for w in label.split() if w.lower() != "total")

        def normalize_year_value(v):
            if v in (None, "_PO_"):
                return 0  
            if isinstance(v, (int, float)):
                return v
            try:
                return float(v)
            except:
                return 0

        def walk(node, path):
            # response uses "label" not "name"
            current_label = clean_label(node.get("label", "")).strip()
            current_path = path + [current_label]

            # update max depth tracker
            all_depths.append(len(current_path))

            # leaf → create row
            if not node.get("subLevel"):
                datum = node.get("datum", [])
                if datum:
                    values = [normalize_year_value(v) for v in datum[5:]]
                    row = {"_subpath": current_path}

                    for year, val in zip(period_col, values):
                        row[year] = val

                    data_rows.append(row)

            for child in node.get("subLevel", []):
                walk(child, current_path)

        for item in response.get("rows", []):
            walk(item, [])

        if not data_rows:
            return pd.DataFrame()

        max_depth = max(all_depths)

        cleaned_rows = []

        for row in data_rows:
            path = row.pop("_subpath")
            full_path = path + [path[-1]] * (max_depth - len(path))
            for i, p in enumerate(full_path, start=0):
                row[f"sub_type{i}"] = p

            cleaned_rows.append(row)

        df = pd.DataFrame(cleaned_rows)
        df = df.rename(columns={"sub_type0": "statement_type"})
        subtype_cols = ["statement_type"] + [f"sub_type{i}" for i in range(1, max_depth)]
        df = df[subtype_cols + period_col]
        df = df[df["statement_type"].isin([self.filter_values[self.statement_type]])]
        df[period_col] = df[period_col]*10**6
        return df
    
class HoldingExtractor(BaseExtractor):
    """
    Extracts ETF or fund holdings data from Morningstar.

    This extractor handles:
        - Validating user inputs (tickers, ISINs, security IDs)
        - Building API requests per security
        - Processing ETF/fund holdings including equity, bond, and other holdings into a standardized pandas DataFrame

    Attributes:
        ticker (str | List[str] | None): Ticker symbols of securities.
        isin (str | List[str] | None): ISIN codes of securities.
        security_id (str | List[str] | None): Morningstar internal security IDs.
        performance_id (str | List[str] | None): Morningstar performance IDs.
        security_id (List[str]): Converted and validated list of Morningstar security IDs.
        url (str): Base API URL for holdings.
        params (dict): Request parameters for API calls.
        rename_columns (dict): Mapping of API column names to standardized names.
        str_columns (List[str]): Columns to treat as strings.
        numeric_columns (List[str]): Columns to treat as numeric.
        final_columns (List[str]): Final column order for the DataFrame.
    """
    config = HoldingConfig
    schema = HoldingSchema
    
    def __init__(
        self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,):
     
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        
        super().__init__(client)

        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        
        self.security_id = IdSecurityConverter(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).convert()      

    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        temp_url = self.url
        self.url = [temp_url + f"{sec_id}/data" for sec_id in self.security_id]
        
    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar ETF holdings response across equity, bond, and other holdings."""
        if not isinstance(response, dict) or not response:
            return pd.DataFrame()

        parent_security_id = response.get("secId")
        holding_pages = ["equityHoldingPage", "boldHoldingPage", "otherHoldingPage"]

        rows = []

        for page_key in holding_pages:
            page = response.get(page_key, {})
            holding_list = page.get("holdingList", [])
            
            for holding in holding_list:
                rows.append({
                    "parent_security_id": parent_security_id,
                    "child_security_id": holding.get("secId"),
                    "performance_id": holding.get("performanceId"),
                    "security_name": holding.get("securityName"),
                    "holding_type_id": holding.get("holdingTypeId"),
                    "holding_type": holding.get("holdingType"),
                    "weighting": holding.get("weighting"),
                    "number_of_share": holding.get("numberOfShare"),
                    "market_value": holding.get("marketValue"),
                    "original_market_value": holding.get("originalMarketValue"),
                    "share_change": holding.get("shareChange"),
                    "country": holding.get("country"),
                    "ticker": holding.get("ticker"),
                    "isin": holding.get("isin"),
                    "cusip": holding.get("cusip"),
                    "total_return_1y": holding.get("totalReturn1Year"),
                    "forward_pe_ratio": holding.get("forwardPERatio"),
                    "stock_rating": holding.get("stockRating"),
                    "assessment": holding.get("assessment"),
                    "economic_moat": holding.get("economicMoat"),
                    "sector": holding.get("sector"),
                    "sector_code": holding.get("sectorCode"),
                    "secondary_sector_id": holding.get("secondarySectorId"),
                    "super_sector_name": holding.get("superSectorName"),
                    "primary_sector_name": holding.get("primarySectorName"),
                    "secondary_sector_name": holding.get("secondarySectorName"),
                    "first_bought_date": holding.get("firstBoughtDate"),
                    "maturity_date": holding.get("maturityDate"),
                    "coupon": holding.get("coupon"),
                    "currency": holding.get("currency"),
                    "currency_name": holding.get("currencyName"),
                    "local_currency_code": holding.get("localCurrencyCode"),
                    "prospectus_net_expense_ratio": holding.get("prospectusNetExpenseRatio"),
                    "one_year_return": holding.get("oneYearReturn"),
                    "morningstar_rating": holding.get("morningstarRating"),
                    "ep_used_for_overall_rating": holding.get("epUsedForOverallRating"),
                    "analyst_rating": holding.get("analystRating"),
                    "medalist_rating": holding.get("medalistRating"),
                    "medalist_rating_label": holding.get("medalistRatingLabel"),
                    "overall_rating_publish_date_utc": holding.get("overallRatingPublishDateUtc"),
                    "total_assets": holding.get("totalAssets"),
                    "ttm_yield": holding.get("ttmYield"),
                    "ep_used_for_1y_return": holding.get("epUsedFor1YearReturn"),
                    "morningstar_category": holding.get("morningstarCategory"),
                    "total_assets_magnitude": holding.get("totalAssetsMagnitude"),
                    "last_turnover_ratio": holding.get("lastTurnoverRatio"),
                    "sus_esg_risk_score": holding.get("susEsgRiskScore"),
                    "sus_esg_risk_globes": holding.get("susEsgRiskGlobes"),
                    "esg_as_of_date": holding.get("esgAsOfDate"),
                    "sus_esg_risk_category": holding.get("susEsgRiskCategory"),
                    "management_expense_ratio": holding.get("managementExpenseRatio"),
                    "qual_rating": holding.get("qualRating"),
                    "quant_rating": holding.get("quantRating"),
                    "best_rating_type": holding.get("bestRatingType"),
                    "security_type": holding.get("securityType"),
                    "domicile_country_id": holding.get("domicileCountryId"),
                    "is_momentum_filter_flag": holding.get("isMomentumFilterFlag"),
                })

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)

        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A")
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)

        if "date" in df.columns:
            df["date"] = pd.to_datetime(df["date"], errors="coerce")

        df.sort_values(by=["parent_security_id", "child_security_id"], inplace=True, ignore_index=True)

        return df
             
class HoldingInfoExtractor(BaseExtractor):
    """
    Extracts high-level metadata about ETF or fund holdings from Morningstar.

    This extractor handles:
        - Validating user inputs (tickers, ISINs, security IDs)
        - Building API requests per security
        - Processing holding info response into a single-row pandas DataFrame per security, including top holdings, turnover, and other portfolio statistics

    Attributes:
        ticker (str | List[str] | None): Ticker symbols of securities.
        isin (str | List[str] | None): ISIN codes of securities.
        security_id (str | List[str] | None): Morningstar internal security IDs.
        performance_id (str | List[str] | None): Morningstar performance IDs.
        security_id (List[str]): Converted and validated list of Morningstar security IDs.
        url (str): Base API URL for holding info.
        params (dict): Request parameters for API calls.
        rename_columns (dict): Mapping of API column names to standardized names.
        str_columns (List[str]): Columns to treat as strings.
        numeric_columns (List[str]): Columns to treat as numeric.
        final_columns (List[str]): Final column order for the DataFrame.
    """
    config = HoldingInfoConfig
    schema = HoldingInfoSchema
    
    def __init__(
        self,
        ticker: Union[str, List[str]] = None,
        isin: Union[str, List[str]] = None,
        security_id: Union[str, List[str]] = None,
        performance_id: Union[str, List[str]] = None,):
     
        client = BaseClient(auth_type=self.config.REQUIRED_AUTH)
        
        super().__init__(client)

        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        
        self.security_id = IdSecurityConverter(
            ticker=ticker,
            isin=isin,
            security_id=security_id,
            performance_id=performance_id
        ).convert()      

    def _check_inputs(self) -> None:
        pass

    def _build_request(self) -> None:
        temp_url = self.url
        self.url = [temp_url + f"{sec_id}/data" for sec_id in self.security_id]
        
    def _process_response(self, response: dict) -> pd.DataFrame:
        """Process Morningstar ETF Holding Info API response safely and cleanly."""
        if not isinstance(response, dict) or not response:
            return pd.DataFrame()

        row = {
            "master_portfolio_id": response.get("masterPortfolioId"),
            "security_id": response.get("secId"),
            "base_currency_id": response.get("baseCurrencyId"),
            "domicile_country_id": response.get("domicileCountryId"),
            "portfolio_suppression": response.get("portfolioSuppression"),
            "asset_type": response.get("assetType"),
            "portfolio_latest_date_footer": response.get("portfolioLastestDateFooter"),
            "number_of_holding": response.get("numberOfHolding"),
            "number_of_equity_holding": response.get("numberOfEquityHolding"),
            "number_of_bond_holding": response.get("numberOfBondHolding"),
            "number_of_other_holding": response.get("numberOfOtherHolding"),
            "number_of_holding_short": response.get("numberOfHoldingShort"),
            "number_of_equity_holding_short": response.get("numberOfEquityHoldingShort"),
            "number_of_bond_holding_short": response.get("numberOfBondHoldingShort"),
            "number_of_other_holding_short": response.get("numberOfOtherHoldingShort"),
            "top_n_count": response.get("topNCount"),
            "number_of_equity_holding_percentage": response.get("numberOfEquityHoldingPer"),
            "number_of_bond_holding_percentage": response.get("numberOfBondHoldingPer"),
            "number_of_other_holding_percentage": response.get("numberOfOtherHoldingPer"),
        }

        holding_summary = response.get("holdingSummary", {}) or {}
        row["top_holding_weighting"] = holding_summary.get("topHoldingWeighting")
        row["last_turnover"] = holding_summary.get("lastTurnover")
        row["last_turnover_date"] = (
            holding_summary.get("LastTurnoverDate")
            or holding_summary.get("lastTurnoverDate")
        )

        df = pd.DataFrame([row]).copy() 

        if hasattr(self, "rename_columns"):
            df.rename(columns=self.rename_columns, inplace=True, errors="ignore")

        if hasattr(self, "final_columns"):
            missing_cols = [c for c in self.final_columns if c not in df.columns]
            for col in missing_cols:
                df[col] = None
            df = df[self.final_columns]

        if hasattr(self, "str_columns") and self.str_columns:
            df.loc[:, self.str_columns] = (
                df[self.str_columns].fillna("N/A").infer_objects(copy=False)
            )

        if hasattr(self, "numeric_columns") and self.numeric_columns:
            df.loc[:, self.numeric_columns] = (
                df[self.numeric_columns].fillna(0).infer_objects(copy=False)
            )

        for date_col in ["last_turnover_date", "portfolio_latest_date_footer"]:
            if date_col in df.columns:
                df.loc[:, date_col] = pd.to_datetime(df[date_col], errors="coerce")

        return df