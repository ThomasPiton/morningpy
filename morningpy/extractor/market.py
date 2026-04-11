import pandas as pd
from datetime import datetime
from typing import Any, Dict, List, Union

from morningpy.core.client import BaseClient
from morningpy.core.base_extract import BaseExtractor
from morningpy.config.market import *
from morningpy.schema.market import *


class MarketCalendarUsInfoExtractor(BaseExtractor):
    """
    Extracts U.S. market calendar events from Morningstar.

    This extractor retrieves earnings announcements, economic releases, IPOs, 
    and stock splits for specified dates from the Morningstar calendar API.

    Attributes
    ----------
    date : str or list of str
        The date(s) to query in YYYY-MM-DD format.
    info_type : str
        Type of calendar information to extract.
    url : str
        Base API URL for market calendar events.
    params : dict
        Request parameters for API calls.
    valid_inputs : list of str
        List of allowed info_type values.
    rename_columns : dict
        Mapping of API column names to standardized names.
    final_columns : list of str
        Final column order for the DataFrame.
    """
    
    config = MarketCalendarUsInfoConfig
    schema = MarketCalendarUsInfoSchema

    def __init__(self, date: Union[str, List[str]], info_type: str):
        """
        Initialize the MarketCalendarUsInfoExtractor.

        Parameters
        ----------
        date : str or list of str
            Single date or list of dates to query in YYYY-MM-DD format
            (e.g., '2024-01-15' or ['2024-01-15', '2024-01-16']).
        info_type : str
            Type of calendar information to extract. Must be one of:
            'earnings', 'economic-releases', 'ipos', or 'splits'.

        Notes
        -----
        The extractor will retrieve calendar events for each specified date.
        Different info_types return different fields in the resulting DataFrame.
        """
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )
        super().__init__(client)

        self.date = date
        self.info_type = info_type.lower().strip() if info_type else ""
        self.url = self.config.API_URL
        self.params = self.config.PARAMS
        self.valid_inputs = self.config.VALID_INPUTS

    def _check_inputs(self) -> None:
        """
        Validate user inputs.

        Validates that info_type is valid and all dates are in YYYY-MM-DD format.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If info_type is invalid or dates are malformed.
        """
        if self.info_type not in self.valid_inputs:
            raise ValueError(
                f"Invalid info_type '{self.info_type}', must be one of {self.valid_inputs}"
            )
        
        dates = self.date if isinstance(self.date, list) else [self.date]
        for d in dates:
            try:
                datetime.strptime(d, "%Y-%m-%d")
            except ValueError:
                raise ValueError(f"Date '{d}' must be in format YYYY-MM-DD (e.g., '2024-01-15').")

    def _build_request(self) -> None:
        """
        Build API request dictionaries for each date.

        Creates one request per date with the specified info_type category.

        Returns
        -------
        None
        """
        dates = self.date if isinstance(self.date, list) else [self.date]
        
        self.requests = [
            {
                "url": self.url,
                "params": {
                    **self.params,
                    "date": d,
                    "category": self.info_type
                }
            }
            for d in dates
        ]

    def _process_response(self, response: dict, param: dict = None) -> pd.DataFrame:
        """
        Process Morningstar calendar response.

        Extracts calendar event data and flattens it into a standardized DataFrame.
        The structure varies based on info_type (earnings, economic-releases, ipos, splits).

        Parameters
        ----------
        response : dict
            API response containing calendar event data.
        param : dict, optional
            Request parameters used for the API call, containing date and info_type.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with calendar event details. Returns empty DataFrame 
            if response is invalid or empty. Column structure varies by info_type.

        Notes
        -----
        For earnings: includes company information, EPS data, and surprise metrics.
        For economic-releases: includes release details, estimates, and actual values.
        For ipos: includes offering details, pricing, and underwriter information.
        For splits: includes split ratios and relevant dates.
        """
        if not response or "page" not in response or "results" not in response["page"]:
            return pd.DataFrame()

        rows = []
        results = response["page"]["results"]
        info_type = (param or {}).get("category", self.info_type).lower()

        for result in results:
            details = result.get("details", {})
            securities = result.get("securities", [])

            base_info = {
                "calendar_date": result.get("date"),
                "updated_at": result.get("updatedAt"),
                "calendar": result.get("calendar"),
                "info_type": info_type,
            }

            if info_type == "earnings":
                for sec in securities or [{}]:
                    row = {
                        **base_info,
                        "security_id": sec.get("securityID"),
                        "ticker": sec.get("ticker"),
                        "name": sec.get("name"),
                        "exchange": sec.get("exchange"),
                        "isin": sec.get("isin"),
                        "exchange_country": sec.get("exchangeCountry"),
                        "market_cap": sec.get("marketCap"),
                        "quarter_end_date": details.get("quarterEndDate"),
                        "actual_diluted_eps": details.get("actualDilutedEps"),
                        "net_income": details.get("netIncome"),
                        "consensus_estimate": details.get("consensusEstimate"),
                        "percentage_surprise": details.get("percentageSurprise"),
                        "quarterly_sales": details.get("quarterlySales"),
                    }
                    rows.append(row)

            elif info_type == "economic-releases":
                row = {
                    **base_info,
                    "release": details.get("release"),
                    "period": details.get("period"),
                    "release_time": details.get("releaseTime"),
                    "consensus_estimate": (details.get("consensusEstimate") or {}).get("value"),
                    "briefing_estimate": (details.get("briefingEstimate") or {}).get("value"),
                    "after_release_actual": (details.get("afterReleaseActual") or {}).get("value"),
                    "prior_release_actual": (details.get("priorReleaseActual") or {}).get("value"),
                }
                rows.append(row)

            elif info_type == "ipos":
                for sec in securities or [{}]:
                    company = details.get("company", {})
                    row = {
                        **base_info,
                        "security_id": sec.get("securityID"),
                        "ticker": sec.get("ticker") or details.get("ticker"),
                        "name": sec.get("name") or company.get("name"),
                        "exchange": sec.get("exchange"),
                        "market_cap": sec.get("marketCap"),
                        "share_value": details.get("shareValue"),
                        "opened_share_value": details.get("openedShareValue"),
                        "lead_underwriter": details.get("leadUnderWriter"),
                        "initial_shares": details.get("initialShares"),
                        "initial_low_range": details.get("initialLowRange"),
                        "initial_high_range": details.get("initialHighRange"),
                        "date_priced": details.get("datePriced"),
                        "week_priced": details.get("weekPriced"),
                        "company_description": company.get("description"),
                    }
                    rows.append(row)

            elif info_type == "splits":
                for sec in securities or [{}]:
                    company = details.get("company", {})
                    row = {
                        **base_info,
                        "security_id": sec.get("securityID"),
                        "ticker": sec.get("ticker") or details.get("ticker"),
                        "name": sec.get("name") or company.get("name"),
                        "exchange": sec.get("exchange"),
                        "market_cap": sec.get("marketCap"),
                        "share_worth": details.get("shareWorth"),
                        "old_share_worth": details.get("oldShareWorth"),
                        "ex_date": details.get("exDate"),
                        "announce_date": details.get("announceDate"),
                        "payable_date": details.get("payableDate"),
                    }
                    rows.append(row)

            else:
                row = {**base_info, **details}
                rows.append(row)

        return pd.DataFrame(rows)


class MarketFairValueExtractor(BaseExtractor):
    """
    Extracts Morningstar fair value information for stocks.

    Retrieves stocks categorized as overvalued or undervalued based on 
    Morningstar's fair value estimates.

    Attributes
    ----------
    value_type : str or list of str
        Stock value categories to extract.
    url : str
        Base API URL for fair value data.
    rename_columns : dict
        Mapping of API column names to standardized names.
    str_columns : list of str
        Columns treated as strings.
    numeric_columns : list of str
        Columns treated as numeric.
    final_columns : list of str
        Final column order for the DataFrame.
    valid_inputs : list of str
        List of allowed value_type values.
    mapping_inputs : dict
        Mapping of value_type to API component keys.
    """
    
    config = MarketFairValueConfig
    schema = MarketFairValueSchema

    def __init__(self, value_type: Union[str, List[str]] = "overvaluated"):
        """
        Initialize the MarketFairValueExtractor.

        Parameters
        ----------
        value_type : str or list of str, optional
            Stock value categories to extract. Must be one of: 'overvaluated' 
            or 'undervalued' (e.g., 'overvaluated' or ['overvaluated', 'undervalued']).
            Default is 'overvaluated'.

        Notes
        -----
        The extractor retrieves stocks from Morningstar's fair value lists,
        including valuation metrics, star ratings, and price information.
        """
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )
        super().__init__(client)

        self.value_type = value_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.mapping_inputs = self.config.MAPPING_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        """
        Validate user inputs.

        Ensures value_type is valid and converts to list format if needed.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If value_type is invalid.
        TypeError
            If value_type is not str or list of str.
        """
        if isinstance(self.value_type, str):
            if self.value_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid value_type '{self.value_type}', must be one of {self.valid_inputs}"
                )
            self.value_type = [self.value_type]
        elif isinstance(self.value_type, list):
            invalid = [m for m in self.value_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid value_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("value_type must be a str or list[str]")

    def _build_request(self) -> None:
        """
        Build API request dictionary.

        This endpoint requires no parameters beyond the URL.

        Returns
        -------
        None
        """
        self.requests = [{"url": self.url}]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar fair value response.

        Extracts and flattens fair value data for selected categories into a 
        standardized DataFrame.

        Parameters
        ----------
        response : dict
            API response containing fair value component data.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes security identifiers, valuation metrics, star ratings, 
            and price information. Returns empty DataFrame if response is invalid.
            
            String columns are filled with "N/A" for missing values.
            Numeric columns are filled with 0 for missing values.
            Rows with missing security_id are dropped.

        Notes
        -----
        The method flattens nested field structures from the API response,
        extracting both primary values and nested properties while excluding
        date and currency metadata fields.
        """
        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        all_rows = []

        for key in self.value_type:
            comp_key = self.mapping_inputs.get(key)
            if not comp_key:
                continue

            comp = components.get(comp_key, {})
            payload = comp.get("payload", [])

            if isinstance(payload, dict):
                results = payload.get("results", [])
            elif isinstance(payload, list):
                results = payload
            else:
                continue

            if not results:
                continue

            rows = []
            for item in results:
                fields = item.get("fields", {})
                meta = item.get("meta", {})

                row = {
                    "securityID": meta.get("securityID"),
                    "performanceID": meta.get("performanceID"),
                    "companyID": meta.get("companyID"),
                    "exchange": meta.get("exchange"),
                    "ticker": meta.get("ticker"),
                    "category": comp_key
                }

                for field_key, field_data in fields.items():
                    if not isinstance(field_data, dict) or "value" not in field_data:
                        continue

                    row[field_key] = field_data.get("value")

                    props = field_data.get("properties", {})
                    for prop_key, prop_val in props.items():
                        if any(x in prop_key.lower() for x in ["date", "currency"]):
                            continue
                        row[f"{field_key}_{prop_key}"] = prop_val.get("value")

                rows.append(row)

            all_rows.extend(rows)

        if not all_rows:
            return pd.DataFrame()

        df = pd.DataFrame(all_rows)
        df.rename(columns=self.rename_columns, inplace=True)
        df = df.dropna(subset=["security_id"]).reset_index(drop=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
   
        return df


class MarketIndexesExtractor(BaseExtractor):
    """
    Extracts market index data from Morningstar.

    Retrieves index performance data for various global markets including 
    Americas, Europe, Asia-Pacific, and other regions.

    Attributes
    ----------
    index_type : str or list of str
        Type of market index to extract.
    url : str
        Base API URL for market indexes.
    rename_columns : dict
        Mapping of API column names to standardized names.
    str_columns : list of str
        Columns treated as strings.
    numeric_columns : list of str
        Columns treated as numeric.
    final_columns : list of str
        Final column order for the DataFrame.
    valid_inputs : list of str
        List of allowed index_type values.
    mapping_inputs : dict
        Mapping of index_type to API component keys.
    """
    
    config = MarketIndexesConfig
    schema = MarketIndexesSchema

    def __init__(self, index_type: Union[str, List[str]] = "americas"):
        """
        Initialize the MarketIndexesExtractor.

        Parameters
        ----------
        index_type : str or list of str, optional
            Type of market index to extract (e.g., 'americas', 'europe', 
            'asia-pacific'). Can specify single index type or list of types.
            Default is 'americas'.

        Notes
        -----
        The extractor retrieves current index values, performance metrics,
        and change percentages for the specified regional market indexes.
        """
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )
        super().__init__(client)

        self.index_type = index_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.mapping_inputs = self.config.MAPPING_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        """
        Validate user inputs.

        Ensures index_type is valid and converts to list format if needed.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If index_type is invalid.
        TypeError
            If index_type is not str or list of str.
        """
        if isinstance(self.index_type, str):
            if self.index_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid index_type '{self.index_type}', must be one of {self.valid_inputs}"
                )
            self.index_type = [self.index_type]
        elif isinstance(self.index_type, list):
            invalid = [m for m in self.index_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid index_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("index_type must be a str or list[str]")

    def _build_request(self) -> None:
        """
        Build API request dictionary.

        This endpoint requires no parameters beyond the URL.

        Returns
        -------
        None
        """
        self.requests = [{"url": self.url}]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar index data.

        Extracts and flattens index performance data for selected regions into
        a standardized DataFrame.

        Parameters
        ----------
        response : dict
            API response containing market index component data.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes index identifiers, values, and performance metrics.
            Returns empty DataFrame if response is invalid.
            
            String columns are filled with "N/A" for missing values.
            Numeric columns are filled with 0 for missing values.
            Sorted by category and percent_net_change (descending).

        Notes
        -----
        Each index entry includes the current value, net change, percent change,
        and other relevant performance metrics.
        """
        if not response or "components" not in response:
            return pd.DataFrame()

        components = response.get("components", {})
        rows = []

        for key in self.index_type:
            comp = components.get(self.mapping_inputs[key], {})
            payload = comp.get("payload", [])
            if not isinstance(payload, list):
                continue

            for item in payload:
                row = {**item, "category": key.replace("Indexes", "").capitalize()}
                rows.append(row)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        df = df.sort_values(
            by=["category", "percent_net_change"],
            ascending=[True, False]
        ).reset_index(drop=True)

        return df

class MarketMoversExtractor(BaseExtractor):
    """
    Extracts market movers from Morningstar.

    Retrieves lists of top gaining and losing stocks based on daily performance.

    Attributes
    ----------
    mover_type : str or list of str
        Type of market movers to extract.
    url : str
        Base API URL for market movers.
    rename_columns : dict
        Mapping of API column names to standardized names.
    str_columns : list of str
        Columns treated as strings.
    numeric_columns : list of str
        Columns treated as numeric.
    final_columns : list of str
        Final column order for the DataFrame.
    valid_inputs : list of str
        List of allowed mover_type values.
    """
    
    config = MarketMoversConfig
    schema = MarketMoversSchema
    
    def __init__(self, mover_type: Union[str, List[str]] = "gainers"):
        """
        Initialize the MarketMoversExtractor.

        Parameters
        ----------
        mover_type : str or list of str, optional
            Type of market movers to extract. Must be 'gainers', 'losers', or
            both (e.g., 'gainers' or ['gainers', 'losers']).
            Default is 'gainers'.

        Notes
        -----
        The extractor retrieves top performers (gainers) or worst performers 
        (losers) including their price changes, volume, and other metrics.
        """
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )
        super().__init__(client)

        self.mover_type = mover_type
        self.url = self.config.API_URL
        self.valid_inputs = self.config.VALID_INPUTS
        self.rename_columns = self.config.RENAME_COLUMNS
        self.str_columns = self.config.STRING_COLUMNS
        self.numeric_columns = self.config.NUMERIC_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS       

    def _check_inputs(self) -> None:
        """
        Validate user inputs.

        Ensures mover_type is valid and converts to list format if needed.

        Returns
        -------
        None

        Raises
        ------
        ValueError
            If mover_type is invalid.
        TypeError
            If mover_type is not str or list of str.
        """
        if isinstance(self.mover_type, str):
            if self.mover_type not in self.valid_inputs:
                raise ValueError(
                    f"Invalid mover_type '{self.mover_type}', must be one of {self.valid_inputs}"
                )
            self.mover_type = [self.mover_type]
        elif isinstance(self.mover_type, list):
            invalid = [m for m in self.mover_type if m not in self.valid_inputs]
            if invalid:
                raise ValueError(
                    f"Invalid mover_type(s) {invalid}, must be among {self.valid_inputs}"
                )
        else:
            raise TypeError("mover_type must be a str or list[str]")

    def _build_request(self) -> None:
        """
        Build API request dictionary.

        This endpoint requires no parameters beyond the URL.

        Returns
        -------
        None
        """
        self.requests = [{"url": self.url}]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar market movers response.

        Extracts and flattens market mover data for selected categories into a 
        standardized DataFrame.

        Parameters
        ----------
        response : dict
            API response containing market movers data.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes security identifiers, prices, changes, and volume information.
            Returns empty DataFrame if response is invalid.
            
            String columns are filled with "N/A" for missing values.
            Numeric columns are filled with 0 for missing values.
            Sorted by percent_net_change (descending).

        Notes
        -----
        The method flattens nested field structures, extracting both primary 
        values and nested properties while excluding date and currency metadata.
        An updated_on timestamp and category label are added to each row.
        """
        if not response:
            return pd.DataFrame()

        all_rows = []

        for m_type in self.mover_type:
            data = response.get(m_type, [])
            if not data:
                continue

            rows = []
            for item in data:
                row = {}

                for key, value_dict in item.items():
                    if not isinstance(value_dict, dict) or "value" not in value_dict:
                        continue
                    
                    if key not in row:
                        row[key] = value_dict.get("value")

                    properties = value_dict.get("properties", {})
                    for prop_key, prop_value in properties.items():
                        col_name = f"{key}_{prop_key}"
                        if any(x in prop_key.lower() for x in ["date", "currency"]):
                            continue
                        if col_name not in row:
                            row[col_name] = prop_value.get("value")

                rows.append(row)

            if not rows:
                continue

            df = pd.DataFrame(rows)
            df["updated_on"] = response.get("updatedOn")
            df["category"] = m_type
            all_rows.append(df)

        if not all_rows:
            return pd.DataFrame()

        df = pd.concat(all_rows, ignore_index=True)
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df[self.str_columns] = df[self.str_columns].fillna("N/A") 
        df[self.numeric_columns] = df[self.numeric_columns].fillna(0)
        df = df.sort_values("percent_net_change", ascending=False).reset_index(drop=True)

        return df


class MarketCommoditiesExtractor(BaseExtractor):
    """
    Extracts commodities market data from Morningstar.

    Retrieves current prices, changes, and performance metrics for various 
    commodity instruments including energy, metals, and agricultural products.

    Attributes
    ----------
    url : str
        Base API URL for commodities data.
    rename_columns : dict
        Mapping of API column names to standardized names.
    final_columns : list of str
        Final column order for the DataFrame.
    """
    
    config = MarketCommoditiesConfig
    schema = MarketCommoditiesSchema
    
    def __init__(self):
        """
        Initialize the MarketCommoditiesExtractor.

        Notes
        -----
        This extractor requires no parameters and retrieves all available 
        commodity data from Morningstar's commodities endpoint.
        """
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )
        super().__init__(client)

        self.url = self.config.API_URL
        self.rename_columns = self.config.RENAME_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS

    def _check_inputs(self) -> None:
        """
        Validate user inputs.

        No validation needed for this extractor as it accepts no parameters.

        Returns
        -------
        None
        """
        pass

    def _build_request(self) -> None:
        """
        Build API request dictionary.

        This endpoint requires no parameters beyond the URL.

        Returns
        -------
        None
        """
        self.requests = [{"url": self.url}]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar commodities response.

        Extracts and flattens commodity price and performance data into a 
        standardized DataFrame.

        Parameters
        ----------
        response : dict
            API response containing commodities data.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes commodity identifiers, prices, changes, and categories.
            Returns empty DataFrame if response is invalid.
            
            Sorted by category (ascending).

        Notes
        -----
        The method flattens nested dataPoints structures, extracting values
        and their properties while excluding date and currency metadata.
        Each commodity includes its exchange information when available.
        """
        if not response or "page" not in response or "commodities" not in response["page"]:
            return pd.DataFrame()

        commodities = response["page"]["commodities"]
        rows = []

        for item in commodities:
            base_info = {
                "id": item.get("id"),
                "instrument": item.get("instrument"),
                "instrumentID": item.get("instrumentID"),
                "name": item.get("name"),
                "category": item.get("category"),
                "exchange": item.get("exchange"),
            }

            data_points = item.get("dataPoints", {})
            for key, dp in data_points.items():
                base_info[f"{key}_value"] = dp.get("value")

                props = dp.get("properties", {})
                for prop_key, prop_value in props.items():
                    if "date" not in prop_key.lower() and "currency" not in prop_key.lower():
                        base_info[f"{key}_{prop_key}"] = prop_value.get("value")

                if "exchange" in dp:
                    base_info[f"{key}_exchange"] = dp["exchange"].get("value")

            rows.append(base_info)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df = df.sort_values(by="category", ascending=True).reset_index(drop=True)
        
        return df


class MarketCurrenciesExtractor(BaseExtractor):
    """
    Extracts currency market data from Morningstar.

    Retrieves current exchange rates, bid/ask prices, and performance metrics 
    for various currency pairs.

    Attributes
    ----------
    url : str
        Base API URL for currency data.
    rename_columns : dict
        Mapping of API column names to standardized names.
    final_columns : list of str
        Final column order for the DataFrame.
    """
    
    config = MarketCurrenciesConfig
    schema = MarketCurrenciesSchema
    
    def __init__(self):
        """
        Initialize the MarketCurrenciesExtractor.

        Notes
        -----
        This extractor requires no parameters and retrieves all available 
        currency pair data from Morningstar's currencies endpoint.
        """
        client = BaseClient(
            auth_type=self.config.REQUIRED_AUTH,
            url=self.config.PAGE_URL,
        )
        super().__init__(client)

        self.url = self.config.API_URL
        self.rename_columns = self.config.RENAME_COLUMNS
        self.final_columns = self.config.FINAL_COLUMNS
        
    def _check_inputs(self) -> None:
        """
        Validate user inputs.

        No validation needed for this extractor as it accepts no parameters.

        Returns
        -------
        None
        """
        pass

    def _build_request(self) -> None:
        """
        Build API request dictionary.

        This endpoint requires no parameters beyond the URL.

        Returns
        -------
        None
        """
        self.requests = [{"url": self.url}]

    def _process_response(self, response: dict) -> pd.DataFrame:
        """
        Process Morningstar currencies response.

        Extracts and flattens currency exchange rate and performance data into 
        a standardized DataFrame.

        Parameters
        ----------
        response : dict
            API response containing currencies data.

        Returns
        -------
        pd.DataFrame
            Processed DataFrame with columns defined in FINAL_COLUMNS config.
            Includes currency pair identifiers, exchange rates, bid prices,
            and performance metrics. Returns empty DataFrame if response is invalid.
            
            Sorted by category (ascending).

        Notes
        -----
        The method flattens nested dataPoints structures, extracting values
        and their properties while excluding date and currency metadata.
        Exchange information is included for each currency pair when available.
        """
        if not response or "page" not in response or "currencies" not in response["page"]:
            return pd.DataFrame()

        currencies = response["page"]["currencies"]
        rows = []

        for item in currencies:
            base_info = {
                "id": item.get("id"),
                "instrumentID": item.get("instrumentID"),
                "label": item.get("label"),
                "name": item.get("name"),
                "category": item.get("category"),
                "bidPriceDecimals": item.get("bidPriceDecimals"),
            }

            data_points = item.get("dataPoints", {})
            for key, dp in data_points.items():
                base_info[f"{key}_value"] = dp.get("value")

                props = dp.get("properties", {})
                for prop_key, prop_value in props.items():
                    if "date" not in prop_key.lower() and "currency" not in prop_key.lower():
                        base_info[f"{key}_{prop_key}"] = prop_value.get("value")

                if "exchange" in dp:
                    base_info[f"{key}_exchange"] = dp["exchange"].get("value")

            rows.append(base_info)

        if not rows:
            return pd.DataFrame()

        df = pd.DataFrame(rows)
        df.rename(columns=self.rename_columns, inplace=True)
        df = df[self.final_columns]
        df = df.sort_values(by="category", ascending=True).reset_index(drop=True)
        
        return df