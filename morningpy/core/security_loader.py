import pandas as pd
import warnings
from typing import List, Union, Dict,Optional
from pathlib import Path

from morningpy.core.config import CoreConfig


class SecurityLoader:
    """
    Resolve various security identifiers to standardized Morningstar security_id values.
    
    Loads a local mapping file containing correspondences between different identifier
    types (ticker, ISIN, performance_id) and Morningstar internal security_id values.
    Provides validation, lookup, and metadata extraction functionality.
    
    Typical workflow:
        1. Initialize with one or more identifier types
        2. Call get() with desired fields to retrieve security_id values
        3. Access id_security_map for security metadata and fields for the list
    
    Attributes
    ----------
    ticker : List[str]
        List of ticker symbols to convert
    isin : List[str]
        List of ISIN codes to convert
    security_id : List[str]
        List of Morningstar security_id values to validate
    performance_id : List[str]
        List of Morningstar performance IDs to convert
    tickers : pd.DataFrame
        DataFrame containing all identifier correspondences loaded from file
    id_security_map : Dict[str, Dict[str, str]]
        Mapping of security_id to field dictionaries (populated by get())
    fields : List[str]
        List of resolved security_id values (populated by get())
    
    Examples
    --------
    >>> lookup = SecurityLookup(ticker=["AAPL", "MSFT"])
    >>> lookup.get(fields=["security_label", "ticker"])
    >>> print(lookup.fields)
    ['0P000000GY', '0P000003MH']
    >>> print(lookup.id_security_map)
    {
        '0P000000GY': {'security_label': 'Apple Inc', 'ticker': 'AAPL'},
        '0P000003MH': {'security_label': 'Microsoft Corp', 'ticker': 'MSFT'}
    }
    """

    def __init__(
        self,
        ticker: Union[str, List[str], None] = None,
        isin: Union[str, List[str], None] = None,
        security_id: Union[str, List[str], None] = None,
        performance_id: Union[str, List[str], None] = None,
    ):
        """
        Initialize the security lookup with one or more identifier types.
        
        Parameters
        ----------
        ticker : str, List[str], or None, optional
            Ticker symbol(s) to convert (e.g., 'AAPL' or ['AAPL', 'MSFT'])
        isin : str, List[str], or None, optional
            ISIN code(s) to convert (e.g., 'US0378331005')
        security_id : str, List[str], or None, optional
            Morningstar security_id value(s) to validate (10-character alphanumeric)
        performance_id : str, List[str], or None, optional
            Morningstar performance ID(s) to convert
        
        Notes
        -----
        - At least one identifier type should be provided for meaningful results
        - All inputs are normalized to lists internally
        - Mapping file is loaded automatically during initialization
        """
        self.tickers_file = CoreConfig.TICKERS_FILE
        self.ticker = self._normalize_input(ticker)
        self.isin = self._normalize_input(isin)
        self.security_id = self._normalize_input(security_id)
        self.performance_id = self._normalize_input(performance_id)
        self._load_tickers()

    def _load_tickers(self) -> None:
        """
        Load the security identifier mapping file from package data directory.
        
        Notes
        -----
        - Creates the data directory if it doesn't exist
        - Loads parquet file containing identifier mappings
        - File location is determined by CoreConfig.TICKERS_FILE
        
        Raises
        ------
        FileNotFoundError
            If the tickers file doesn't exist
        """
        package_dir = Path(__file__).resolve().parent.parent
        self.data_dir = package_dir / "data"
        self.path_file = self.data_dir / self.tickers_file
        if not self.path_file.exists():
            raise FileNotFoundError(
                f"Tickers mapping file not found: {self.path_file}. "
                "Ensure the package was installed correctly (include package data)."
            )
        self.tickers = pd.read_parquet(self.path_file)
    
    @staticmethod
    def _normalize_input(value: Union[str, List[str], None]) -> List[str]:
        """
        Normalize any input value into a list format.
        
        Parameters
        ----------
        value : str, List[str], or None
            A single identifier, a list of identifiers, or None
        
        Returns
        -------
        List[str]
            Normalized list of identifiers (empty list if input was None)
        
        Examples
        --------
        >>> SecurityLookup._normalize_input("AAPL")
        ['AAPL']
        >>> SecurityLookup._normalize_input(["AAPL", "MSFT"])
        ['AAPL', 'MSFT']
        >>> SecurityLookup._normalize_input(None)
        []
        """
        if not value:
            return []
        return [value] if isinstance(value, str) else list(value)

    def _lookup_ids(self, values: List[str], column: str) -> List[str]:
        """
        Look up security_id values by matching against a specific column.
        
        Parameters
        ----------
        values : List[str]
            List of identifier values to search for
        column : str
            Column name in the mapping DataFrame to search
            (e.g., 'ticker', 'isin', 'performance_id')
        
        Returns
        -------
        List[str]
            List of unique security_id values matching the input identifiers
        
        Warnings
        --------
        - Issues warning if multiple security_ids match a single identifier
        - Issues warning if some identifiers have no matches in the mapping
        
        Notes
        -----
        All matching security_ids are included, even when duplicates exist
        """
        if not values:
            return []

        matches = self.tickers[self.tickers[column].isin(values)]

        duplicates = (
            matches.groupby(column)["security_id"]
            .nunique()
            .loc[lambda x: x > 1]
            .index.tolist()
        )
        if duplicates:
            warnings.warn(
                f"Multiple IDs found for {column}(s): {duplicates}. "
                f"All matching IDs will be included."
            )

        found_ids = matches["security_id"].dropna().unique().tolist()

        missing = set(values) - set(matches[column].unique())
        if missing:
            warnings.warn(f"No match found in column '{column}' for: {sorted(missing)}")

        return found_ids

    def _validate_ids(self, ids: List[str]) -> List[str]:
        """
        Validate security_id format and check presence in mapping.
        
        Parameters
        ----------
        ids : List[str]
            List of security_id values to validate
        
        Returns
        -------
        List[str]
            List of IDs with valid format (10-character strings)
        
        Warnings
        --------
        - Issues warning for IDs with invalid format (not 10 characters)
        - Issues warning for valid-format IDs not found in mapping
        
        Notes
        -----
        - Valid Morningstar security_id values are 10-character alphanumeric strings
        - IDs not found in mapping are still returned but flagged with warning
        - Non-string values are filtered out
        """
        if not ids:
            return []

        valid_format = [i for i in ids if isinstance(i, str) and len(i) == 10]
        invalid_format = set(ids) - set(valid_format)
        if invalid_format:
            warnings.warn(f"Invalid ID format detected: {sorted(invalid_format)}")

        valid_in_mapping = self.tickers[
            self.tickers["security_id"].isin(valid_format)
        ]["security_id"].unique().tolist()

        missing = set(valid_format) - set(valid_in_mapping)
        if missing:
            warnings.warn(
                f"The following IDs are not found in the mapping: {sorted(missing)}. "
                f"They will still be returned."
            )

        return valid_format
    
    def _get_fields(
        self, 
        security_ids: List[str], 
        field_names: Optional[List[str]] = None
    ) -> List[Dict[str, str]]:
        """
        Populate id_security_map with requested fields for given security IDs.
        
        Parameters
        ----------
        security_ids : list of str
            List of security IDs to retrieve fields for.
        field_names : list of str, optional
            List of field names to extract from the mapping
            (e.g., ['security_label', 'ticker', 'isin']).
            If None, returns all available columns except 'security_id'.
        
        Returns
        -------
        list of dict
            List of dictionaries where each dictionary contains the requested
            fields for a security. Keys are field names, values are field values.
            Missing or invalid fields are set to empty strings.
        
        Raises
        ------
        ValueError
            If security_ids list is empty.
        
        Warnings
        --------
        Issues warning if requested fields are not available in the mapping.
        
        Notes
        -----
        - Returns a list of field dictionaries rather than updating instance state
        - For IDs not found in mapping, they are excluded from results
        - Each security_id maps to a dictionary of {field_name: field_value}
        - 'security_id' is automatically included in column selection
        - Missing fields are set to empty strings with a warning
        - NaN/None values in the DataFrame are filled with empty strings
        
        Examples
        --------
        >>> # With specific field names
        >>> fields = self._get_fields(
        ...     security_ids=['0P000000GY', '0P00000B3T'],
        ...     field_names=['security_label', 'ticker']
        ... )
        >>> fields
        [
            {'security_label': 'Apple Inc', 'ticker': 'AAPL'},
            {'security_label': 'Microsoft Corp', 'ticker': 'MSFT'}
        ]
        
        >>> # With field_names=None (returns all columns)
        >>> fields = self._get_fields(
        ...     security_ids=['0P000000GY'],
        ...     field_names=None
        ... )
        >>> fields[0].keys()
        dict_keys(['security_id', 'security_label', 'ticker', 'isin', 'performance_id', ...])
        
        >>> # With invalid field names
        >>> fields = self._get_fields(
        ...     security_ids=['0P000000GY'],
        ...     field_names=['security_label', 'invalid_field']
        ... )
        UserWarning: Fields not available in mapping: ['invalid_field']. Will be set to empty strings.
        """
        if not security_ids:
            raise ValueError("security_ids list is empty")
        
        # Handle None field_names - return all columns except security_id
        if field_names is None:
            field_names = [col for col in self.tickers.columns if col != "security_id"]
        
        # Ensure field_names is a list (handle edge cases)
        if not isinstance(field_names, list):
            field_names = [field_names]
        
        # Build columns to select, ensuring security_id is included for filtering
        columns_to_select = ["security_id"] + [f for f in field_names if f != "security_id"]
        
        # Validate columns exist in the DataFrame
        available_columns = set(self.tickers.columns)
        invalid_fields = [f for f in columns_to_select if f not in available_columns]
        
        if invalid_fields:
            warnings.warn(
                f"Fields not available in mapping: {invalid_fields}. "
                f"Will be set to empty strings."
            )
        
        # Filter to only valid columns
        valid_columns = [c for c in columns_to_select if c in available_columns]
        
        # Get matching rows
        matches = self.tickers[
            self.tickers["security_id"].isin(security_ids)
        ][valid_columns].drop_duplicates()
        
        # For invalid fields that were requested, add them as empty strings
        for invalid_field in invalid_fields:
            if invalid_field in field_names:  # Only add if it was in original request
                matches[invalid_field] = ""
        
        # Ensure we select only the requested field_names in the output
        # (excluding security_id unless explicitly requested)
        output_columns = [f for f in field_names if f in matches.columns]
        
        # Convert to list of dictionaries with NaN/None filled as empty strings
        field_dicts = matches[output_columns].fillna("").to_dict('records')
        
        return field_dicts


    def get(self, fields: CoreConfig.FieldsLiteral=None) -> None:
        """
        Convert all provided identifiers to Morningstar security_ids and populate results.
        
        This is the main public method that orchestrates the entire conversion pipeline:
        validates inputs, looks up identifiers, extracts metadata, and populates both
        self.fields (list of security_ids) and self.id_security_map (metadata dict).
        
        Parameters
        ----------
        fields : List[str]
            List of field names to extract and populate in id_security_map
            Available fields depend on the mapping file, typically include:
            - 'security_label': Security name
            - 'ticker': Stock ticker symbol
            - 'isin': ISIN code
            - 'exchange': Exchange code
            - 'currency': Trading currency
        
        Returns
        -------
        None
            Results are stored in self.fields and self.id_security_map
        
        Notes
        -----
        Conversion and validation pipeline:
        1. Validate provided security_id values (format and existence check)
        2. Look up matches for performance_id identifiers
        3. Look up matches for ISIN identifiers
        4. Look up matches for ticker identifiers
        5. Deduplicate and sort all collected IDs
        6. Store sorted IDs in self.fields
        7. Extract requested fields and populate id_security_map
        
        Side Effects
        ------------
        - Populates self.fields with sorted list of security_id values
        - Populates self.id_security_map with requested field data
        
        Examples
        --------
        >>> lookup = SecurityLookup(ticker=["AAPL", "MSFT"])
        >>> lookup.get(fields=["security_label", "ticker"])
        >>> print(lookup.fields)
        ['0P000000GY', '0P000003MH']
        >>> print(lookup.id_security_map)
        {
            '0P000000GY': {'security_label': 'Apple Inc', 'ticker': 'AAPL'},
            '0P000003MH': {'security_label': 'Microsoft Corp', 'ticker': 'MSFT'}
        }
        
        Using multiple identifier types:
        >>> lookup = SecurityLookup(
        ...     ticker=["AAPL"],
        ...     isin=["US5949181045"]  # MSFT ISIN
        ... )
        >>> lookup.get(fields=["security_label"])
        >>> len(lookup.fields)
        2
        """
        ids = set()

        ids.update(self._validate_ids(self.security_id))
        ids.update(self._lookup_ids(self.performance_id, "performance_id"))
        ids.update(self._lookup_ids(self.isin, "isin"))
        ids.update(self._lookup_ids(self.ticker, "ticker"))

        return self._get_fields(ids, fields)
 