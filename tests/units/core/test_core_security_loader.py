import pytest
import pandas as pd
import warnings
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock
from morningpy.core.config import CoreConfig
from morningpy.core.security_loader import SecurityLoader


# ============================================================================
# FIXTURES
# ============================================================================

@pytest.fixture
def sample_tickers_df():
    """Create a sample tickers DataFrame for testing."""
    return pd.DataFrame({
        'security_id': ['0P000000GY', '0P000003MH', '0P00000B3T', '0P00000ABC', '0P00000XYZ'],
        'security_label': ['Apple Inc', 'Microsoft Corp', 'Amazon.com Inc', 'Tesla Inc', 'Google LLC'],
        'ticker': ['AAPL', 'MSFT', 'AMZN', 'TSLA', 'GOOGL'],
        'isin': ['US0378331005', 'US5949181045', 'US0231351067', 'US88160R1014', 'US02079K1079'],
        'performance_id': ['0P000000GY', '0P000003MH', '0P00000B3T', '0P00000ABC', '0P00000XYZ'],
        'exchange': ['XNAS', 'XNAS', 'XNAS', 'XNAS', 'XNAS'],
        'currency': ['USD', 'USD', 'USD', 'USD', 'USD']
    })


@pytest.fixture
def sample_tickers_with_duplicates():
    """Create a DataFrame with duplicate mappings for testing."""
    return pd.DataFrame({
        'security_id': ['0P000000GY', '0P000000GY', '0P000003MH'],
        'security_label': ['Apple Inc', 'Apple Inc', 'Microsoft Corp'],
        'ticker': ['AAPL', 'AAPL', 'MSFT'],
        'isin': ['US0378331005', 'US0378331005', 'US5949181045'],
        'performance_id': ['0P000000GY', '0P000000AA', '0P000003MH']
    })


@pytest.fixture
def mock_security_loader(sample_tickers_df):
    """Create a SecurityLoader instance with mocked file loading."""
    with patch.object(SecurityLoader, '_load_tickers'):
        loader = SecurityLoader()
        loader.tickers = sample_tickers_df
        return loader


# ============================================================================
# INITIALIZATION TESTS
# ============================================================================

class TestInitialization:
    """Test SecurityLoader initialization."""
    
    def test_init_with_single_ticker(self, mock_security_loader):
        """Test initialization with a single ticker string."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(ticker="AAPL")
            assert loader.ticker == ["AAPL"]
            assert loader.isin == []
            assert loader.security_id == []
            assert loader.performance_id == []
    
    def test_init_with_list_of_tickers(self, mock_security_loader):
        """Test initialization with a list of tickers."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(ticker=["AAPL", "MSFT"])
            assert loader.ticker == ["AAPL", "MSFT"]
    
    def test_init_with_multiple_identifier_types(self, mock_security_loader):
        """Test initialization with multiple identifier types."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(
                ticker=["AAPL"],
                isin="US0378331005",
                security_id=["0P000000GY"]
            )
            assert loader.ticker == ["AAPL"]
            assert loader.isin == ["US0378331005"]
            assert loader.security_id == ["0P000000GY"]
    
    def test_init_with_none_values(self, mock_security_loader):
        """Test initialization with None values."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(ticker=None, isin=None)
            assert loader.ticker == []
            assert loader.isin == []
    
    def test_init_calls_load_tickers(self):
        """Test that initialization calls _load_tickers."""
        with patch.object(SecurityLoader, '_load_tickers') as mock_load:
            SecurityLoader()
            mock_load.assert_called_once()


# ============================================================================
# NORMALIZE INPUT TESTS
# ============================================================================

class TestNormalizeInput:
    """Test the _normalize_input static method."""
    
    def test_normalize_string(self):
        """Test normalization of a single string."""
        result = SecurityLoader._normalize_input("AAPL")
        assert result == ["AAPL"]
    
    def test_normalize_list(self):
        """Test normalization of a list."""
        result = SecurityLoader._normalize_input(["AAPL", "MSFT"])
        assert result == ["AAPL", "MSFT"]
    
    def test_normalize_none(self):
        """Test normalization of None."""
        result = SecurityLoader._normalize_input(None)
        assert result == []
    
    def test_normalize_empty_string(self):
        """Test normalization of empty string."""
        result = SecurityLoader._normalize_input("")
        assert result == []
    
    def test_normalize_empty_list(self):
        """Test normalization of empty list."""
        result = SecurityLoader._normalize_input([])
        assert result == []


# ============================================================================
# LOAD TICKERS TESTS
# ============================================================================

class TestLoadTickers:
    """Test the _load_tickers method."""
    
    @patch('pandas.read_parquet')
    @patch('pathlib.Path.exists', return_value=True)
    def test_load_tickers_reads_parquet_file(self, mock_exists, mock_read_parquet, sample_tickers_df):
        """Test that _load_tickers reads the parquet file."""
        mock_read_parquet.return_value = sample_tickers_df

        loader = SecurityLoader()

        assert isinstance(loader.tickers, pd.DataFrame)
        assert len(loader.tickers) == 5

    @patch('pathlib.Path.exists', return_value=False)
    def test_load_tickers_file_not_found(self, mock_exists):
        """Test that a missing tickers file raises FileNotFoundError."""
        with pytest.raises(FileNotFoundError, match="Tickers mapping file not found"):
            SecurityLoader()


# ============================================================================
# LOOKUP IDS TESTS
# ============================================================================

class TestLookupIds:
    """Test the _lookup_ids method."""
    
    def test_lookup_valid_ticker(self, mock_security_loader):
        """Test looking up a valid ticker."""
        result = mock_security_loader._lookup_ids(["AAPL"], "ticker")
        assert result == ["0P000000GY"]
    
    def test_lookup_multiple_tickers(self, mock_security_loader):
        """Test looking up multiple tickers."""
        result = mock_security_loader._lookup_ids(["AAPL", "MSFT"], "ticker")
        assert set(result) == {"0P000000GY", "0P000003MH"}
    
    def test_lookup_invalid_ticker(self, mock_security_loader):
        """Test looking up an invalid ticker."""
        with pytest.warns(UserWarning, match="No match found"):
            result = mock_security_loader._lookup_ids(["INVALID"], "ticker")
            assert result == []
    
    def test_lookup_mixed_valid_invalid(self, mock_security_loader):
        """Test looking up mix of valid and invalid tickers."""
        with pytest.warns(UserWarning, match="No match found.*INVALID"):
            result = mock_security_loader._lookup_ids(["AAPL", "INVALID"], "ticker")
            assert "0P000000GY" in result
            assert len(result) == 1
    
    def test_lookup_empty_list(self, mock_security_loader):
        """Test looking up with empty list."""
        result = mock_security_loader._lookup_ids([], "ticker")
        assert result == []
    
    def test_lookup_by_isin(self, mock_security_loader):
        """Test looking up by ISIN."""
        result = mock_security_loader._lookup_ids(["US0378331005"], "isin")
        assert result == ["0P000000GY"]
    
    def test_lookup_by_performance_id(self, mock_security_loader):
        """Test looking up by performance_id."""
        result = mock_security_loader._lookup_ids(["0P000000GY"], "performance_id")
        assert result == ["0P000000GY"]
    
    def test_lookup_with_duplicates(self):
        """Test lookup warning when multiple IDs found for same identifier."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader()
            loader.tickers = pd.DataFrame({
                'security_id': ['ID1', 'ID2', 'ID3'],
                'ticker': ['AAPL', 'AAPL', 'MSFT'],
                'performance_id': ['P1', 'P2', 'P3']
            })
            
            with pytest.warns(UserWarning, match="Multiple IDs found"):
                result = loader._lookup_ids(["AAPL"], "ticker")
                assert set(result) == {"ID1", "ID2"}
    
    def test_lookup_returns_unique_ids(self, mock_security_loader):
        """Test that lookup returns unique security_ids."""
        # Add duplicate row
        mock_security_loader.tickers = pd.concat([
            mock_security_loader.tickers,
            mock_security_loader.tickers.iloc[[0]]
        ])
        
        result = mock_security_loader._lookup_ids(["AAPL"], "ticker")
        assert result == ["0P000000GY"]


# ============================================================================
# VALIDATE IDS TESTS
# ============================================================================

class TestValidateIds:
    """Test the _validate_ids method."""
    
    def test_validate_valid_id(self, mock_security_loader):
        """Test validation of a valid security_id."""
        result = mock_security_loader._validate_ids(["0P000000GY"])
        assert result == ["0P000000GY"]
    
    def test_validate_multiple_valid_ids(self, mock_security_loader):
        """Test validation of multiple valid IDs."""
        result = mock_security_loader._validate_ids(["0P000000GY", "0P000003MH"])
        assert set(result) == {"0P000000GY", "0P000003MH"}
    
    def test_validate_invalid_format_too_short(self, mock_security_loader):
        """Test validation of ID with invalid format (too short)."""
        with pytest.warns(UserWarning, match="Invalid ID format"):
            result = mock_security_loader._validate_ids(["SHORT"])
            assert result == []
    
    def test_validate_invalid_format_too_long(self, mock_security_loader):
        """Test validation of ID with invalid format (too long)."""
        with pytest.warns(UserWarning, match="Invalid ID format"):
            result = mock_security_loader._validate_ids(["TOOLONGID123"])
            assert result == []
    
    def test_validate_id_not_in_mapping(self, mock_security_loader):
        """Test validation of ID not found in mapping."""
        with pytest.warns(UserWarning, match="not found in the mapping"):
            result = mock_security_loader._validate_ids(["0P99999999"])
            assert result == ["0P99999999"]  # Still returned
    
    def test_validate_mixed_valid_invalid_format(self, mock_security_loader):
        """Test validation of mix of valid and invalid format IDs."""
        with pytest.warns(UserWarning, match="Invalid ID format"):
            result = mock_security_loader._validate_ids(["0P000000GY", "SHORT"])
            assert result == ["0P000000GY"]
    
    def test_validate_empty_list(self, mock_security_loader):
        """Test validation of empty list."""
        result = mock_security_loader._validate_ids([])
        assert result == []
    
    def test_validate_non_string_values(self, mock_security_loader):
        """Test validation filters out non-string values."""
        with pytest.warns(UserWarning):
            result = mock_security_loader._validate_ids([123, "0P000000GY"])
            assert result == ["0P000000GY"]


# ============================================================================
# GET FIELDS TESTS
# ============================================================================

class TestGetFields:
    """Test the _get_fields method."""
    
    def test_get_fields_single_field(self, mock_security_loader):
        """Test getting a single field."""
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY"],
            field_names=["security_label"]
        )
        assert len(result) == 1
        assert result[0] == {"security_label": "Apple Inc"}
    
    def test_get_fields_multiple_fields(self, mock_security_loader):
        """Test getting multiple fields."""
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY"],
            field_names=["security_label", "ticker", "isin"]
        )
        assert len(result) == 1
        assert result[0] == {
            "security_label": "Apple Inc",
            "ticker": "AAPL",
            "isin": "US0378331005"
        }
    
    def test_get_fields_multiple_securities(self, mock_security_loader):
        """Test getting fields for multiple securities."""
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY", "0P000003MH"],
            field_names=["security_label", "ticker"]
        )
        assert len(result) == 2
        assert result[0]["ticker"] in ["AAPL", "MSFT"]
        assert result[1]["ticker"] in ["AAPL", "MSFT"]
    
    def test_get_fields_with_none_returns_all_fields(self, mock_security_loader):
        """Test that field_names=None returns all columns except security_id."""
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY"],
            field_names=None
        )
        assert len(result) == 1
        expected_keys = {"security_label", "ticker", "isin", "performance_id", "exchange", "currency"}
        assert set(result[0].keys()) == expected_keys
    
    def test_get_fields_invalid_field_name(self, mock_security_loader):
        """Test warning when requesting invalid field names."""
        with pytest.warns(UserWarning, match="Fields not available"):
            result = mock_security_loader._get_fields(
                security_ids=["0P000000GY"],
                field_names=["security_label", "invalid_field"]
            )
            assert result[0]["security_label"] == "Apple Inc"
            assert result[0]["invalid_field"] == ""
    
    def test_get_fields_empty_security_ids(self, mock_security_loader):
        """Test that empty security_ids raises ValueError."""
        with pytest.raises(ValueError, match="security_ids list is empty"):
            mock_security_loader._get_fields(
                security_ids=[],
                field_names=["security_label"]
            )
    
    def test_get_fields_security_id_not_in_mapping(self, mock_security_loader):
        """Test getting fields for ID not in mapping."""
        result = mock_security_loader._get_fields(
            security_ids=["0P99999999"],
            field_names=["security_label"]
        )
        assert len(result) == 0
    
    def test_get_fields_handles_nan_values(self, mock_security_loader):
        """Test that NaN values are filled with empty strings."""
        # Add row with NaN
        mock_security_loader.tickers.loc[0, "ticker"] = None
        
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY"],
            field_names=["security_label", "ticker"]
        )
        assert result[0]["ticker"] == ""
    
    def test_get_fields_removes_duplicates(self, mock_security_loader):
        """Test that duplicate rows are removed."""
        # Duplicate a row
        mock_security_loader.tickers = pd.concat([
            mock_security_loader.tickers,
            mock_security_loader.tickers.iloc[[0]]
        ])
        
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY"],
            field_names=["security_label"]
        )
        assert len(result) == 1


# ============================================================================
# GET METHOD TESTS (INTEGRATION)
# ============================================================================

class TestGetMethod:
    """Test the main get() method."""
    
    def test_get_with_single_ticker(self, mock_security_loader):
        """Test get with a single ticker."""
        mock_security_loader.ticker = ["AAPL"]
        result = mock_security_loader.get(fields=["security_label", "ticker"])
        
        assert len(result) == 1
        assert result[0]["ticker"] == "AAPL"
        assert result[0]["security_label"] == "Apple Inc"
    
    def test_get_with_multiple_tickers(self, mock_security_loader):
        """Test get with multiple tickers."""
        mock_security_loader.ticker = ["AAPL", "MSFT"]
        result = mock_security_loader.get(fields=["security_label"])
        
        assert len(result) == 2
        labels = {r["security_label"] for r in result}
        assert "Apple Inc" in labels
        assert "Microsoft Corp" in labels
    
    def test_get_with_isin(self, mock_security_loader):
        """Test get with ISIN."""
        mock_security_loader.isin = ["US0378331005"]
        result = mock_security_loader.get(fields=["security_label"])
        
        assert len(result) == 1
        assert result[0]["security_label"] == "Apple Inc"
    
    def test_get_with_security_id(self, mock_security_loader):
        """Test get with security_id."""
        mock_security_loader.security_id = ["0P000000GY"]
        result = mock_security_loader.get(fields=["security_label"])
        
        assert len(result) == 1
        assert result[0]["security_label"] == "Apple Inc"
    
    def test_get_with_performance_id(self, mock_security_loader):
        """Test get with performance_id."""
        mock_security_loader.performance_id = ["0P000000GY"]
        result = mock_security_loader.get(fields=["security_label"])
        
        assert len(result) == 1
        assert result[0]["security_label"] == "Apple Inc"
    
    def test_get_with_multiple_identifier_types(self, mock_security_loader):
        """Test get with multiple identifier types."""
        mock_security_loader.ticker = ["AAPL"]
        mock_security_loader.isin = ["US5949181045"]  # MSFT
        result = mock_security_loader.get(fields=["security_label"])
        
        assert len(result) == 2
        labels = {r["security_label"] for r in result}
        assert labels == {"Apple Inc", "Microsoft Corp"}
    
    def test_get_with_no_fields_specified(self, mock_security_loader):
        """Test get with fields=None returns all fields."""
        mock_security_loader.ticker = ["AAPL"]
        result = mock_security_loader.get(fields=None)
        
        assert len(result) == 1
        assert "security_label" in result[0]
        assert "ticker" in result[0]
        assert "isin" in result[0]
    
    def test_get_deduplicates_ids(self, mock_security_loader):
        """Test that get deduplicates security IDs from different sources."""
        # Same security via different identifiers
        mock_security_loader.ticker = ["AAPL"]
        mock_security_loader.isin = ["US0378331005"]  # Also AAPL
        mock_security_loader.security_id = ["0P000000GY"]  # Also AAPL
        
        result = mock_security_loader.get(fields=["security_label"])
        
        # Should only return one result
        assert len(result) == 1
        assert result[0]["security_label"] == "Apple Inc"
    
    def test_get_with_no_identifiers(self, mock_security_loader):
        """Test get with no identifiers provided."""
        with pytest.raises(ValueError, match="security_ids list is empty"):
            mock_security_loader.get(fields=["security_label"])
    
    def test_get_with_invalid_identifiers(self, mock_security_loader):
        """Test get with invalid identifiers."""
        mock_security_loader.ticker = ["INVALID"]
        
        with pytest.warns(UserWarning, match="No match found"):
            with pytest.raises(ValueError, match="security_ids list is empty"):
                mock_security_loader.get(fields=["security_label"])


# ============================================================================
# EDGE CASES AND ERROR HANDLING
# ============================================================================

class TestEdgeCases:
    """Test edge cases and error handling."""
    
    def test_special_characters_in_ticker(self, mock_security_loader):
        """Test handling of special characters in ticker."""
        with pytest.warns(UserWarning):
            result = mock_security_loader._lookup_ids(["AA$PL"], "ticker")
            assert result == []
    
    def test_case_sensitivity(self, mock_security_loader):
        """Test that lookups are case-sensitive."""
        with pytest.warns(UserWarning):
            result = mock_security_loader._lookup_ids(["aapl"], "ticker")
            assert result == []
    
    def test_whitespace_in_identifiers(self, mock_security_loader):
        """Test handling of whitespace in identifiers."""
        with pytest.warns(UserWarning):
            result = mock_security_loader._lookup_ids([" AAPL "], "ticker")
            assert result == []
    
    def test_empty_dataframe(self):
        """Test behavior with empty tickers DataFrame."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader()
            loader.tickers = pd.DataFrame(columns=['security_id', 'ticker'])
            loader.ticker = ["AAPL"]
            
            with pytest.warns(UserWarning):
                with pytest.raises(ValueError):
                    loader.get(fields=["security_label"])
    
    def test_large_batch_lookup(self, mock_security_loader):
        """Test lookup with large batch of identifiers."""
        # Create 100 identifiers
        tickers = [f"TICK{i:02d}" for i in range(100)]
        
        with pytest.warns(UserWarning):  # Will warn about not found
            result = mock_security_loader._lookup_ids(tickers, "ticker")
            # Should only find the ones that exist
            assert all(id in mock_security_loader.tickers["security_id"].values 
                      for id in result)


# ============================================================================
# INTEGRATION TESTS
# ============================================================================

class TestIntegration:
    """Integration tests simulating real usage patterns."""
    
    def test_complete_workflow_single_ticker(self, sample_tickers_df):
        """Test complete workflow with single ticker."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(ticker="AAPL")
            loader.tickers = sample_tickers_df
            
            result = loader.get(fields=["security_label", "ticker", "isin"])
            
            assert len(result) == 1
            assert result[0]["ticker"] == "AAPL"
            assert result[0]["security_label"] == "Apple Inc"
            assert result[0]["isin"] == "US0378331005"
    
    def test_complete_workflow_multiple_types(self, sample_tickers_df):
        """Test complete workflow with multiple identifier types."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(
                ticker=["AAPL", "MSFT"],
                isin=["US0231351067"],  # AMZN
                security_id=["0P00000ABC"]  # TSLA
            )
            loader.tickers = sample_tickers_df
            
            result = loader.get(fields=["security_label"])
            
            assert len(result) == 4
            labels = {r["security_label"] for r in result}
            expected = {"Apple Inc", "Microsoft Corp", "Amazon.com Inc", "Tesla Inc"}
            assert labels == expected
    
    def test_error_recovery_partial_success(self, sample_tickers_df):
        """Test that valid IDs are processed even when some are invalid."""
        with patch.object(SecurityLoader, '_load_tickers'):
            loader = SecurityLoader(ticker=["AAPL", "INVALID", "MSFT"])
            loader.tickers = sample_tickers_df
            
            with pytest.warns(UserWarning, match="No match found"):
                result = loader.get(fields=["security_label"])
                
                assert len(result) == 2
                labels = {r["security_label"] for r in result}
                assert labels == {"Apple Inc", "Microsoft Corp"}


# ============================================================================
# PARAMETRIZED TESTS
# ============================================================================

class TestParametrized:
    """Parametrized tests for comprehensive coverage."""
    
    @pytest.mark.parametrize("identifier,column,expected", [
        ("AAPL", "ticker", "0P000000GY"),
        ("US0378331005", "isin", "0P000000GY"),
        ("0P000000GY", "performance_id", "0P000000GY"),
    ])
    def test_lookup_various_identifiers(self, mock_security_loader, identifier, column, expected):
        """Test lookup with various identifier types."""
        result = mock_security_loader._lookup_ids([identifier], column)
        assert expected in result
    
    @pytest.mark.parametrize("invalid_id", [
        "SHORT",
        "TOOLONGID123",
        "123",
        "",
        "0P00000",
    ])
    def test_validate_various_invalid_formats(self, mock_security_loader, invalid_id):
        """Test validation with various invalid formats."""
        with pytest.warns(UserWarning):
            result = mock_security_loader._validate_ids([invalid_id])
            assert result == []
    
    @pytest.mark.parametrize("field_names,expected_keys", [
        (["security_label"], {"security_label"}),
        (["security_label", "ticker"], {"security_label", "ticker"}),
        (["security_label", "ticker", "isin"], {"security_label", "ticker", "isin"}),
    ])
    def test_get_fields_various_combinations(self, mock_security_loader, field_names, expected_keys):
        """Test getting various field combinations."""
        result = mock_security_loader._get_fields(
            security_ids=["0P000000GY"],
            field_names=field_names
        )
        assert set(result[0].keys()) == expected_keys
