# tests/conftest.py

import inspect
from dataclasses import is_dataclass
from pathlib import Path
import json

import pytest
import pandas as pd

import morningpy.schema as schema_module
from morningpy.core.dataframe_schema import DataFrameSchema
from tests.mocks.schema_mocks import SCHEMA_MOCKS


# --------------------------
# Schema discovery fixtures
# --------------------------
def get_all_schema_classes():
    """Auto-discover all dataclass schemas that subclass DataFrameSchema."""
    return sorted(
        [
            obj
            for _, obj in inspect.getmembers(schema_module)
            if inspect.isclass(obj)
            and is_dataclass(obj)
            and issubclass(obj, DataFrameSchema)
            and obj is not DataFrameSchema
        ],
        key=lambda c: c.__name__,
    )


@pytest.fixture(scope="session")
def schema_classes():
    """Returns all discovered schema classes."""
    return get_all_schema_classes()


@pytest.fixture(scope="session")
def schema_mocks():
    """Returns the dict mapping SchemaClass → mock instance."""
    return SCHEMA_MOCKS


# --------------------------
# API scenario fixtures
# --------------------------
BASE_DIR = Path(__file__).parent
REQUESTS_DIR = BASE_DIR / "fixtures/fake_requests"
RESPONSES_DIR = BASE_DIR / "fixtures/fake_responses"
MOCKS_DIR = BASE_DIR / "mocks"


def _load_json(filepath: Path):
    """Load JSON file as dict; return None if file does not exist."""
    if not filepath.exists():
        return None
    with open(filepath, "r") as f:
        return json.load(f)


@pytest.fixture
def scenario_files(request):
    """
    Load request and response JSON files and optional mock CSV based on the test parameter.
    Returns a dict:
    {
        "request": dict or None,
        "response": dict or None,
        "mock": DataFrame or None
    }
    """
    func_name = request.param
    request_data = _load_json(REQUESTS_DIR / f"{func_name}_request.json")
    response_data = _load_json(RESPONSES_DIR / f"{func_name}_response.json")
    mock_csv = MOCKS_DIR / f"{func_name}_mock.csv"
    mock_df = pd.read_csv(mock_csv) if mock_csv.exists() else None

    return {"request": request_data, "response": response_data, "mock": mock_df}


@pytest.fixture
def fake_request(scenario_files):
    """Return request data as dict (or empty dict if None)."""
    return scenario_files.get("request") or {}


@pytest.fixture
def fake_response(scenario_files):
    """Return response data as dict (or empty dict if None)."""
    return scenario_files.get("response") or {}


@pytest.fixture
def expected_output_df(scenario_files):
    """Return expected output DataFrame (mock) for assertions."""
    return scenario_files.get("mock") or pd.DataFrame()
