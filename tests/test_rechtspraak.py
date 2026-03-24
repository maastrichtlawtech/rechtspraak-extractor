"""
Test suite for rechtspraak_extractor.

Tests integration between:
- get_rechtspraak: API-based ECLI download with pagination
- get_rechtspraak_metadata: Metadata extraction from ECLI documents
"""
import pandas as pd
import pytest
from pathlib import Path

from rechtspraak_extractor.rechtspraak import get_rechtspraak
from rechtspraak_extractor.rechtspraak_metadata import get_rechtspraak_metadata


@pytest.fixture(scope="session", autouse=True)
def ensure_data_directories():
    """Create required data directories before tests run."""
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data").mkdir(parents=True, exist_ok=True)
    yield


# ============================================================================
# API DOWNLOAD TESTS
# ============================================================================
def test_download_rechtspraak_returns_dataframe():
    """Verify get_rechtspraak returns DataFrame when save_file='n'."""
    df = get_rechtspraak(max_ecli=50, sd="2025-04-01", save_file="n")

    assert df is not None, "Should return DataFrame"
    assert isinstance(df, pd.DataFrame), "Result should be pandas DataFrame"
    assert len(df) > 0, "DataFrame should contain data"
    assert all(
        col in df.columns for col in ["id", "title", "link"]
    ), "Should have required columns"


def test_download_rechtspraak_saves_to_file():
    """Verify get_rechtspraak creates CSV file when save_file='y'."""
    result = get_rechtspraak(max_ecli=25, sd="2025-04-01", save_file="y")

    assert result is None, "Should return None when saving to file"

    csv_files = list(Path("data").glob("rechtspraak_*.csv"))
    assert len(csv_files) > 0, "CSV file should be created"


# ============================================================================
# METADATA EXTRACTION TESTS
# ============================================================================
def test_metadata_extraction_from_api_data():
    """Test metadata extraction from downloaded API data."""
    # Download data from API
    df = get_rechtspraak(max_ecli=25, sd="2025-04-01", save_file="n")

    # Extract metadata from DataFrame
    metadata = get_rechtspraak_metadata(save_file="n", dataframe=df, _fake_headers=True)

    assert metadata is not None, "Should return metadata DataFrame"
    assert isinstance(metadata, pd.DataFrame), "Metadata should be DataFrame"
    assert all(
        col in metadata.columns for col in ["ecli", "full_text", "creator"]
    ), "Should have key metadata columns"


def test_metadata_extraction_and_save():
    """Test metadata extraction with file saving."""
    df = get_rechtspraak(max_ecli=25, sd="2025-04-01", save_file="n")

    result = get_rechtspraak_metadata(save_file="y", dataframe=df, _fake_headers=True)

    assert result is True, "Should return True when saving metadata"


def test_metadata_invalid_dataframe():
    """Test metadata extraction handles invalid input."""
    invalid_df = pd.DataFrame({"wrong_col": [1, 2, 3]})

    result = get_rechtspraak_metadata(save_file="n", dataframe=invalid_df)
    assert result is False, "Should return False for invalid DataFrame"


def test_metadata_empty_dataframe():
    """Test metadata extraction handles empty input."""
    empty_df = pd.DataFrame()

    result = get_rechtspraak_metadata(save_file="n", dataframe=empty_df)
    assert result is False, "Should return False for empty DataFrame"


# ============================================================================
# INTEGRATED WORKFLOW TESTS
# ============================================================================
def test_metadata_workflow_integration():
    """Test metadata extraction is properly integrated with downloads."""
    # This validates the workflow: when data is downloaded, metadata can be extracted
    df = get_rechtspraak(max_ecli=30, sd="2025-04-01", save_file="n")

    # Even if download fails, the metadata function should handle it gracefully
    if df is not None and len(df) > 0:
        metadata = get_rechtspraak_metadata(
            save_file="n", dataframe=df, _fake_headers=True
        )
        assert isinstance(metadata, pd.DataFrame)
        assert len(metadata) > 0


# ============================================================================
# BACKWARD COMPATIBILITY TESTS
# ============================================================================
def rechtspraak_n():
    """Helper: download without saving and extract metadata."""
    df = get_rechtspraak(max_ecli=100, sd="2025-04-01", save_file="n")
    get_rechtspraak_metadata(save_file="n", dataframe=df, _fake_headers=True)


def rechtspraak_y():
    """Helper: download with saving and extract metadata."""
    df = get_rechtspraak(max_ecli=100, sd="2025-04-01", save_file="y")
    get_rechtspraak_metadata(save_file="y", dataframe=df, _fake_headers=True)


def test_rechtspraak_y():
    """Legacy test: API download and metadata extraction with saving."""
    try:
        rechtspraak_y()
        assert True
    except Exception as e:
        assert False, f"Workflow with save failed: {e}"


def test_rechtspraak_n():
    """Legacy test: API download and metadata extraction without saving."""
    try:
        rechtspraak_n()
        assert True
    except Exception as e:
        assert False, f"Workflow without save failed: {e}"
