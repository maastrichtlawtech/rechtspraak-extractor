"""
Integration test suite for rechtspraak_extractor.

Tests the full workflow against real APIs and the local SQLite database.
Run with: pytest -m integration
Skip for CI without network: pytest -m "not integration"
"""
import glob
from pathlib import Path
import sqlite3

import pandas as pd
import pytest

from rechtspraak_extractor.rechtspraak import get_rechtspraak
from rechtspraak_extractor.rechtspraak_metadata import (
    fetch_eclis_via_sqlite,
    get_rechtspraak_metadata,
)


# ============================================================================
# API DOWNLOAD TESTS
# ============================================================================
@pytest.mark.integration
def test_download_rechtspraak_returns_dataframe():
    """Verify get_rechtspraak returns DataFrame when save_file='n'."""
    df = get_rechtspraak(max_ecli=50, sd="2025-04-01", save_file="n")

    assert df is not None, "Should return DataFrame"
    assert isinstance(df, pd.DataFrame), "Result should be pandas DataFrame"
    assert len(df) > 0, "DataFrame should contain data"
    assert all(col in df.columns for col in ["id", "title", "link"]), "Should have required columns"


@pytest.mark.integration
def test_download_rechtspraak_saves_to_file():
    """Verify get_rechtspraak creates CSV file when save_file='y'."""
    result = get_rechtspraak(max_ecli=25, sd="2025-04-01", save_file="y")

    assert result is None, "Should return None when saving to file"
    csv_files = list(Path("data").glob("rechtspraak_*.csv"))
    assert len(csv_files) > 0, "CSV file should be created"


# ============================================================================
# METADATA EXTRACTION TESTS
# ============================================================================
@pytest.mark.integration
def test_metadata_extraction_from_api_data():
    """Test metadata extraction from downloaded API data."""
    df = get_rechtspraak(max_ecli=25, sd="2025-04-01", save_file="n")
    metadata = get_rechtspraak_metadata(save_file="n", dataframe=df, _fake_headers=False)

    assert metadata is not None, "Should return metadata DataFrame"
    assert isinstance(metadata, pd.DataFrame), "Metadata should be DataFrame"
    assert all(col in metadata.columns for col in ["ecli", "full_text", "creator"]), \
        "Should have key metadata columns"

    if len(metadata) > 0:
        output_file = Path("data/test_metadata_extraction_from_api_data.csv")
        metadata.to_csv(output_file, index=False)
        assert output_file.exists()


@pytest.mark.integration
def test_metadata_extraction_and_save():
    """Test metadata extraction with file saving."""
    df = get_rechtspraak(max_ecli=25, sd="2025-04-01", save_file="n")
    result = get_rechtspraak_metadata(save_file="y", dataframe=df, _fake_headers=False)

    assert result is True, "Should return True when saving metadata"


@pytest.mark.integration
def test_metadata_invalid_dataframe():
    """Test metadata extraction handles invalid input."""
    invalid_df = pd.DataFrame({"wrong_col": [1, 2, 3]})
    result = get_rechtspraak_metadata(save_file="n", dataframe=invalid_df)
    assert result is None, "Should return None for invalid DataFrame"


@pytest.mark.integration
def test_metadata_empty_dataframe():
    """Test metadata extraction handles empty input."""
    empty_df = pd.DataFrame()
    result = get_rechtspraak_metadata(save_file="n", dataframe=empty_df)
    assert result is None, "Should return None for empty DataFrame"


# ============================================================================
# SQLITE DATABASE TESTS
# ============================================================================
@pytest.mark.integration
def test_sqlite_database_exists():
    """Verify SQLite database is available for testing."""
    db_path = "data/lido.db"
    assert Path(db_path).exists(), f"SQLite database not found at {db_path}"

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table' AND name='metadata'")
    result = cursor.fetchone()
    conn.close()
    assert result is not None, "Metadata table not found in SQLite database"


@pytest.mark.integration
def test_fetch_eclis_via_sqlite():
    """Test fetching metadata from SQLite database for known ECLIs."""
    test_eclis = ["ECLI:NL:RBDHA:2025:5366", "ECLI:NL:RBDHA:2025:5384"]
    expected_columns = ["ecli", "document_type", "date_decision", "title"]

    metadata_df = fetch_eclis_via_sqlite(
        ecli_list=test_eclis,
        sqlite_db_path="data/lido.db",
        columns=expected_columns,
    )

    assert isinstance(metadata_df, pd.DataFrame)
    assert len(metadata_df) > 0
    retrieved_eclis = metadata_df["ecli"].tolist()
    for ecli in test_eclis:
        assert ecli in retrieved_eclis, f"Expected ECLI {ecli} not in results"


@pytest.mark.integration
@pytest.mark.parametrize("columns", [
    pytest.param(
        ["ecli", "document_type", "date_decision", "title"],
        id="minimal",
    ),
    pytest.param(
        [
            "ecli", "full_text", "creator", "date_decision", "issued", "zaaknummer",
            "type", "relations", "references", "subject", "procedure", "inhoudsindicatie",
            "hasVersion", "document_type", "date_publication", "language", "instance",
            "jurisdiction_city", "case_number", "procedure_type", "domains",
            "referenced_legislation_titles", "alternative_publications", "title",
            "summary", "citing", "cited_by", "legislations_cited",
            "predecessor_successor_cases", "url_publications", "info", "source",
        ],
        id="full",
    ),
])
def test_fetch_eclis_via_sqlite_column_sets(columns):
    """Parametrized: fetch one known ECLI with minimal and full column sets."""
    metadata_df = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:RBDHA:2025:5366"],
        sqlite_db_path="data/lido.db",
        columns=columns,
    )

    assert isinstance(metadata_df, pd.DataFrame)
    if len(metadata_df) > 0:
        assert "ecli" in metadata_df.columns
        assert "document_type" in metadata_df.columns


@pytest.mark.integration
def test_fetch_eclis_via_sqlite_nonexistent():
    """Test fetching non-existent ECLIs returns empty DataFrame."""
    expected_columns = ["ecli", "document_type", "date_decision"]
    metadata_df = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:INVALID:9999:0000"],
        sqlite_db_path="data/lido.db",
        columns=expected_columns,
    )

    assert isinstance(metadata_df, pd.DataFrame)
    assert len(metadata_df) == 0
    assert list(metadata_df.columns) == expected_columns


@pytest.mark.integration
def test_fetch_eclis_via_sqlite_nonexistent_db():
    """Test handling of non-existent SQLite database."""
    expected_columns = ["ecli", "document_type"]
    metadata_df = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:RBDHA:2025:5366"],
        sqlite_db_path="data/nonexistent_db.db",
        columns=expected_columns,
    )

    assert isinstance(metadata_df, pd.DataFrame)
    assert len(metadata_df) == 0
    assert list(metadata_df.columns) == expected_columns


# ============================================================================
# INTEGRATED WORKFLOW TESTS
# ============================================================================
@pytest.mark.integration
def test_metadata_workflow_integration():
    """End-to-end: download then extract metadata with fake headers."""
    df = get_rechtspraak(max_ecli=30, sd="2025-04-01", save_file="n")

    if df is not None and len(df) > 0:
        metadata = get_rechtspraak_metadata(save_file="n", dataframe=df, _fake_headers=True)
        assert isinstance(metadata, pd.DataFrame)
        assert len(metadata) > 0

        output_file = Path("data/test_metadata_workflow_integration.csv")
        metadata.to_csv(output_file, index=False)
        assert output_file.exists()


@pytest.mark.integration
def test_metadata_extraction_sqlite_method():
    """Test metadata extraction using existing CSV files via API method."""
    csv_files = glob.glob("data/rechtspraak_*.csv")
    if not csv_files:
        pytest.skip("No ECLI CSV files found in data directory")

    df_eclis = pd.read_csv(csv_files[0])
    if len(df_eclis) == 0:
        pytest.skip("CSV file is empty")

    test_df = df_eclis.head(2).copy()
    if "summary" not in test_df.columns:
        test_df["summary"] = ""

    metadata = get_rechtspraak_metadata(save_file="n", dataframe=test_df, _fake_headers=False)

    assert metadata is None or isinstance(metadata, pd.DataFrame), \
        "Should return DataFrame or None on graceful failure"

    if isinstance(metadata, pd.DataFrame) and len(metadata) > 0:
        output_file = Path("data/test_metadata_extraction_sqlite_method.csv")
        metadata.to_csv(output_file, index=False)
        assert output_file.exists()


@pytest.mark.integration
def test_sqlite_extraction_method_with_real_data():
    """Test SQLite extraction method using real ECLIs from CSV files."""
    csv_files = glob.glob("data/rechtspraak_*.csv")
    if not csv_files:
        pytest.skip("No ECLI CSV files found in data directory")

    df_csv = pd.read_csv(csv_files[0])
    if len(df_csv) == 0:
        pytest.skip("CSV file is empty")

    test_eclis = df_csv.head(5)["id"].tolist()
    sqlite_columns = ["ecli", "document_type", "date_decision", "date_publication", "title"]

    results = fetch_eclis_via_sqlite(
        ecli_list=test_eclis,
        sqlite_db_path="data/lido.db",
        columns=sqlite_columns,
    )

    assert isinstance(results, pd.DataFrame)
    assert len(results) > 0

    retrieved_eclis = results["ecli"].tolist()
    for ecli in test_eclis:
        assert ecli in retrieved_eclis
    for col in sqlite_columns:
        assert col in results.columns

    output_file = Path("data/test_sqlite_extraction_method_with_real_data.csv")
    results.to_csv(output_file, index=False)
    assert output_file.exists()


@pytest.mark.integration
def test_sqlite_vs_csv_data_comparison():
    """Compare SQLite retrieval with original CSV data."""
    csv_files = glob.glob("data/rechtspraak_*.csv")
    if not csv_files:
        pytest.skip("No ECLI CSV files found in data directory")

    df_csv = pd.read_csv(csv_files[0])
    if len(df_csv) == 0:
        pytest.skip("CSV file is empty")

    test_eclis = df_csv.head(3)["id"].tolist()
    sqlite_columns = ["ecli", "document_type", "date_decision", "title"]

    results = fetch_eclis_via_sqlite(
        ecli_list=test_eclis,
        sqlite_db_path="data/lido.db",
        columns=sqlite_columns,
    )

    assert len(results) == len(test_eclis)
    for _, row in results.iterrows():
        assert row["ecli"] in test_eclis
        assert row["document_type"]
        assert row["date_decision"]

    output_file = Path("data/test_sqlite_vs_csv_data_comparison.csv")
    results.to_csv(output_file, index=False)
    assert output_file.exists()


# ============================================================================
# BACKWARD COMPATIBILITY / SMOKE TESTS
# ============================================================================
@pytest.mark.integration
def test_rechtspraak_y():
    """Smoke test: download with saving and extract metadata — verify file artifacts."""
    result = get_rechtspraak(max_ecli=10, sd="2025-04-01", save_file="y")
    assert result is None, "save_file='y' should return None"
    csv_files = glob.glob("data/rechtspraak_*.csv")
    assert len(csv_files) > 0, "At least one CSV should be written"


@pytest.mark.integration
def test_rechtspraak_n():
    """Smoke test: download without saving — verify DataFrame is returned."""
    df = get_rechtspraak(max_ecli=10, sd="2025-04-01", save_file="n")
    assert isinstance(df, pd.DataFrame), "save_file='n' should return a DataFrame"
    assert len(df) > 0, "DataFrame should have rows"
