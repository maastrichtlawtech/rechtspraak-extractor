"""
Unit tests for rechtspraak_metadata.py.

All tests are isolated — no real network calls, no real filesystem writes
(except tests that use tmp_path for file-write verification).
"""
from __future__ import annotations

import urllib.error
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from bs4 import BeautifulSoup

from rechtspraak_extractor.rechtspraak_metadata import (
    METADATA_COLUMNS,
    extract_data_from_xml,
    fetch_eclis_via_sqlite,
    get_data_from_api,
    get_rechtspraak_metadata,
    process_metadata_fields,
)


# ---------------------------------------------------------------------------
# extract_data_from_xml
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_extract_data_from_xml_returns_bytes_on_success(sample_ecli_xml):
    mock_response = MagicMock()
    mock_response.read.return_value = sample_ecli_xml
    mock_response.__enter__ = lambda s: s
    mock_response.__exit__ = MagicMock(return_value=False)

    with patch("urllib.request.urlopen", return_value=mock_response):
        result = extract_data_from_xml("http://fake-url")

    assert result == sample_ecli_xml


@pytest.mark.unit
def test_extract_data_from_xml_retries_and_returns_none():
    call_count = 0

    def fake_urlopen(*a, **kw):
        nonlocal call_count
        call_count += 1
        raise urllib.error.URLError("timeout")

    with patch("urllib.request.urlopen", side_effect=fake_urlopen):
        with patch("time.sleep"):  # avoid real delay in tests
            result = extract_data_from_xml("http://fake-url")

    assert result is None
    assert call_count == 2  # MAX_RETRIES = 2


@pytest.mark.unit
def test_extract_data_from_xml_returns_none_on_http_error():
    with patch(
        "urllib.request.urlopen",
        side_effect=urllib.error.HTTPError("url", 404, "Not Found", {}, None),
    ):
        with patch("time.sleep"):
            result = extract_data_from_xml("http://fake-url")

    assert result is None


# ---------------------------------------------------------------------------
# process_metadata_fields
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_process_metadata_fields_extracts_known_fields(sample_ecli_xml):
    soup = BeautifulSoup(sample_ecli_xml, features="xml")
    metadata, has_metadata = process_metadata_fields(soup, "ECLI:NL:HR:2020:1")

    assert has_metadata is True
    assert metadata.get("creator") == "Hoge Raad"
    assert metadata.get("date_decision") == "2020-01-15"
    assert metadata.get("language") == "nl"


@pytest.mark.unit
def test_process_metadata_fields_extracts_full_text(sample_ecli_xml):
    soup = BeautifulSoup(sample_ecli_xml, features="xml")
    metadata, has_metadata = process_metadata_fields(soup, "ECLI:NL:HR:2020:1")

    assert "Full decision text" in metadata.get("full_text", "")


@pytest.mark.unit
def test_process_metadata_fields_empty_xml_returns_no_metadata():
    soup = BeautifulSoup(b"<open-rechtspraak></open-rechtspraak>", features="xml")
    metadata, has_metadata = process_metadata_fields(soup, "ECLI:NL:HR:2020:99")

    assert has_metadata is False
    assert metadata == {}


# ---------------------------------------------------------------------------
# get_data_from_api
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_get_data_from_api_success(sample_ecli_xml, tmp_path):
    with patch(
        "rechtspraak_extractor.rechtspraak_metadata.extract_data_from_xml",
        return_value=sample_ecli_xml,
    ):
        result = get_data_from_api(
            "ECLI:NL:HR:2020:1",
            METADATA_COLUMNS,
            fake_headers=False,
            data_dir=str(tmp_path),
        )

    assert result is not None
    assert len(result) == len(METADATA_COLUMNS)


@pytest.mark.unit
def test_get_data_from_api_returns_none_on_network_failure(tmp_path):
    with patch(
        "rechtspraak_extractor.rechtspraak_metadata.extract_data_from_xml",
        return_value=None,
    ):
        result = get_data_from_api(
            "ECLI:NL:HR:2020:1",
            METADATA_COLUMNS,
            fake_headers=False,
            data_dir=str(tmp_path),
        )

    assert result is None
    failed_files = list(tmp_path.glob("*failed_eclis*"))
    assert len(failed_files) == 1


# ---------------------------------------------------------------------------
# fetch_eclis_via_sqlite (uses in_memory_sqlite_db fixture from conftest)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_fetch_eclis_via_sqlite_returns_matching_row(in_memory_sqlite_db):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:HR:2020:1"],
        sqlite_db_path=in_memory_sqlite_db,
        columns=["ecli", "type", "date_decision"],
    )

    assert len(result) == 1
    assert result.iloc[0]["ecli"] == "ECLI:NL:HR:2020:1"
    assert result.iloc[0]["type"] == "Uitspraak"


@pytest.mark.unit
def test_fetch_eclis_via_sqlite_unknown_ecli_returns_empty(in_memory_sqlite_db):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:INVALID:9999:0"],
        sqlite_db_path=in_memory_sqlite_db,
        columns=["ecli", "type"],
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


@pytest.mark.unit
def test_fetch_eclis_via_sqlite_nonexistent_db_returns_empty(tmp_path):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:HR:2020:1"],
        sqlite_db_path=str(tmp_path / "does_not_exist.db"),
        columns=["ecli"],
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 0


@pytest.mark.unit
@pytest.mark.parametrize(
    "columns",
    [
        ["ecli", "type"],
        ["ecli", "type", "date_decision", "language", "creator"],
    ],
)
def test_fetch_eclis_via_sqlite_various_column_sets(in_memory_sqlite_db, columns):
    result = fetch_eclis_via_sqlite(
        ecli_list=["ECLI:NL:HR:2020:1"],
        sqlite_db_path=in_memory_sqlite_db,
        columns=columns,
    )

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    for col in columns:
        assert col in result.columns


# ---------------------------------------------------------------------------
# get_rechtspraak_metadata — input validation (no network needed)
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_metadata_rejects_both_dataframe_and_filename():
    df = pd.DataFrame({"id": [], "link": []})
    result = get_rechtspraak_metadata(dataframe=df, filename="file.csv", save_file="n")
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_invalid_save_file():
    result = get_rechtspraak_metadata(save_file="maybe")
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_empty_dataframe():
    result = get_rechtspraak_metadata(save_file="n", dataframe=pd.DataFrame())
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_dataframe_with_wrong_columns():
    df = pd.DataFrame({"wrong_col": [1, 2, 3]})
    result = get_rechtspraak_metadata(save_file="n", dataframe=df)
    assert result is None


@pytest.mark.unit
def test_metadata_rejects_no_source_when_save_file_n():
    result = get_rechtspraak_metadata(save_file="n")
    assert result is None
