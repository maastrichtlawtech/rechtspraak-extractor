"""
Unit tests for rechtspraak.py and rechtspraak_functions.py.

All tests are isolated — no real network calls, no real filesystem writes.
"""
from __future__ import annotations

import logging
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
import requests

from rechtspraak_extractor.rechtspraak import (
    _build_api_url,
    _parse_json_entry,
    get_rechtspraak,
    save_csv,
)
from rechtspraak_extractor.rechtspraak_functions import (
    _num_of_available_docs,
    check_api,
    get_exe_time,
    parse_xml_response,
    read_csv,
)


# ---------------------------------------------------------------------------
# parse_xml_response
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_parse_xml_response_returns_plain_dict():
    xml = "<feed><subtitle>#text>10</subtitle></feed>"
    result = parse_xml_response(xml)
    assert isinstance(result, dict)
    assert "feed" in result


@pytest.mark.unit
def test_parse_xml_response_malformed_raises():
    with pytest.raises(Exception):
        parse_xml_response("<<not valid xml")


# ---------------------------------------------------------------------------
# _build_api_url
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_build_api_url_contains_all_params():
    url = _build_api_url("https://base.nl/?", 100, 50, "2020-01-01", "2020-12-31")
    assert "max=100" in url
    assert "from=50" in url
    assert "date=2020-01-01" in url
    assert "date=2020-12-31" in url


@pytest.mark.unit
def test_build_api_url_starts_with_base():
    base = "https://base.nl/?"
    url = _build_api_url(base, 10, 0, "2020-01-01", "2020-12-31")
    assert url.startswith(base)


# ---------------------------------------------------------------------------
# _parse_json_entry
# ---------------------------------------------------------------------------
_VALID_ENTRY = {
    "id": "ECLI:NL:HR:2020:1",
    "title": {"#text": "Test Decision"},
    "summary": {"#text": "Summary text"},
    "updated": "2020-01-01",
    "link": {"@href": "https://example.com/ecli"},
}


@pytest.mark.unit
def test_parse_json_entry_valid_returns_dict():
    result = _parse_json_entry(_VALID_ENTRY)
    assert result is not None
    assert result["id"] == "ECLI:NL:HR:2020:1"
    assert result["title"] == "Test Decision"
    assert result["summary"] == "Summary text"
    assert result["link"] == "https://example.com/ecli"


@pytest.mark.unit
def test_parse_json_entry_missing_key_returns_none():
    result = _parse_json_entry({"id": "X"})
    assert result is None


@pytest.mark.unit
def test_parse_json_entry_empty_summary_uses_default():
    entry = {**_VALID_ENTRY, "summary": {}}
    result = _parse_json_entry(entry)
    assert result is not None
    assert result["summary"] == "No summary available"


@pytest.mark.unit
@pytest.mark.parametrize(
    "bad_entry",
    [
        {},
        {"id": "X", "title": None},
        None,
    ],
)
def test_parse_json_entry_bad_inputs_return_none(bad_entry):
    result = (
        _parse_json_entry(bad_entry) if bad_entry is not None else _parse_json_entry({})
    )
    assert result is None


# ---------------------------------------------------------------------------
# check_api
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_check_api_returns_status_code(monkeypatch):
    mock_resp = MagicMock(status_code=200)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: mock_resp)
    assert check_api("http://fake-url") == 200


@pytest.mark.unit
def test_check_api_returns_non_200(monkeypatch):
    mock_resp = MagicMock(status_code=404)
    monkeypatch.setattr(requests, "get", lambda *a, **kw: mock_resp)
    assert check_api("http://fake-url") == 404


@pytest.mark.unit
def test_check_api_raises_on_request_exception(monkeypatch):
    def raise_exc(*a, **kw):
        raise requests.RequestException("fail")

    monkeypatch.setattr(requests, "get", raise_exc)
    with pytest.raises(requests.RequestException):
        check_api("http://fake-url")


# ---------------------------------------------------------------------------
# get_exe_time
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_get_exe_time_logs_execution_time(caplog):
    with caplog.at_level(logging.INFO):
        get_exe_time(time.time() - 65)
    assert "Total execution time" in caplog.text


@pytest.mark.unit
def test_get_exe_time_handles_sub_minute(caplog):
    with caplog.at_level(logging.INFO):
        get_exe_time(time.time() - 5)
    assert "Total execution time" in caplog.text


# ---------------------------------------------------------------------------
# read_csv
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_read_csv_finds_rechtspraak_files(tmp_path):
    (tmp_path / "rechtspraak_data.csv").touch()
    (tmp_path / "other_file.csv").touch()
    result = read_csv(str(tmp_path))
    assert len(result) == 1
    assert "rechtspraak_data" in result[0]


@pytest.mark.unit
def test_read_csv_excludes_by_keyword(tmp_path):
    (tmp_path / "rechtspraak_data.csv").touch()
    (tmp_path / "rechtspraak_data_metadata.csv").touch()
    result = read_csv(str(tmp_path), exclude="metadata")
    assert len(result) == 1
    assert "metadata" not in result[0]


@pytest.mark.unit
def test_read_csv_empty_directory(tmp_path):
    result = read_csv(str(tmp_path))
    assert result == []


# ---------------------------------------------------------------------------
# _num_of_available_docs
# ---------------------------------------------------------------------------
_FEED_WITH_COUNT = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed><subtitle type="text">42</subtitle></feed>"""


@pytest.mark.unit
def test_num_of_available_docs_extracts_count(monkeypatch):
    mock_resp = MagicMock(status_code=200, text=_FEED_WITH_COUNT)
    mock_resp.raise_for_status = MagicMock()
    mock_resp.raw = MagicMock()
    mock_resp.raw.decode_content = True
    monkeypatch.setattr(requests, "get", lambda *a, **kw: mock_resp)
    count = _num_of_available_docs("https://base.nl/?", "2020-01-01", "2020-12-31", 100)
    assert count == 42


@pytest.mark.unit
def test_num_of_available_docs_raises_on_request_failure(monkeypatch):
    def raise_exc(*a, **kw):
        raise requests.RequestException("timeout")

    monkeypatch.setattr(requests, "get", raise_exc)
    with pytest.raises(requests.RequestException):
        _num_of_available_docs("https://base.nl/?", "2020-01-01", "2020-12-31", 100)


# ---------------------------------------------------------------------------
# save_csv
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_save_csv_returns_dataframe_without_saving(tmp_path):
    import pandas as pd

    entries = [_VALID_ENTRY]
    df = save_csv(entries, "test_output", save_file="n")
    assert isinstance(df, pd.DataFrame)
    assert "id" in df.columns
    assert len(df) == 1
    assert not (tmp_path / "data" / "test_output.csv").exists()


@pytest.mark.unit
def test_save_csv_skips_unparseable_entries():
    import pandas as pd

    entries = [_VALID_ENTRY, {"bad": "entry"}]
    df = save_csv(entries, "test_output", save_file="n")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1


# ---------------------------------------------------------------------------
# get_rechtspraak error paths
# ---------------------------------------------------------------------------
@pytest.mark.unit
def test_get_rechtspraak_returns_none_on_api_failure(monkeypatch):
    def raise_exc(*a, **kw):
        raise requests.RequestException("network error")

    monkeypatch.setattr(requests, "get", raise_exc)
    result = get_rechtspraak(max_ecli=10, sd="2020-01-01", save_file="n")
    assert result is None


@pytest.mark.unit
def test_get_rechtspraak_returns_none_on_http_error(monkeypatch):
    mock_resp = MagicMock(status_code=200)
    mock_resp.raise_for_status.side_effect = requests.HTTPError("404")
    mock_resp.raw = MagicMock()
    mock_resp.raw.decode_content = True
    monkeypatch.setattr(requests, "get", lambda *a, **kw: mock_resp)
    result = get_rechtspraak(max_ecli=10, sd="2020-01-01", save_file="n")
    assert result is None
