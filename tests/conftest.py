from __future__ import annotations

import sqlite3
from pathlib import Path
from unittest.mock import MagicMock

import pandas as pd
import pytest


# ---------------------------------------------------------------------------
# Marker registration
# ---------------------------------------------------------------------------
def pytest_configure(config: pytest.Config) -> None:
    config.addinivalue_line("markers", "unit: fast, no network, no filesystem")
    config.addinivalue_line("markers", "integration: requires live API or real files")


# ---------------------------------------------------------------------------
# Sample XML strings (mirror real Rechtspraak API responses)
# ---------------------------------------------------------------------------
SAMPLE_FEED_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<feed>
  <subtitle>#text>42</subtitle>
  <entry>
    <id>ECLI:NL:HR:2020:1</id>
    <title><![CDATA[Test Decision]]></title>
    <summary><![CDATA[Summary text]]></summary>
    <updated>2020-01-01</updated>
    <link href="https://data.rechtspraak.nl/uitspraken/content?id=ECLI:NL:HR:2020:1"/>
  </entry>
</feed>"""

SAMPLE_ECLI_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<open-rechtspraak
  xmlns:dcterms="http://purl.org/dc/terms/"
  xmlns:psi="http://www.rechtspraak.nl/schema/rechtspraak-1.0">
  <rs:metadata xmlns:rs="http://www.rechtspraak.nl/schema/rechtspraak-1.0">
    <dcterms:creator>Hoge Raad</dcterms:creator>
    <dcterms:date>2020-01-15</dcterms:date>
    <dcterms:issued>2020-01-20</dcterms:issued>
    <dcterms:type>Uitspraak</dcterms:type>
    <dcterms:language>nl</dcterms:language>
    <psi:zaaknummer>19/01234</psi:zaaknummer>
    <dcterms:spatial>Amsterdam</dcterms:spatial>
  </rs:metadata>
  <uitspraak>Full decision text here.</uitspraak>
</open-rechtspraak>"""


@pytest.fixture
def sample_feed_xml() -> str:
    return SAMPLE_FEED_XML


@pytest.fixture
def sample_ecli_xml() -> bytes:
    return SAMPLE_ECLI_XML.encode("utf-8")


# ---------------------------------------------------------------------------
# DataFrame fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def sample_ecli_dataframe() -> pd.DataFrame:
    return pd.DataFrame(
        {
            "id": ["ECLI:NL:HR:2020:1", "ECLI:NL:HR:2020:2"],
            "title": ["Decision One", "Decision Two"],
            "summary": ["Summary one", "Summary two"],
            "updated": ["2020-01-01", "2020-01-02"],
            "link": [
                "https://data.rechtspraak.nl/uitspraken/content?id=ECLI:NL:HR:2020:1",
                "https://data.rechtspraak.nl/uitspraken/content?id=ECLI:NL:HR:2020:2",
            ],
        }
    )


# ---------------------------------------------------------------------------
# Mock API response fixture
# ---------------------------------------------------------------------------
@pytest.fixture
def mock_api_response(sample_feed_xml: str) -> MagicMock:
    mock_resp = MagicMock()
    mock_resp.status_code = 200
    mock_resp.text = sample_feed_xml
    mock_resp.raise_for_status = MagicMock()
    mock_resp.raw = MagicMock()
    mock_resp.raw.decode_content = True
    return mock_resp


# ---------------------------------------------------------------------------
# In-memory SQLite database fixture
# ---------------------------------------------------------------------------
_SQLITE_SCHEMA = """
CREATE TABLE metadata (
    ecli TEXT PRIMARY KEY,
    date_publication TEXT,
    language TEXT,
    instance TEXT,
    jurisdiction_city TEXT,
    date_decision TEXT,
    case_number TEXT,
    document_type TEXT,
    procedure_type TEXT,
    domains TEXT,
    referenced_legislation_titles TEXT,
    alternative_publications TEXT,
    title TEXT,
    full_text TEXT,
    summary TEXT,
    citing TEXT,
    cited_by TEXT,
    legislations_cited TEXT,
    predecessor_successor_cases TEXT,
    url_publications TEXT,
    info TEXT,
    source TEXT
)
"""

_SQLITE_SEED = """
INSERT INTO metadata (ecli, document_type, date_decision, language, instance)
VALUES ('ECLI:NL:HR:2020:1', 'Uitspraak', '2020-01-15', 'nl', 'Hoge Raad')
"""


@pytest.fixture
def in_memory_sqlite_db(tmp_path: Path) -> str:
    """Temporary SQLite database with the metadata table and one seed row."""
    db_path = str(tmp_path / "test_metadata.db")
    conn = sqlite3.connect(db_path)
    conn.execute(_SQLITE_SCHEMA)
    conn.execute(_SQLITE_SEED)
    conn.commit()
    conn.close()
    return db_path


# ---------------------------------------------------------------------------
# Data-directory setup (kept from existing test file, now centralised)
# ---------------------------------------------------------------------------
@pytest.fixture(scope="session", autouse=True)
def ensure_data_directories() -> None:
    Path("data/raw").mkdir(parents=True, exist_ok=True)
    Path("data").mkdir(parents=True, exist_ok=True)
