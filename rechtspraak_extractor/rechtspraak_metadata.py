# This file is used for getting the metadata of the ECLIs obtained using
# rechtspraak_api file. This file takes all the CSV files created by rechtspraak_api,
# picks up ECLIs and links column, and using an API gets the metadata and saves it
# in another CSV file with metadata suffix.

from __future__ import annotations

import logging
import os
import urllib.request
import urllib.error
from typing import Optional, Union
from dataclasses import dataclass
from enum import Enum

import pandas as pd
import time
from bs4 import BeautifulSoup
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from fake_headers import Headers
from pathlib import Path
from rechtspraak_extractor.rechtspraak_functions import (
    read_csv,
    get_exe_time,
)
from threading import Lock
from tqdm import tqdm
from SPARQLWrapper import SPARQLWrapper, JSON
import sqlite3


# ============================================================================
# CONSTANTS
# ============================================================================
RECHTSPRAAK_METADATA_API_BASE_URL = "https://data.rechtspraak.nl/uitspraken/content?id="
API_RETURN_TYPE = "&return=DOC"
MAX_RETRIES = 2
DATE_FORMAT_YMD = "%Y%m%d"
DATE_FORMAT_HMS = "%H-%M-%S"
FAILED_ECLIS_FILENAME_PATTERN = "custom_rechtspraak_{date}_failed_eclis.txt"
NO_METADATA_ECLIS_FILENAME_PATTERN = "custom_rechtspraak_{date}_no_metadata_eclis.txt"
METADATA_CSV_SUFFIX = "_metadata.csv"
DEFAULT_DATA_DIR = "data/raw/"

METADATA_COLUMNS = [
    "ecli",
    "document_type",
    "date_decision",
    "date_publication",
    "language",
    "instance",
    "case_number",
    "procedure_type",
    "spatial",
    "citing",
    "domains",
    "alternative_publications",
    "info",
    "full_text",
]

METADATA_FIELD_MAPPING = {
    "instance": "dcterms:creator",
    "date_decision": "dcterms:date",
    "date_publication": "dcterms:issued",
    "case_number": "psi:zaaknummer",
    "document_type": "dcterms:type",
    "domains": "dcterms:subject",
    "citing": "dcterms:relation",
    "procedure_type": "psi:procedure",
    "alternative_publications": "dcterms:hasVersion",
    "full_text": "uitspraak",
    "info": "dcterms:description",
    "language": "dcterms:language",
    "spatial": "dcterms:spatial",
}

MULTIPLE_VALUE_FIELDS = {
    "domains",
    "case_number",
}

# Global lock for thread-safe file operations
file_write_lock = Lock()
progress_lock = Lock()


# ============================================================================
# TYPE DEFINITIONS
# ============================================================================
class SaveFileOption(Enum):
    """Valid values for save_file parameter."""

    YES = "y"
    NO = "n"


@dataclass
class ExtractionResult:
    """Result of metadata extraction operation."""

    success: bool
    data: Optional[pd.DataFrame] = None
    failed_count: int = 0
    total_count: int = 0


def get_cores() -> int:
    """
    Determines the number of logical CPU cores available on the machine,
    minus one (assuming the main process is computationally intensive).

    Returns:
        int: The maximum number of worker threads to use (CPU count - 1).

    Logging:
        Logs the calculated `max_workers` value as an informational message.
    """
    max_workers = max(1, os.cpu_count() - 1) if os.cpu_count() else 1
    logging.info(f"Maximum {max_workers} threads supported by your machine.")
    return max_workers


def check_file_in_directory(directory_path: str, file_name: str) -> bool:
    """
    Checks if a specific file exists in the specified directory.

    Args:
        directory_path: The path to the directory to check.
        file_name: The name of the file to look for.

    Returns:
        True if the file exists in the directory, False otherwise.
    """
    if not os.path.exists(directory_path):
        logging.debug(f"Directory '{directory_path}' does not exist.")
        return False

    return os.path.isfile(os.path.join(directory_path, file_name))


def extract_data_from_xml(url: str, fake_headers: bool = False) -> Optional[bytes]:
    """
    Fetches and returns XML content from a given URL with retry logic.

    The function attempts to retrieve the XML file up to MAX_RETRIES times
    in case of errors.

    Args:
        url: The URL from which to fetch the XML content.
        fake_headers: Whether to use randomly generated headers (default: False).

    Returns:
        The XML content as bytes if successful, None if all retries fail.
    """
    headers = Headers(headers=True).generate() if fake_headers else None

    for attempt in range(MAX_RETRIES):
        try:
            request = (
                urllib.request.Request(url, headers=headers)
                if headers
                else urllib.request.Request(url)
            )
            with urllib.request.urlopen(request, timeout=10) as response:
                return response.read()
        except (urllib.error.URLError, urllib.error.HTTPError, TimeoutError) as e:
            if attempt < MAX_RETRIES - 1:
                logging.debug(
                    f"Retry {attempt + 1}/{MAX_RETRIES} for URL {url}: {type(e).__name__}"
                )
            else:
                logging.debug(
                    f"Failed to fetch {url} after {MAX_RETRIES} attempts: {type(e).__name__}"
                )
        except Exception as e:
            logging.debug(f"Unexpected error fetching {url}: {e}")

    return None


def get_text_if_exists(element: BeautifulSoup, ecli: str) -> str:
    """
    Safely extracts text from a BeautifulSoup element.

    Returns empty string if element is None or text extraction fails.

    Args:
        element: The BeautifulSoup element to extract text from.
        ecli: The ECLI for logging purposes.

    Returns:
        The text content, or empty string if not available.
    """
    try:
        return element.text if element else ""
    except Exception as e:
        logging.debug(f"Error extracting text from element for ECLI {ecli}: {e}")
        return ""


def save_data_when_crashed(ecli: str, data_dir: str = DEFAULT_DATA_DIR) -> None:
    """
    Saves a failed ECLI to a file in a thread-safe manner.

    Args:
        ecli: The ECLI identifier that failed.
        data_dir: The directory where the failed ECLIs file is stored.
    """
    failed_eclis_filename = FAILED_ECLIS_FILENAME_PATTERN.format(
        date=datetime.now().strftime(DATE_FORMAT_YMD)
    )

    with file_write_lock:  # Thread-safe file write
        try:
            file_path = Path(data_dir) / failed_eclis_filename
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(f"{ecli}\n")
        except IOError as e:
            logging.error(f"Failed to write failed ECLI {ecli} to file: {e}")


def save_no_metadata_ecli(ecli: str, data_dir: str = DEFAULT_DATA_DIR) -> None:
    """
    Saves an ECLI with no metadata to a file in a thread-safe manner.

    When an ECLI is successfully accessed but contains no metadata fields,
    it is recorded separately from failed ECLIs (which indicate access/network issues).

    Args:
        ecli: The ECLI identifier that has no metadata.
        data_dir: The directory where the no-metadata ECLIs file is stored.
    """
    no_metadata_filename = NO_METADATA_ECLIS_FILENAME_PATTERN.format(
        date=datetime.now().strftime(DATE_FORMAT_YMD)
    )

    with file_write_lock:  # Thread-safe file write
        try:
            file_path = Path(data_dir) / no_metadata_filename
            with open(file_path, "a", encoding="utf-8") as f:
                f.write(f"{ecli}\n")
        except IOError as e:
            logging.error(f"Failed to write no-metadata ECLI {ecli} to file: {e}")


def process_metadata_fields(soup: BeautifulSoup, ecli_id: str) -> tuple[dict, bool]:
    """
    Extracts metadata fields from a BeautifulSoup object.

    Args:
        soup: Parsed XML as BeautifulSoup object.
        ecli_id: The ECLI identifier for logging.

    Returns:
        Tuple of (metadata_dict, has_metadata) where has_metadata is True
        if at least one metadata field was found.
    """
    metadata_dict = {}

    for field, tag in METADATA_FIELD_MAPPING.items():
        element = soup.find(tag)
        if element is not None:
            if field in MULTIPLE_VALUE_FIELDS:
                # Handle multiple values for relations and references
                items = soup.find_all(tag)
                values = [get_text_if_exists(item, ecli_id) for item in items]
                value = "\n".join(v for v in values if v)
            else:
                value = get_text_if_exists(element, ecli_id)

            metadata_dict[field] = value

    # Check if any actual metadata was extracted (excluding empty values)
    has_metadata = any(metadata_dict.values())
    return metadata_dict, has_metadata


def report_failed_eclis(
    data_dir: str, file_name: Union[str, int] = ""
) -> tuple[bool, int]:
    """
    Reports summary of failed ECLIs after extraction.

    Args:
        data_dir: The data directory path.
        file_name: Optional file name (string) or ECLI count (int) for batch processing.

    Returns:
        Tuple of (has_failures: bool, count: int)
    """
    failed_eclis_filename = FAILED_ECLIS_FILENAME_PATTERN.format(
        date=datetime.now().strftime(DATE_FORMAT_YMD)
    )

    if check_file_in_directory(data_dir, failed_eclis_filename):
        file_path = Path(data_dir) / failed_eclis_filename
        failed_count = file_path.read_text(encoding="utf-8").count("\n")

        file_suffix = (
            f" from {Path(str(file_name)).stem}.csv"
            if isinstance(file_name, str)
            else ""
        )
        logging.warning(
            f"FAILED: {failed_count} ECLI(s){file_suffix} failed to fetch metadata "
            f"from the API after attempting retries.\nFailed ECLI(s) are stored in: "
            f"{file_path}\nPlease review and retry or contact the administrator."
        )
        return True, failed_count
    else:
        return False, 0


def report_no_metadata_eclis(
    data_dir: str, file_name: Union[str, int] = ""
) -> tuple[bool, int]:
    """
    Reports summary of ECLIs with no metadata after extraction.

    Args:
        data_dir: The data directory path.
        file_name: Optional file name (string) or ECLI count (int) for batch processing.

    Returns:
        Tuple of (has_no_metadata: bool, count: int)
    """
    no_metadata_filename = NO_METADATA_ECLIS_FILENAME_PATTERN.format(
        date=datetime.now().strftime(DATE_FORMAT_YMD)
    )

    if check_file_in_directory(data_dir, no_metadata_filename):
        file_path = Path(data_dir) / no_metadata_filename
        no_metadata_count = file_path.read_text(encoding="utf-8").count("\n")

        file_suffix = (
            f" from {Path(str(file_name)).stem}.csv"
            if isinstance(file_name, str)
            else ""
        )
        logging.info(
            f"INFO: {no_metadata_count} ECLI(s){file_suffix} were successfully accessed "
            f"but contain no metadata in the API.\nThese ECLI(s) are stored in: "
            f"{file_path}\nThey can be reviewed separately or retried later if the API is updated."
        )
        return True, no_metadata_count
    else:
        return False, 0


def fetch_eclis_via_sparql(
    ecli_list: list[str],
    endpoint: str,
    columns: list[str],
    batch_size: int = 100,
) -> pd.DataFrame:
    """
    Fetches metadata for multiple ECLIs in batches using a SPARQL endpoint.

    Args:
        ecli_list: List of ECLI identifiers to fetch.
        endpoint: The SPARQL endpoint URL (e.g., GraphDB).
        columns: Expected column names for the result DataFrame.
        batch_size: Number of ECLIs to query per SPARQL request.

    Returns:
        DataFrame with fetched metadata.
    """
    sparql = SPARQLWrapper(endpoint)
    sparql.setReturnFormat(JSON)
    
    all_results = []
    
    with tqdm(
        total=len(ecli_list),
        colour="BLUE",
        position=0,
        leave=True,
        desc="SPARQL Extraction"
    ) as progress_bar:
        # Process in batches to avoid overwhelming the endpoint or hitting URI limits
        for i in range(0, len(ecli_list), batch_size):
            batch = ecli_list[i:i + batch_size]
            
            # Format ECLI values for the SPARQL query
            values_clause = " ".join([f'<http://deeplink.rechtspraak.nl/uitspraak?id={ecli}>' for ecli in batch])
            
            # LIDO SPARQL Query mapping to required columns
            query = f"""
            PREFIX dcterms: <http://purl.org/dc/terms/>
            PREFIX psi: <http://psi.rechtspraak.nl/>
            PREFIX bwb: <bwb-ontology-url>
            
            SELECT ?ecli 
                   (GROUP_CONCAT(DISTINCT ?creator; separator="\\n") as ?creator_list)
                   (MAX(?date) as ?date_decision)
                   (MAX(?issued) as ?issued_date)
                   (GROUP_CONCAT(DISTINCT ?zaaknummer; separator="\\n") as ?zaaknummer_list)
                   (MAX(?type) as ?type_val)
                   (GROUP_CONCAT(DISTINCT ?subject; separator="\\n") as ?subject_list)
                   (GROUP_CONCAT(DISTINCT ?procedure; separator="\\n") as ?procedure_list)
                   (MAX(?inhoudsindicatie) as ?inhoud_val)
                   (MAX(?hasVersion) as ?version)
                   (GROUP_CONCAT(DISTINCT ?relation; separator="\\n") as ?relations_list)
                   (GROUP_CONCAT(DISTINCT ?reference; separator="\\n") as ?references_list)
                   (MAX(?uitspraak) as ?full_text_val)
            WHERE {{
              VALUES ?ecli_uri {{ {values_clause} }}
              
              ?ecli_uri dcterms:identifier ?ecli .
              OPTIONAL {{ ?ecli_uri dcterms:creator ?creator }}
              OPTIONAL {{ ?ecli_uri dcterms:date ?date }}
              OPTIONAL {{ ?ecli_uri dcterms:issued ?issued }}
              OPTIONAL {{ ?ecli_uri psi:zaaknummer ?zaaknummer }}
              OPTIONAL {{ ?ecli_uri dcterms:type ?type }}
              OPTIONAL {{ ?ecli_uri dcterms:subject ?subject }}
              
              OPTIONAL {{ ?ecli_uri dcterms:relation ?relation }}
              OPTIONAL {{ ?ecli_uri dcterms:references ?reference }}
              OPTIONAL {{ ?ecli_uri psi:procedure ?procedure }}
              OPTIONAL {{ ?ecli_uri <inhoudsindicatie> ?inhoudsindicatie }}
              OPTIONAL {{ ?ecli_uri dcterms:hasVersion ?hasVersion }}
              OPTIONAL {{ ?ecli_uri <uitspraak> ?uitspraak }}
            }}
            GROUP BY ?ecli
            """
            
            sparql.setQuery(query)
            try:
                response = sparql.query().convert()
                
                for result in response["results"]["bindings"]:
                    row = {col: "" for col in columns}
                    
                    ecli_val = result.get("ecli", {}).get("value", "")
                    row["ecli"] = ecli_val
                    row["instance"] = result.get("creator_list", {}).get("value", "")
                    row["date_decision"] = result.get("date_decision", {}).get("value", "")
                    row["date_publication"] = result.get("issued_date", {}).get("value", "")
                    row["language"] = "nl"
                    row["source"] = "Rechtspraak"
                    row["url_publications"] = f"https://uitspraken.rechtspraak.nl/inziendocument?id={ecli_val}"
                    row["case_number"] = result.get("zaaknummer_list", {}).get("value", "")
                    row["document_type"] = result.get("type_val", {}).get("value", "")
                    row["domains"] = result.get("subject_list", {}).get("value", "")
                    row["procedure_type"] = result.get("procedure_list", {}).get("value", "")
                    row["summary"] = result.get("inhoud_val", {}).get("value", "")
                    row["alternative_publications"] = result.get("version", {}).get("value", "")
                    row["citing"] = result.get("relations_list", {}).get("value", "")
                    row["full_text"] = result.get("full_text_val", {}).get("value", "")
                    
                    all_results.append(row)
            except Exception as e:
                logging.error(f"Error querying SPARQL endpoint for batch: {e}")
                
            progress_bar.update(len(batch))
            
    if all_results:
        return pd.DataFrame(all_results, columns=columns)
    else:
        return pd.DataFrame(columns=columns)


def fetch_eclis_via_sqlite(
    ecli_list: list[str],
    sqlite_db_path: str,
    columns: list[str],
) -> pd.DataFrame:
    """
    Fetches metadata for multiple ECLIs using a local pre-built SQLite DB.
    
    Args:
        ecli_list: List of ECLIs to lookup.
        sqlite_db_path: Path to the SQLite database (built via build_lido_sqlite.py).
        columns: Expected column names.
        
    Returns:
        DataFrame with fetched metadata.
    """
    if not os.path.exists(sqlite_db_path):
        logging.error(f"SQLite database {sqlite_db_path} does not exist. Please run `build_lido_sqlite.py` or use method='api'.")
        return pd.DataFrame(columns=columns)
        
    conn = sqlite3.connect(sqlite_db_path)
    
    # Chunking query to not exceed SQLite variable limits (usually 999)
    chunk_size = 900
    all_results = []
    
    with tqdm(total=len(ecli_list), colour="YELLOW", desc="SQLite Extraction") as progress_bar:
        for i in range(0, len(ecli_list), chunk_size):
            chunk = ecli_list[i:i + chunk_size]
            placeholders = ",".join("?" * len(chunk))
            
            # Match Case Law Explorer mapping directly
            query = f"""
                SELECT 
                    ecli, date_publication, language, instance, jurisdiction_city, 
                    date_decision, case_number, document_type, procedure_type, 
                    domains, referenced_legislation_titles, alternative_publications, 
                    title, full_text, summary, citing, cited_by, legislations_cited, 
                    predecessor_successor_cases, url_publications, info, source
                FROM metadata
                WHERE ecli IN ({placeholders})
            """
            
            try:
                # Let pandas parse it directly
                df_chunk = pd.read_sql_query(query, conn, params=chunk)
                
                # Make sure we only grab requested columns and they exist
                for col in columns:
                    if col not in df_chunk.columns:
                        df_chunk[col] = ""
                        
                all_results.append(df_chunk[columns])
            except Exception as e:
                logging.error(f"SQLite query error: {e}")
                
            progress_bar.update(len(chunk))
            
    conn.close()
    
    if all_results:
        return pd.concat(all_results, ignore_index=True)
    return pd.DataFrame(columns=columns)

def fetch_eclis_in_parallel(
    ecli_list: list[str],
    columns: list[str],
    fake_headers: bool = False,
    data_dir: str = DEFAULT_DATA_DIR,
) -> pd.DataFrame:
    """
    Fetches metadata for multiple ECLIs in parallel using thread pool.

    Args:
        ecli_list: List of ECLI identifiers to fetch.
        columns: Column names for the result DataFrame.
        fake_headers: Whether to use fake headers for requests.
        data_dir: The data directory for storing failed ECLIs.

    Returns:
        DataFrame with fetched metadata (may be empty if all failed).
    """
    max_workers = get_cores()
    thread_results = []

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        with tqdm(
            total=len(ecli_list),
            colour="GREEN",
            position=0,
            leave=True,
            miniters=max(1, len(ecli_list) // 100),
            maxinterval=10000,
        ) as progress_bar:
            futures = {
                executor.submit(
                    get_data_from_api,
                    ecli_id=ecli,
                    columns=columns,
                    fake_headers=fake_headers,
                    data_dir=data_dir,
                ): ecli
                for ecli in ecli_list
            }

            for future in futures:
                try:
                    row_data = future.result()
                    if row_data:
                        thread_results.append(pd.DataFrame([row_data], columns=columns))
                except Exception as e:
                    logging.error(f"Error processing future: {e}")
                finally:
                    progress_bar.update(1)

    return (
        pd.concat(thread_results, ignore_index=True)
        if thread_results
        else pd.DataFrame(columns=columns)
    )


def get_data_from_api(
    ecli_id: str,
    columns: list[str],
    fake_headers: bool = False,
    data_dir: str = DEFAULT_DATA_DIR,
) -> Optional[list]:
    """
    Fetches metadata for a single ECLI from the Rechtspraak API.

    Args:
        ecli_id: The ECLI identifier.
        columns: Expected column names for the result.
        fake_headers: Whether to use fake headers for the request.
        data_dir: The data directory for storing failed ECLIs.

    Returns:
        List of row data matching the columns, or None if extraction fails.
        Note: Returns row data even if no metadata was found (all values empty),
        but such ECLIs are logged separately as "no metadata" cases.
    """
    url = f"{RECHTSPRAAK_METADATA_API_BASE_URL}{ecli_id}{API_RETURN_TYPE}"

    try:
        xml_object = extract_data_from_xml(url, fake_headers=fake_headers)
        if xml_object is None:
            logging.debug(
                f"Failed to fetch XML content for ECLI: {ecli_id} after "
                f"attempting {MAX_RETRIES} retries"
            )
            save_data_when_crashed(ecli_id, data_dir)
            return None

        soup = BeautifulSoup(xml_object, features="xml")
        metadata_dict, has_metadata = process_metadata_fields(soup, ecli_id)

        # Check if any metadata was actually found
        if not has_metadata:
            logging.debug(f"No metadata found for ECLI {ecli_id}")
            save_no_metadata_ecli(ecli_id, data_dir)
            return None

        # Add ECLI and ensure all expected columns exist
        metadata_dict["ecli"] = ecli_id
        metadata_dict = {col: metadata_dict.get(col, "") for col in columns}
        row_data = [metadata_dict[col] for col in columns]

        if len(row_data) != len(columns):
            logging.error(
                f"Row data length ({len(row_data)}) does not match "
                f"expected columns ({len(columns)}) for ECLI {ecli_id}."
            )
            return None

        return row_data

    except Exception as e:
        logging.debug(
            f"Error extracting metadata for ECLI {ecli_id}: {type(e).__name__}: {e}. "
            f"ECLI will be marked as failed."
        )
        save_data_when_crashed(ecli_id, data_dir)
        return None


def get_rechtspraak_metadata(
    save_file: str = SaveFileOption.NO.value,
    dataframe: Optional[pd.DataFrame] = None,
    filename: Optional[str] = None,
    _fake_headers: bool = False,
    multi_threading: bool = True,
    data_dir: str = DEFAULT_DATA_DIR,
    method: str = "api",
    sparql_endpoint: str = "http://localhost:7200/repositories/lido",
    sqlite_db_path: str = "data/lido_metadata.db",
    fallback_to_api: bool = True,
    batch_size: int = 100,
) -> Union[bool, pd.DataFrame]:
    """
    Extracts metadata from the Rechtspraak API or a SPARQL endpoint for a given dataset or file.

    Args:
        save_file: Save to file? 'y' (yes) or 'n' (no). Default: 'n'.
        dataframe: Optional DataFrame with "id" and "link" columns.
        filename: Optional CSV filename in data_dir with "id" and "link" columns.
        _fake_headers: Use fake headers for API requests (use responsibly).
        multi_threading: Enable multithreading (default: True).
        data_dir: Directory path for data files (default: "data/raw/").

    Returns:
        - DataFrame if save_file="n" and extraction succeeds.
        - True if save_file="y" and extraction succeeds.
        - False if there are errors or inputs are invalid.

    Raises:
        ValueError: If both dataframe and filename are provided, or if neither
        is provided when save_file="n".

    Notes:
        - If save_file="y" and neither dataframe nor filename is provided,
          metadata will be extracted for all CSV files in data_dir.
        - Failed ECLIs are logged and saved to "_failed_eclis.txt".
        - Uses multithreading for better performance.

    Example:
        # From DataFrame
        result = get_rechtspraak_metadata(dataframe=df, save_file="n")

        # From file
        get_rechtspraak_metadata(filename="data.csv", save_file="y")

        # All files in directory
        get_rechtspraak_metadata(save_file="y")
    """
    # Input validation
    if dataframe is not None and filename is not None:
        logging.error("Provide either dataframe or filename, not both.")
        return False

    if dataframe is None and filename is None and save_file == SaveFileOption.NO.value:
        logging.error("Provide dataframe or filename when save_file='n'.")
        return False

    if save_file not in (SaveFileOption.YES.value, SaveFileOption.NO.value):
        logging.error(f"save_file must be 'y' or 'n', got '{save_file}'.")
        return False

    logging.info("Starting extraction with Rechtspraak metadata API")
    start_time = time.time()
    data_dir = str(Path(data_dir))  # Normalize path

    # Process single file or dataframe
    if filename is not None or dataframe is not None:
        return _process_single_source(
            dataframe=dataframe,
            filename=filename,
            save_file=save_file,
            fake_headers=_fake_headers,
            data_dir=data_dir,
            start_time=start_time,
            method=method,
            sparql_endpoint=sparql_endpoint,
            sqlite_db_path=sqlite_db_path,
            fallback_to_api=fallback_to_api,
            batch_size=batch_size,
        )

    # Process all files in directory
    if save_file == SaveFileOption.YES.value:
        return _process_all_files_in_directory(
            data_dir=data_dir,
            fake_headers=_fake_headers,
            start_time=start_time,
            method=method,
            sparql_endpoint=sparql_endpoint,
            sqlite_db_path=sqlite_db_path,
            fallback_to_api=fallback_to_api,
            batch_size=batch_size,
        )

    return False


def _validate_data_source(data: pd.DataFrame, source_name: str = "DataFrame") -> bool:
    """
    Validates that a DataFrame has required columns.

    Args:
        data: The DataFrame to validate.
        source_name: Name of the source for logging.

    Returns:
        True if valid, False otherwise.
    """
    if data.empty:
        logging.error(f"{source_name} is empty.")
        return False

    if "id" not in data.columns or "link" not in data.columns:
        logging.error(f"{source_name} missing required 'id' or 'link' columns.")
        logging.debug(f"Available columns: {data.columns.tolist()}")
        return False

    return True


def _process_single_source(
    dataframe: Optional[pd.DataFrame],
    filename: Optional[str],
    save_file: str,
    fake_headers: bool,
    data_dir: str,
    start_time: float,
    method: str = "api",
    sparql_endpoint: str = "http://localhost:7200/repositories/lido",
    sqlite_db_path: str = "data/lido_metadata.db",
    fallback_to_api: bool = True,
    batch_size: int = 100,
) -> Union[bool, pd.DataFrame]:
    """
    Process metadata extraction for a single source (file or dataframe).

    Args:
        dataframe: Optional input DataFrame.
        filename: Optional input filename.
        save_file: Whether to save to file.
        fake_headers: Use fake headers for requests.
        data_dir: Data directory path.
        start_time: Start time for execution timing.

    Returns:
        DataFrame if save_file="n", True if save_file="y", False on error.
    """
    # Load data
    if filename is not None:
        file_path = Path(data_dir) / filename

        if not file_path.exists():
            logging.error(f"File not found: {file_path}")
            return False

        data = pd.read_csv(file_path)
        source_name = f"File '{filename}'"
    else:
        data = dataframe
        source_name = "DataFrame"

    if not _validate_data_source(data, source_name):
        return False

    # Check if metadata already exists
    if filename is not None:
        output_path = Path(data_dir) / (Path(filename).stem + METADATA_CSV_SUFFIX)
        if output_path.exists():
            logging.info(f"Metadata already exists: {output_path}")
            return False

    logging.info(f"Processing {len(data)} ECLIs...")
    num_eclis = len(data)
    ecli_list = data["id"].tolist()

    # Fetch metadata
    if method == "sqlite":
        logging.info(f"Extracting {len(ecli_list)} ECLIs via local SQLite database at {sqlite_db_path}")
        metadata_df = fetch_eclis_via_sqlite(
            ecli_list=ecli_list,
            sqlite_db_path=sqlite_db_path,
            columns=METADATA_COLUMNS,
        )
        
        # Fallback check
        if not metadata_df.empty:
            found_eclis = set(metadata_df["ecli"].tolist())
        else:
            found_eclis = set()
            
        missing_eclis = [e for e in ecli_list if e not in found_eclis]
        
        if missing_eclis and fallback_to_api:
            logging.info(f"SQLite returned {len(found_eclis)}/{len(ecli_list)} records. Falling back to API for {len(missing_eclis)} remaining ECLIs...")
            fallback_df = fetch_eclis_in_parallel(
                ecli_list=missing_eclis,
                columns=METADATA_COLUMNS,
                fake_headers=fake_headers,
                data_dir=data_dir,
            )
            if not metadata_df.empty:
                metadata_df = pd.concat([metadata_df, fallback_df], ignore_index=True)
            else:
                metadata_df = fallback_df
        elif missing_eclis:
            logging.warning(f"{len(missing_eclis)} ECLIs not found via SQLite and fallback is disabled.")
            for ecli in missing_eclis:
                save_data_when_crashed(ecli, data_dir)
                
    elif method == "sparql":
        logging.info(f"Extracting {len(ecli_list)} ECLIs via SPARQL endpoint {sparql_endpoint}")
        metadata_df = fetch_eclis_via_sparql(
            ecli_list=ecli_list,
            endpoint=sparql_endpoint,
            columns=METADATA_COLUMNS,
            batch_size=batch_size,
        )
        
        # Check for missing
        if not metadata_df.empty:
            found_eclis = set(metadata_df["ecli"].tolist())
        else:
            found_eclis = set()
            
        missing_eclis = [e for e in ecli_list if e not in found_eclis]
        
        if missing_eclis and fallback_to_api:
            logging.info(f"SPARQL returned {len(found_eclis)}/{len(ecli_list)} records. Falling back to API for remaining {len(missing_eclis)} ECLIs...")
            fallback_df = fetch_eclis_in_parallel(
                ecli_list=missing_eclis,
                columns=METADATA_COLUMNS,
                fake_headers=fake_headers,
                data_dir=data_dir,
            )
            if not metadata_df.empty:
                metadata_df = pd.concat([metadata_df, fallback_df], ignore_index=True)
            else:
                metadata_df = fallback_df
        elif missing_eclis:
            logging.warning(f"{len(missing_eclis)} ECLIs not found via SPARQL and fallback is disabled.")
            for ecli in missing_eclis:
                save_data_when_crashed(ecli, data_dir)
                
    else:
        metadata_df = fetch_eclis_in_parallel(
            ecli_list=ecli_list,
            columns=METADATA_COLUMNS,
            fake_headers=fake_headers,
            data_dir=data_dir,
        )

    # Merge with original data
    if not metadata_df.empty:
        metadata_df = metadata_df.merge(
            data[["id", "summary"]],
            how="left",
            left_on="ecli",
            right_on="id",
        ).drop("id", axis=1)

    # Handle empty results
    if metadata_df.empty:
        logging.warning(
            "Metadata not found. Check if API is available or has changed. "
            "Please try again or contact the administrator."
        )

    # Save to file if requested
    if save_file == SaveFileOption.YES.value:
        Path(data_dir).mkdir(parents=True, exist_ok=True)

        if filename:
            output_file = Path(data_dir) / (Path(filename).stem + METADATA_CSV_SUFFIX)
        else:
            output_file = Path(data_dir) / (
                f"custom_rechtspraak_{datetime.now().strftime(DATE_FORMAT_HMS)}.csv"
            )

        metadata_df.to_csv(output_file, index=False, encoding="utf-8")
        logging.info(f"Metadata saved to: {output_file}")

    get_exe_time(start_time)
    report_failed_eclis(data_dir, filename or num_eclis)
    report_no_metadata_eclis(data_dir, filename or num_eclis)

    return metadata_df if save_file == SaveFileOption.NO.value else True


def _process_all_files_in_directory(
    data_dir: str,
    fake_headers: bool,
    start_time: float,
    method: str = "api",
    sparql_endpoint: str = "http://localhost:7200/repositories/lido",
    sqlite_db_path: str = "data/lido_metadata.db",
    fallback_to_api: bool = True,
    batch_size: int = 100,
) -> bool:
    """
    Process metadata extraction for all CSV files in a directory.

    Args:
        data_dir: The data directory path.
        fake_headers: Use fake headers for requests.
        start_time: Start time for execution timing.

    Returns:
        True if all files processed successfully, False on error.
    """
    csv_files = read_csv(data_dir, "metadata")

    if not csv_files:
        logging.warning("No CSV files found in data directory.")
        return True

    logging.info(f"Processing {len(csv_files)} files...")

    for file_path in csv_files:
        file_name = Path(file_path).name
        file_stem = Path(file_path).stem

        # Check if metadata already exists
        output_path = Path(data_dir) / (file_stem + METADATA_CSV_SUFFIX)
        if output_path.exists():
            logging.info(f"Metadata already exists: {file_stem}{METADATA_CSV_SUFFIX}")
            continue

        try:
            data = pd.read_csv(file_path)

            if not _validate_data_source(data, f"File '{file_name}'"):
                continue

            logging.info(f"Processing {len(data)} ECLIs from {file_name}...")
            ecli_list = data["id"].tolist()

            # Fetch metadata
            if method == "sqlite":
                logging.info(f"Extracting {len(ecli_list)} ECLIs via local SQLite database at {sqlite_db_path}")
                metadata_df = fetch_eclis_via_sqlite(
                    ecli_list=ecli_list,
                    sqlite_db_path=sqlite_db_path,
                    columns=METADATA_COLUMNS,
                )
                
                if not metadata_df.empty:
                    found_eclis = set(metadata_df["ecli"].tolist())
                else:
                    found_eclis = set()
                    
                missing_eclis = [e for e in ecli_list if e not in found_eclis]
                
                if missing_eclis and fallback_to_api:
                    logging.info(f"SQLite returned {len(found_eclis)}/{len(ecli_list)} records. Falling back to API for {len(missing_eclis)} remaining ECLIs...")
                    fallback_df = fetch_eclis_in_parallel(
                        ecli_list=missing_eclis,
                        columns=METADATA_COLUMNS,
                        fake_headers=fake_headers,
                        data_dir=data_dir,
                    )
                    if not metadata_df.empty:
                        metadata_df = pd.concat([metadata_df, fallback_df], ignore_index=True)
                    else:
                        metadata_df = fallback_df
                elif missing_eclis:
                    logging.warning(f"{len(missing_eclis)} ECLIs not found via SQLite and fallback is disabled.")
                    for ecli in missing_eclis:
                        save_data_when_crashed(ecli, data_dir)
            elif method == "sparql":
                logging.info(f"Extracting {len(ecli_list)} ECLIs via SPARQL endpoint {sparql_endpoint}")
                metadata_df = fetch_eclis_via_sparql(
                    ecli_list=ecli_list,
                    endpoint=sparql_endpoint,
                    columns=METADATA_COLUMNS,
                    batch_size=batch_size,
                )
                
                if not metadata_df.empty:
                    found_eclis = set(metadata_df["ecli"].tolist())
                else:
                    found_eclis = set()
                    
                missing_eclis = [e for e in ecli_list if e not in found_eclis]
                
                if missing_eclis and fallback_to_api:
                    logging.info(f"SPARQL returned {len(found_eclis)}/{len(ecli_list)} records. Falling back to API for {len(missing_eclis)} remaining ECLIs...")
                    fallback_df = fetch_eclis_in_parallel(
                        ecli_list=missing_eclis,
                        columns=METADATA_COLUMNS,
                        fake_headers=fake_headers,
                        data_dir=data_dir,
                    )
                    if not metadata_df.empty:
                        metadata_df = pd.concat([metadata_df, fallback_df], ignore_index=True)
                    else:
                        metadata_df = fallback_df
                elif missing_eclis:
                    logging.warning(f"{len(missing_eclis)} ECLIs not found via SPARQL and fallback is disabled.")
                    for ecli in missing_eclis:
                        save_data_when_crashed(ecli, data_dir)
            else:
                metadata_df = fetch_eclis_in_parallel(
                    ecli_list=ecli_list,
                    columns=METADATA_COLUMNS,
                    fake_headers=fake_headers,
                    data_dir=data_dir,
                )

            # Merge with original data
            if not metadata_df.empty:
                metadata_df = metadata_df.merge(
                    data[["id", "summary"]],
                    how="left",
                    left_on="ecli",
                    right_on="id",
                ).drop("id", axis=1)

            # Save to file
            if not metadata_df.empty:
                output_path.parent.mkdir(parents=True, exist_ok=True)
                metadata_df.to_csv(output_path, index=False, encoding="utf-8")
                logging.info(f"Saved: {output_path}")
            else:
                logging.warning(
                    f"No metadata retrieved for {file_name}. Check API status."
                )

            # Report failed and no-metadata ECLIs
            report_failed_eclis(data_dir, file_name)
            report_no_metadata_eclis(data_dir, file_name)

        except Exception as e:
            logging.error(f"Error processing {file_name}: {e}")
            continue

    get_exe_time(start_time)
    return True
