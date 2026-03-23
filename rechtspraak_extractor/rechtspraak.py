# This file is used to get all the Rechtspraak ECLIs from an API.
# It takes two required arguments and one optional argument:
# 1. max - Maximum number of ECLIs to retrieve
# 2. starting-date (yyyy-mm-dd) - Start date of ECLI publication
# 3. ending-date (yyyy-mm-dd) - It's an optional parameter. If not given,
#    current date will be automatically chosen
# File is stored in data/rechtspraak folder

from __future__ import annotations

import json
import logging
import re
import time
from datetime import date, datetime
from pathlib import Path
from typing import Optional

import pandas as pd
import requests
import xmltodict

from rechtspraak_extractor.rechtspraak_functions import (
    check_api,
    get_exe_time,
    _num_of_available_docs,
)


# ============================================================================
# CONSTANTS
# ============================================================================
RECHTSPRAAK_API_BASE_URL = "https://data.rechtspraak.nl/uitspraken/zoeken?"
MAX_ECLIS_PER_PAGE = 1000
API_REQUEST_TIMEOUT = 10
SLEEP_BETWEEN_REQUESTS = 1
DATA_DIRECTORY = "data"
CSV_ENCODING = "utf-8"

# DataFrame columns for rechtspraak data
RECHTSPRAAK_COLUMNS = ["id", "title", "summary", "updated", "link"]


# ============================================================================
# HELPER FUNCTIONS
# ============================================================================
def _build_api_url(
    base_url: str,
    max_items: int,
    from_index: int,
    start_date: str,
    end_date: str,
) -> str:
    """
    Build a formatted API URL with query parameters.

    Args:
        base_url: The base API endpoint URL.
        max_items: Maximum number of items to retrieve per page.
        from_index: Starting index for pagination.
        start_date: Start date in format 'yyyy-mm-dd'.
        end_date: End date in format 'yyyy-mm-dd'.

    Returns:
        The complete formatted API URL.
    """
    return (
        f"{base_url}max={max_items}&from={from_index}&"
        f"date={start_date}&date={end_date}"
    )


def _parse_json_entry(entry: dict) -> Optional[dict]:
    """
    Parse a single JSON entry from the API response.

    Args:
        entry: A dictionary representing a single ECLI entry from the API.

    Returns:
        Dictionary with keys: id, title, summary, updated, link.
        Returns None if parsing fails.
    """
    try:
        return {
            "id": entry["id"],
            "title": entry["title"]["#text"],
            "summary": entry["summary"].get("#text", "No summary available"),
            "updated": entry["updated"],
            "link": entry["link"]["@href"],
        }
    except (KeyError, TypeError) as e:
        logging.debug(f"Error parsing JSON entry: {e}")
        return None


def get_data_from_url(
    base_url: str,
    total_docs: int,
    start_date: str,
    end_date: str,
) -> list[dict]:
    """
    Retrieve all ECLI data from the API using pagination.

    Args:
        base_url: The base API endpoint URL.
        total_docs: Total number of documents to retrieve.
        start_date: Start date in format 'yyyy-mm-dd'.
        end_date: End date in format 'yyyy-mm-dd'.

    Returns:
        List of dictionaries containing ECLI data.

    Raises:
        requests.RequestException: If API requests fail.
    """
    all_results = []
    from_index = 0

    while True:
        url = _build_api_url(
            base_url,
            MAX_ECLIS_PER_PAGE,
            from_index,
            start_date,
            end_date,
        )

        try:
            response = requests.get(url, timeout=API_REQUEST_TIMEOUT)
            response.raise_for_status()
            response.raw.decode_content = True

            # Parse XML to JSON
            parsed_xml = xmltodict.parse(response.text)
            json_string = json.dumps(parsed_xml)
            json_object = json.loads(json_string)

            entries = json_object.get("feed", {}).get("entry", [])
            # Ensure entries is a list
            if not isinstance(entries, list):
                entries = [entries] if entries else []

            all_results.extend(entries)
            logging.info(
                f"Retrieved {len(entries)} cases (total: {len(all_results)})"
            )

            # Check if we've reached the target
            if len(all_results) >= total_docs:
                logging.info("Maximum number of ECLIs reached")
                break

            from_index += MAX_ECLIS_PER_PAGE
            time.sleep(SLEEP_BETWEEN_REQUESTS)

        except requests.RequestException as e:
            logging.error(f"Error fetching data from API: {e}")
            raise

    return all_results


def save_csv(
    json_object: list[dict],
    file_name: str,
    save_file: str = "y",
) -> pd.DataFrame:
    """
    Convert JSON data to DataFrame and optionally save as CSV.

    Args:
        json_object: List of dictionaries containing ECLI data.
        file_name: Name for the output CSV file (without extension).
        save_file: Whether to save to disk ('y' or 'n'). Defaults to 'y'.

    Returns:
        pandas DataFrame with columns: id, title, summary, updated, link.

    Notes:
        - If save_file='y', saves to data/{file_name}.csv
        - If summary is missing, uses "No summary available" as placeholder
    """
    df = pd.DataFrame(columns=RECHTSPRAAK_COLUMNS)
    ecli_data = {col: [] for col in RECHTSPRAAK_COLUMNS}

    # Extract data from JSON objects
    for entry in json_object:
        parsed = _parse_json_entry(entry)
        if parsed:
            for col in RECHTSPRAAK_COLUMNS:
                ecli_data[col].append(parsed[col])
        else:
            logging.warning(f"Skipped malformed entry: {entry}")
            continue

    # Create DataFrame
    for col, values in ecli_data.items():
        df[col] = values

    # Save to CSV if requested
    if save_file == "y":
        Path(DATA_DIRECTORY).mkdir(parents=True, exist_ok=True)
        csv_path = Path(DATA_DIRECTORY) / f"{file_name}.csv"
        
        try:
            df.to_csv(csv_path, index=False, encoding=CSV_ENCODING)
            logging.info(f"Data saved to CSV file: {csv_path}")
        except IOError as e:
            logging.error(f"Failed to save CSV file: {e}")
            raise

    return df


def get_rechtspraak(
    max_ecli: int = 1000,
    sd: str = "1900-01-01",
    ed: Optional[str] = None,
    save_file: str = "y",
) -> Optional[pd.DataFrame]:
    """
    Download ECLIs from the Rechtspraak API for a given date range.

    This is the main entry point for retrieving Rechtspraak data.

    Args:
        max_ecli: Maximum number of ECLIs to retrieve. Defaults to 1000.
        sd: Start date in format 'yyyy-mm-dd'. Defaults to '1900-01-01'.
        ed: End date in format 'yyyy-mm-dd'. If None, uses today's date.
        save_file: Whether to save to CSV ('y' or 'n'). Defaults to 'y'.

    Returns:
        pandas DataFrame with ECLI data if save_file='n',
        None if save_file='y' (data saved to file instead).
        Returns None if no documents are available.

    Raises:
        requests.RequestException: If API requests fail.
        KeyError: If API response format is unexpected.

    Example:
        >>> df = get_rechtspraak(max_ecli=100, sd="2020-01-01", save_file="n")
        >>> len(df)
        100
    """
    logging.info("Rechtspraak dump downloader API")

    starting_date = sd
    ending_date = ed or date.today().strftime("%Y-%m-%d")

    # Log parameters
    logging.info(
        f"Parameters: max_ecli={max_ecli}, date_range={starting_date} to {ending_date}"
    )

    start_time = time.time()

    # Build initial request URL to check API
    check_url = _build_api_url(
        RECHTSPRAAK_API_BASE_URL,
        max_ecli,
        0,
        starting_date,
        ending_date,
    )

    # Check if API is working
    logging.info("Checking the API...")
    try:
        response_code = check_api(check_url)
    except requests.RequestException:
        logging.error("Failed to connect to API")
        return None

    if response_code != 200:
        logging.error(f"API returned status code {response_code}")
        return None

    logging.info("API is working fine!")

    # Get the number of available documents
    logging.info("Checking the number of available documents...")
    try:
        total_docs = _num_of_available_docs(
            RECHTSPRAAK_API_BASE_URL,
            starting_date,
            ending_date,
            max_ecli,
        )
    except Exception as e:
        logging.error(f"Failed to get document count: {e}")
        return None

    if total_docs == 0:
        logging.info("No documents available for the given date range")
        return None

    if total_docs < max_ecli:
        logging.info(
            f"Only {total_docs} documents available "
            f"(requested {max_ecli})"
        )

    target_docs = min(total_docs, max_ecli)
    logging.info(
        f"Total documents for retrieval: {target_docs} "
        f"from {starting_date} to {ending_date}"
    )

    # Fetch the data
    try:
        json_object = get_data_from_url(
            RECHTSPRAAK_API_BASE_URL,
            target_docs,
            starting_date,
            ending_date,
        )
    except requests.RequestException:
        logging.error("Failed to retrieve data from API")
        return None

    logging.info(f"Found {len(json_object)} cases")

    if not json_object:
        logging.warning("No data retrieved from API")
        return None

    # Generate output filename
    current_time = datetime.now().strftime("%H-%M-%S")
    filename = f"rechtspraak_{starting_date}_{ending_date}_{current_time}"

    # Save data to CSV and get DataFrame
    result_df = save_csv(json_object, filename, save_file)

    # Log execution time
    get_exe_time(start_time)

    # Return DataFrame only if not saving to file
    if save_file == "n":
        return result_df
    
    return None

