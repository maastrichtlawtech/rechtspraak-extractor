from __future__ import annotations

import glob
import logging
import re
import time
from typing import Optional

import json
import requests
import xmltodict


# ============================================================================
# CONSTANTS
# ============================================================================
RECHTSPRAAK_CSV_PREFIX = "rechtspraak"
METADATA_CSV_SUFFIX = "metadata"


# ============================================================================
# UTILITY FUNCTIONS
# ============================================================================
def check_api(url: str) -> int:
    """
    Check whether the API is working and return the response code.

    Args:
        url: The URL of the API to check.

    Returns:
        The HTTP response status code from the API.
        
    Raises:
        requests.RequestException: If the API request fails.
    """
    try:
        response = requests.get(url, timeout=10)
        return response.status_code
    except requests.RequestException as e:
        logging.error(f"Failed to check API at {url}: {e}")
        raise


def read_csv(dir_name: str, exclude: Optional[str] = None) -> list[str]:
    """
    Read all CSV files in a directory and return a filtered list.

    Args:
        dir_name: The directory path containing CSV files.
        exclude: Optional word to exclude files containing it. Defaults to None.

    Returns:
        List of CSV file paths matching the criteria (with "rechtspraak" in name).

    Notes:
        - Only files with "rechtspraak" in their name are included.
        - If `exclude` is provided, files containing the `exclude` word are excluded.
        
    Example:
        >>> files = read_csv("data/", exclude="metadata")
        >>> len(files)
        5
    """
    csv_files = glob.glob(f"{dir_name}/*.csv")
    files = []
    
    for file_path in csv_files:
        has_rechtspraak = RECHTSPRAAK_CSV_PREFIX in file_path
        has_exclude = exclude is not None and exclude in file_path
        
        if has_rechtspraak and not has_exclude:
            files.append(file_path)

    logging.info(f"Found {len(files)} CSV file(s)")
    return files


def get_exe_time(start_time: float) -> None:
    """
    Calculate and log the total execution time from a start time.

    Args:
        start_time: The start time in seconds since the epoch (from time.time()).

    Logs:
        The total execution time in the format "hours:minutes:seconds".
        
    Example:
        >>> import time
        >>> start = time.time()
        >>> time.sleep(0.5)
        >>> get_exe_time(start)  # Logs approximately "0:0:0.5"
    """
    end_time = time.time()
    elapsed_seconds = end_time - start_time
    
    minutes = int(elapsed_seconds // 60)
    seconds = elapsed_seconds % 60
    hours = minutes // 60
    minutes = minutes % 60
    
    logging.info(f"Total execution time: {hours}:{minutes}:{seconds:.2f}")
    logging.info("")


def _num_of_available_docs(
    url: str,
    start_date: str,
    end_date: str,
    amount: int,
    from_index: int = 0,
) -> int:
    """
    Get the number of available documents from the API for a date range.

    Args:
        url: Base URL of the API.
        start_date: Start date in format 'yyyy-mm-dd'.
        end_date: End date in format 'yyyy-mm-dd'.
        amount: Maximum number of documents to query in a single request.
        from_index: Starting index for pagination. Defaults to 0.

    Returns:
        The number of available documents for the given date range.

    Raises:
        requests.RequestException: If the API request fails.
        KeyError: If the API response format is unexpected.
        ValueError: If the document count cannot be parsed from the response.
    """
    api_url = (
        f"{url}max={amount}&from={from_index}&date={start_date}&date={end_date}"
    )
    
    try:
        response = requests.get(api_url, timeout=10)
        response.raise_for_status()
        response.raw.decode_content = True
        
        # Parse XML response
        parsed_xml = xmltodict.parse(response.text)
        json_string = json.dumps(parsed_xml)
        json_object = json.loads(json_string)
        
        # Extract document count from API response
        subtitle_text = json_object["feed"]["subtitle"]["#text"]
        match = re.search(r"\d+", subtitle_text)
        
        if not match:
            logging.error(f"Could not parse document count from response: {subtitle_text}")
            return 0
            
        doc_count = int(match.group())
        return doc_count
        
    except requests.RequestException as e:
        logging.error(f"Failed to fetch available documents from API: {e}")
        raise
    except (KeyError, ValueError, AttributeError) as e:
        logging.error(f"Unexpected API response format: {e}")
        raise

