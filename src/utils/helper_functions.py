"""Helper functions."""

import logging
import math

from requests import Session


def _get_number_of_records(session: Session, endpoint: str) -> int:
    """Fetch the total number of records from the API's metadata endpoint.

    Makes a GET request to the metadata URL ("/meta") and extracts the "total" field
    which represents the total number of records.

    Returns:
        int: The total number of records from the API metadata. Defaults to 0 if an error occurs.

    """
    try:
        # Make a request with retries
        response = session.get(endpoint + "/meta").json()
        return int(response.get("total", 0))
    except Exception as e:
        logging.error(e)


def get_number_of_pages(session: Session, endpoint: str, max_items_per_page: int) -> int:
    """Calculate the total number of pages required to fetch all records from the API.

    It uses the total number of records and divides by the maximum number of items per page (MAX_ITEMS_PAGE).
    If the number of records is zero or an error occurs, returns 0 pages.

    Returns:
        int: The number of pages needed to fetch all the data. Defaults to 0 if no records or error occurs.

    """
    try:
        value = (
            math.ceil(_get_number_of_records(session, endpoint) / max_items_per_page) + 1
            if _get_number_of_records(session, endpoint) > 0
            else 0
        )
    except Exception as e:
        logging.error(e)
        value = 0

    return value
