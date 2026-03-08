"""Configuration and API client setup for Vantor/Maxar API."""

import os

MAXAR_API_KEY = os.environ.get("MAXAR_API_KEY", "")
MAXAR_BASE_URL = "https://api.maxar.com"

# Default test AOI: Washington DC
DC_BBOX = [-77.05, 38.88, -77.00, 38.92]


def maxar_headers() -> dict[str, str]:
    """Return auth headers for Maxar API requests.

    Auth is via MAXAR-API-KEY header (NOT Bearer token).
    """
    return {"MAXAR-API-KEY": MAXAR_API_KEY}
