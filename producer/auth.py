"""
auth.py

Handles OAuth2 client-credentials authentication against the OpenSky Network API.

OpenSky now requires OAuth2 tokens for /states/all (anonymous access has been
phased out). Tokens expire after ~30 minutes, so this module wraps token
fetching + automatic refresh behind a single method, so the rest of the
producer code never has to think about expiry.
"""

import os
import time
import logging
import requests

from dotenv import load_dotenv

from config.config_loader import load_config, get_env_credential

# -------------------------------------------------------
# Load environment variables from the project root (.env)
# -------------------------------------------------------
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
load_dotenv(os.path.join(PROJECT_ROOT, ".env"))

logger = logging.getLogger(__name__)

# Refresh a bit before actual expiry to avoid a request failing mid-flight
_REFRESH_SAFETY_MARGIN_SECONDS = 60


class OpenSkyAuthClient:
    """
    Manages OAuth2 client-credentials tokens for the OpenSky API.

    Example:
        auth = OpenSkyAuthClient()
        headers = {"Authorization": f"Bearer {auth.get_token()}"}
    """

    def __init__(self):
        cfg = load_config()

        self._token_url = cfg["opensky"]["token_url"]
        self._client_id = get_env_credential(
            cfg["opensky"]["client_id_env_var"]
        )
        self._client_secret = get_env_credential(
            cfg["opensky"]["client_secret_env_var"]
        )

        self._access_token = None
        self._expires_at = 0.0  # Force token fetch on first use

    def get_token(self) -> str:
        """
        Return a valid access token.

        Automatically refreshes the token if it is missing or about to expire.
        """
        if self._access_token is None or time.time() >= self._expires_at:
            self._fetch_new_token()

        return self._access_token

    def _fetch_new_token(self) -> None:
        """Fetch a new OAuth2 token from OpenSky."""

        logger.info("Fetching new OpenSky OAuth2 token...")

        payload = {
            "grant_type": "client_credentials",
            "client_id": self._client_id,
            "client_secret": self._client_secret,
        }

        try:
            response = requests.post(
                self._token_url,
                data=payload,
                timeout=15,
            )
            response.raise_for_status()

        except requests.exceptions.RequestException as e:
            logger.error("Failed to fetch OpenSky OAuth2 token: %s", e)
            raise

        data = response.json()

        self._access_token = data["access_token"]

        expires_in = data.get("expires_in", 1800)

        self._expires_at = (
            time.time()
            + expires_in
            - _REFRESH_SAFETY_MARGIN_SECONDS
        )

        logger.info(
            "Token acquired successfully. Valid for ~%d seconds.",
            expires_in,
        )