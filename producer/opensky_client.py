"""
opensky_client.py

Thin client around the OpenSky /states/all endpoint.

Key thing this module handles correctly (a common source of bugs):
OpenSky returns each aircraft as a flat POSITIONAL array, not a named JSON
object. Field order is fixed and documented - get it wrong and every field
downstream is silently mislabeled. See STATE_VECTOR_FIELDS below.
"""

import logging
import requests

from producer.auth import OpenSkyAuthClient
from config.config_loader import load_config

logger = logging.getLogger(__name__)

# Official OpenSky state vector field order (index position matters).
# Reference: https://openskynetwork.github.io/opensky-api/rest.html
STATE_VECTOR_FIELDS = [
    "icao24",           # 0  - unique 24-bit ICAO transponder address (hex string)
    "callsign",         # 1  - flight callsign, often null/blank, needs stripping
    "origin_country",   # 2  - country inferred from ICAO24 address
    "time_position",    # 3  - unix timestamp of last position report (nullable)
    "last_contact",     # 4  - unix timestamp of last message of any kind received
    "longitude",        # 5  - decimal degrees (nullable)
    "latitude",         # 6  - decimal degrees (nullable)
    "baro_altitude",    # 7  - barometric altitude in meters (nullable)
    "on_ground",        # 8  - boolean, true if aircraft is on the ground
    "velocity",         # 9  - m/s (nullable)
    "true_track",       # 10 - heading in degrees, clockwise from north (nullable)
    "vertical_rate",    # 11 - m/s, positive = climbing, negative = descending (nullable)
    "sensors",          # 12 - list of sensor IDs that received this state (usually null)
    "geo_altitude",     # 13 - geometric altitude in meters (nullable, GPS-based)
    "squawk",           # 14 - transponder squawk code (nullable)
    "spi",              # 15 - special purpose indicator flag
    "position_source",  # 16 - 0=ADS-B, 1=ASTERIX, 2=MLAT, 3=FLARM
    "category",         # 17 - aircraft category enum (may be absent on older API responses)
]


class OpenSkyClient:
    """Fetches and parses OpenSky state vectors within a configured bounding box."""

    def __init__(self):
        self._cfg = load_config()
        self._base_url = self._cfg["opensky"]["base_url"]
        self._bbox = self._cfg["opensky"]["bounding_box"]
        self._auth = OpenSkyAuthClient()

    def fetch_states(self) -> list[dict]:
        """
        Fetch current state vectors from OpenSky, parsed into a list of dicts.

        Returns:
            list[dict]: one dict per aircraft state, keyed by STATE_VECTOR_FIELDS.
                         Returns an empty list on failure (caller decides retry policy)
                         rather than raising, so a single failed poll doesn't crash
                         the producer's long-running loop.
        """
        params = {}
        if self._bbox:
            params = {
                "lamin": self._bbox["lamin"],
                "lamax": self._bbox["lamax"],
                "lomin": self._bbox["lomin"],
                "lomax": self._bbox["lomax"],
            }

        headers = {"Authorization": f"Bearer {self._auth.get_token()}"}

        try:
            response = requests.get(
                self._base_url, params=params, headers=headers, timeout=15
            )
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logger.error("OpenSky fetch failed: %s", e)
            return []

        payload = response.json()
        raw_states = payload.get("states") or []

        parsed = [self._parse_state_vector(s) for s in raw_states]
        logger.info("Fetched %d aircraft states.", len(parsed))
        return parsed

    @staticmethod
    def _parse_state_vector(raw: list) -> dict:
        """
        Map a single flat state-vector array to a named dict using STATE_VECTOR_FIELDS.

        Defensive against the 'category' field sometimes being absent (17 vs 18
        element arrays have both been observed from the live API) - pads with
        None rather than raising an IndexError.
        """
        padded = raw + [None] * (len(STATE_VECTOR_FIELDS) - len(raw))
        record = dict(zip(STATE_VECTOR_FIELDS, padded))

        # Clean up callsign: OpenSky often returns it with trailing whitespace
        if record.get("callsign"):
            record["callsign"] = record["callsign"].strip() or None

        return record
