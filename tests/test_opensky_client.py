"""
Tests for producer/opensky_client.py - focused on state vector parsing,
since that's the part most likely to silently break (wrong field order,
missing fields, whitespace in callsign).
"""

from producer.opensky_client import OpenSkyClient, STATE_VECTOR_FIELDS


def test_parses_full_18_field_state_vector():
    raw = [
        "abc123", "AIC101 ", "India", 1690000000, 1690000005,
        77.5, 12.9, 9500.0, False, 230.5, 180.0, 2.5,
        None, 9700.0, "1000", False, 0, "A320"
    ]
    parsed = OpenSkyClient._parse_state_vector(raw)

    assert parsed["icao24"] == "abc123"
    assert parsed["callsign"] == "AIC101"  # whitespace stripped
    assert parsed["latitude"] == 12.9
    assert parsed["longitude"] == 77.5
    assert parsed["on_ground"] is False
    assert parsed["vertical_rate"] == 2.5


def test_handles_shorter_array_missing_category_field():
    # Some live API responses omit the trailing 'category' field (17 items, not 18)
    raw = [
        "def456", None, "India", None, 1690000005,
        None, None, None, True, None, None, None,
        None, None, None, False, 1
    ]
    parsed = OpenSkyClient._parse_state_vector(raw)

    assert parsed["icao24"] == "def456"
    assert parsed["callsign"] is None
    assert parsed["category"] is None  # padded, not an IndexError
    assert len(parsed) == len(STATE_VECTOR_FIELDS)


def test_null_callsign_stays_none_not_empty_string():
    raw = ["abc123"] + [None] * 17
    parsed = OpenSkyClient._parse_state_vector(raw)
    assert parsed["callsign"] is None


def test_blank_whitespace_callsign_becomes_none():
    raw = ["abc123", "   "] + [None] * 16
    parsed = OpenSkyClient._parse_state_vector(raw)
    assert parsed["callsign"] is None
