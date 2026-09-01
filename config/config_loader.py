"""
config_loader.py

Single source of truth for reading config.yaml. Every module in this project
(producer, spark_jobs, tests) should import `load_config()` from here rather
than reading config.yaml directly or hardcoding values.

Usage:
    from config.config_loader import load_config
    cfg = load_config()
    print(cfg["kafka"]["bootstrap_servers"])
"""

import os
import yaml
from functools import lru_cache

_CONFIG_FILENAME = "config.yaml"


@lru_cache(maxsize=1)
def load_config(config_path: str = None) -> dict:
    """
    Load and cache the project configuration.

    Cached with lru_cache so repeated calls across modules don't re-read
    the file from disk on every access - this runs in a tight polling loop
    (the producer) so avoiding repeated file I/O matters.

    Args:
        config_path: optional explicit path to config.yaml. If not given,
                      resolves relative to this file's location so it works
                      regardless of the current working directory the script
                      is launched from.

    Returns:
        dict: parsed configuration
    """
    if config_path is None:
        this_dir = os.path.dirname(os.path.abspath(__file__))
        config_path = os.path.join(this_dir, _CONFIG_FILENAME)

    if not os.path.exists(config_path):
        raise FileNotFoundError(
            f"Config file not found at {config_path}. "
            "Did you rename or move config/config.yaml?"
        )

    with open(config_path, "r") as f:
        config = yaml.safe_load(f)

    return config


def get_env_credential(env_var_name: str) -> str:
    """
    Fetch a credential from environment variables, failing loudly if missing.

    Kept as a small helper (rather than inlining os.environ[...] everywhere)
    so we get one consistent, clear error message across the whole project
    when someone forgets to set their .env / export the variable.
    """
    value = os.environ.get(env_var_name)
    if not value:
        raise EnvironmentError(
            f"Required environment variable '{env_var_name}' is not set. "
            f"Set it before running (e.g. `export {env_var_name}=your_value`), "
            "or add it to a local .env file (see .env.example)."
        )
    return value
