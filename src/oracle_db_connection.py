"""Shared Oracle connection helper for the Sanlam data pipeline.

Credentials are loaded from the local .env file. Do not hard-code them in
Python files or commit .env to source control.
"""

from __future__ import annotations

import os
from pathlib import Path

import oracledb
from dotenv import load_dotenv


ENV_FILE = Path(__file__).resolve().parent.parent / ".env"
REQUIRED_SETTINGS = ("ORACLE_USER", "ORACLE_PASSWORD", "ORACLE_DSN")


def get_oracle_connection() -> oracledb.Connection:
    """Return an open connection using credentials from this project's .env."""
    load_dotenv(ENV_FILE)

    missing = [setting for setting in REQUIRED_SETTINGS if not os.getenv(setting)]
    if missing:
        names = ", ".join(missing)
        raise RuntimeError(
            f"Missing {names} in {ENV_FILE.name}. "
            "Copy .env.example to .env and add your Oracle details."
        )

    return oracledb.connect(
        user=os.environ["ORACLE_USER"],
        password=os.environ["ORACLE_PASSWORD"],
        dsn=os.environ["ORACLE_DSN"],
    )


def test_oracle_connection() -> None:
    """Connect and print the Oracle server version; useful as a quick test."""
    with get_oracle_connection() as connection:
        print(f"Oracle connection successful: {connection.version}")


if __name__ == "__main__":
    test_oracle_connection()
