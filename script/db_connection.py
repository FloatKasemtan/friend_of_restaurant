import os
import sys
from decimal import Decimal, InvalidOperation
from pathlib import Path

import psycopg2

def _get_env(name: str, default: str | None = None) -> str:
    """Get environment variable with optional default."""
    value = os.environ.get(name, default)
    if value is None:
        print(f"ERROR: Missing environment variable {name}")
        sys.exit(1)
    return value


def get_connection():
    """
    Get database connection using environment variables.
    
    Required environment variables:
    - POSTGRES_USER (or defaults to admin)
    - POSTGRES_PASSWORD (or defaults to V9DscuMN22EobZ_3)
    - POSTGRES_DB (or defaults to meat_the_potato)
    
    Optional environment variables:
    - POSTGRES_HOST (default: localhost)
    - POSTGRES_PORT (default: 5432)
    """
    user = os.environ.get("POSTGRES_USER")
    password = os.environ.get("POSTGRES_PASSWORD")
    database = os.environ.get("POSTGRES_DB")
    host = os.environ.get("POSTGRES_HOST", "localhost")
    port = int(os.environ.get("POSTGRES_PORT", "5432"))
    
    conn = psycopg2.connect(
        dbname=database, 
        user=user, 
        password=password, 
        host=host, 
        port=port
    )
    
    # Set the default schema to dbo
    with conn.cursor() as cur:
        cur.execute("SET search_path TO dbo, public;")
    conn.commit()
    
    return conn


def parse_decimal(value: str | None, default: Decimal = Decimal("0")) -> Decimal:
    """
    Parse string to Decimal, handling common formatting.
    
    Args:
        value: String value to parse
        default: Default value if parsing fails
        
    Returns:
        Decimal value
    """
    if value is None or value == "":
        return default
    try:
        # Remove common formatting artifacts like commas
        cleaned = str(value).strip().replace(",", "")
        return Decimal(cleaned) if cleaned != "" else default
    except (InvalidOperation, AttributeError):
        return default
