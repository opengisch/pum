"""Utilities for handling PostgreSQL connection strings."""


def format_connection_string(pg_connection: str) -> str:
    """Format a connection string for use with psycopg.

    Detects whether the input is a service name or a full connection string.
    If it's a service name (simple identifier), wraps it as 'service=name'.
    If it's a connection string (contains '=' or '://'), returns as-is.

    Args:
        pg_connection: Either a service name or a PostgreSQL connection string

    Returns:
        A properly formatted connection string for psycopg

    Examples:
        >>> format_connection_string('myservice')
        'service=myservice'
        >>> format_connection_string('postgresql://user:pass@localhost/db')
        'postgresql://user:pass@localhost/db'
        >>> format_connection_string('host=localhost dbname=mydb')
        'host=localhost dbname=mydb'

    """
    # If it contains '=' or '://', it's already a connection string
    if "=" in pg_connection or "://" in pg_connection:
        return pg_connection
    # Otherwise, it's a service name
    return f"service={pg_connection}"
