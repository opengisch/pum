# Getting Started

## Prerequisites
- Python 3.10 or newer
- PostgreSQL server (tested with 12+)
- Database connection:
    - Either configure pg_service.conf for your database connections (see [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-pgservice.html))
    - Or use a PostgreSQL connection string directly

## Installation

```bash
pip install pum
```

## Installing the datamodel

Once the SQL migrations are organized in [changelogs](changelogs.md), the datamodel can be installed from the command line:

```sh
pum -p {pg_connection} install
```

`pg_connection` can be either:
- A service name defined in pg_service.conf: `pum -p mydb install`
- A PostgreSQL connection string: `pum -p "postgresql://user:password@localhost/mydb" install`
- Connection parameters: `pum -p "host=localhost dbname=mydb user=postgres" install`
