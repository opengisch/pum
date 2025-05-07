#!/bin/bash

# This script runs a PostGIS Docker container

# Set default values for environment variables
POSTGRES_USER=${POSTGRES_USER:-postgres}
POSTGRES_PASSWORD=${POSTGRES_PASSWORD:-1234}
POSTGRES_DB=${POSTGRES_DB:-pum_test}
POSTGIS_PORT=${POSTGIS_PORT:-5432}
CONTAINER_NAME=${CONTAINER_NAME:-postgis-container}

# Run the PostGIS Docker container
docker run --name "$CONTAINER_NAME" \
  -e POSTGRES_USER="$POSTGRES_USER" \
  -e POSTGRES_PASSWORD="$POSTGRES_PASSWORD" \
  -e POSTGRES_DB="$POSTGRES_DB" \
  -p "$POSTGIS_PORT":5432 \
  -d postgis/postgis

# Print container status
echo "PostGIS container '$CONTAINER_NAME' is running on port $POSTGIS_PORT."
