#!/bin/bash
# scripts/setup_db.sh

set -e  # Stop script if any command fails

echo "Loading environment variables..."
source /opt/nexus_core/.env

echo "Waiting for TimescaleDB to be ready..."
until docker exec nexus_timescaledb pg_isready -U ${POSTGRES_USER} -d postgres; do
  sleep 2
done

echo "Database is ready. Ensuring nexus_core database exists..."
docker exec nexus_timescaledb psql -U ${POSTGRES_USER} -d postgres -tc "SELECT 1 FROM pg_database WHERE datname = '${POSTGRES_DB}'" | grep -q 1 || \
docker exec nexus_timescaledb psql -U ${POSTGRES_USER} -d postgres -c "CREATE DATABASE ${POSTGRES_DB};"

echo "Granting privileges..."
docker exec nexus_timescaledb psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} -c "GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_DB} TO ${POSTGRES_USER};"

echo "Applying schema.sql..."
docker exec -i nexus_timescaledb psql -U ${POSTGRES_USER} -d ${POSTGRES_DB} < /opt/nexus_core/database/schema.sql

echo "Database setup completed successfully!"