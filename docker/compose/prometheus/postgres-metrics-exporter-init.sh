#!/bin/sh
set -eu

echo "📡 [Exporter] Sourcing Vault-managed configuration layers..."
while [ ! -f /run/secrets/pg_db_metrics_creds.env ]; do
	sleep 1
done

DB_USER=$(grep '^username:' /run/secrets/pg_db_metrics_creds.env | cut -d'"' -f2)
DB_PASS=$(grep '^password:' /run/secrets/pg_db_metrics_creds.env | cut -d'"' -f2)

if [ -z "$DB_USER" ] || [ -z "$DB_PASS" ]; then
    echo "❌ FATAL: Failed to resolve database tracking identities from Vault secret mapping."
    exit 1
fi

export DATA_SOURCE_URI="pg-db:5432/postgres?sslmode=disable"
export DATA_SOURCE_USER="$DB_USER"
export DATA_SOURCE_PASS="$DB_PASS"

PROM_AUTH_USER=$(grep 'username:' /run/secrets/promtool_http_config.yml | cut -d'"' -f2)
PROM_REAL_PASS=$(grep 'password:' /run/secrets/promtool_http_config.yml | cut -d'"' -f2)
if [ -z "$PROM_AUTH_USER" ] || [ -z "$PROM_REAL_PASS" ]; then
    echo "❌ FATAL: Unable to resolve web server basic authentication variables from yaml payload."
    exit 1
fi

cat <<EOF > /run/secrets/healthcheck.env
export SCRAPER_USER="$PROM_AUTH_USER"
export SCRAPER_PASS="$PROM_REAL_PASS"
EOF


echo "✅ [Exporter] All credential variables successfully mapped into memory properties."
echo "🚀 Igniting Prometheus PostgreSQL Exporter engine daemon..."

exec /bin/postgres_exporter --web.config.file=/run/secrets/prometheus_web_config
