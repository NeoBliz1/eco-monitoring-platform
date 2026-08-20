#!/bin/bash
set -euo pipefail

log_time() {
	date -u +"[%Y-%m-%d %H:%M:%S UTC]"
}

echo "$(log_time) 📡 Waiting for CLICKHOUSE_USER variable configuration..."

echo "📡 Loading variables from secure RAM volume..."

while :; do
	log_time
	if wget --spider -q http://clickhouse-db:8123/ping; then
		echo "✅ Success: ClickHouse is healthy and responding."
		break
	fi
	sleep 1
done

log_time

echo "📡 Bootstrapping ClickHouse administrative access..."
SECRETS_FILE="/run/secrets/clickhouse_creds.env"
# shellcheck source=/run/secrets/clickhouse_creds.env
source $SECRETS_FILE

echo "🔒 Default admin account locked via environment profile credentials. Initializing logging schema..."

clickhouse-client --host clickhouse-db \
	--user "${CLICKHOUSE_USER}" \
	--password "${CLICKHOUSE_PASSWORD}" \
	--multiquery <<EOF

  CREATE DATABASE IF NOT EXISTS ${CLICKHOUSE_DB};

  USE ${CLICKHOUSE_DB};

  CREATE TABLE IF NOT EXISTS application_logs (
      timestamp DateTime64(3, 'Europe/Moscow'),
      service_name LowCardinality(String),
      level LowCardinality(String),
      thread_name String,
      logger_name String,
      message String,
      trace_id String,
      span_id String,
      stack_trace String
  ) ENGINE = MergeTree()
  ORDER BY (service_name, level, timestamp)
  PARTITION BY toYYYYMM(timestamp)
  TTL timestamp + INTERVAL 7 DAY DELETE;
EOF

rm -f /run/secrets/clickhouse_creds.env

echo "✅ ClickHouse telemetry log database initialization finished successfully!"
