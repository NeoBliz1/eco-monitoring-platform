#!/bin/bash
set -euo pipefail

echo "📡 Loading dynamic database credentials directly from Vault RAM volume..."
while [ ! -f /run/secrets/postgres_creds.env ]; do
	sleep 1
done

source /run/secrets/postgres_creds.env

echo "⚡ Initialize the cluster storage layout if it does not exist..."
if [ ! -d "${PGDATA}/base" ]; then
	echo "Initializing fresh PostgreSQL database storage layout..."
	mkdir -p "${PGDATA}"
	chown -R postgres:postgres /var/lib/postgresql/data

	echo "$POSTGRES_PASSWORD" >/tmp/pg_pass
	chown postgres:postgres /tmp/pg_pass

	gosu postgres initdb --username="postgres" --pwfile=/tmp/pg_pass --auth=scram-sha-256
	rm -f /tmp/pg_pass

	echo "host all all 0.0.0.0/0 scram-sha-256" >>"${PGDATA}/pg_hba.conf"
	echo "host all all ::/0 scram-sha-256" >>"${PGDATA}/pg_hba.conf"
	echo "⚡ Cluster storage layout does not exist, it was created"
fi

echo "⚡ Boot the engine safely under unprivileged execution rules..."
gosu postgres postgres -h '*' -c shared_buffers=512MB -c synchronous_commit=off -c max_connections=200 &

echo "⏳ Waiting for PostgreSQL database core to become healthy..."
export PGPASSWORD=$POSTGRES_PASSWORD
until psql -U postgres -d postgres -c 'SELECT 1;' >/dev/null 2>&1; do
	sleep 1
done

echo "⚡ Configuring application-tier database security and storage layers..."
DB_EXISTS=$(psql -h 127.0.0.1 -U postgres -d postgres -tAc "SELECT 1 FROM pg_database WHERE datname='${POSTGRES_DB}'")
if [ "$DB_EXISTS" != "1" ]; then
	echo "Database ${POSTGRES_DB} is missing. Provisioning target storage catalog..."
	psql -h 127.0.0.1 -U postgres -d postgres -c "CREATE DATABASE ${POSTGRES_DB};"
fi

echo "⚡ Provisioning dedicated schema architecture within ${POSTGRES_DB}..."
psql -h 127.0.0.1 -U postgres -d "${POSTGRES_DB}" -c "CREATE SCHEMA IF NOT EXISTS ${POSTGRES_DB_SCHEMA} AUTHORIZATION postgres;"

echo "⚡ Create User Role and assign dynamic database privileges..."
psql -h 127.0.0.1 -U postgres -d "${POSTGRES_DB}" <<-EOF
	  DO '
	  BEGIN
	    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = ''${ECO_USER_NAME}'') THEN
	      EXECUTE format(''CREATE USER %I WITH PASSWORD %L'', ''${ECO_USER_NAME}'', ''${ECO_USER_PASSWORD}'');
	      EXECUTE format(''GRANT ALL PRIVILEGES ON DATABASE %I TO %I'', current_database(), ''${ECO_USER_NAME}'');
	      RAISE NOTICE ''✅ SUCCESS: Role % created and privileges granted!'', ''${ECO_USER_NAME}'';
	      EXECUTE format(''GRANT ALL PRIVILEGES ON SCHEMA ${POSTGRES_DB_SCHEMA} TO %I'', ''${ECO_USER_NAME}'');
	      EXECUTE format(''ALTER DEFAULT PRIVILEGES IN SCHEMA ${POSTGRES_DB_SCHEMA} GRANT ALL PRIVILEGES ON TABLES TO %I'', ''${ECO_USER_NAME}'');
	      EXECUTE format(''ALTER DEFAULT PRIVILEGES IN SCHEMA ${POSTGRES_DB_SCHEMA} GRANT ALL PRIVILEGES ON SEQUENCES TO %I'', ''${ECO_USER_NAME}'');
	    ELSE
	      EXECUTE format(''ALTER USER %I WITH PASSWORD %L'', ''${ECO_USER_NAME}'', ''${ECO_USER_PASSWORD}'');
	      RAISE NOTICE ''Role % already exists. Password synchronized.'', ''${ECO_USER_NAME}'';
	    END IF;
	    IF NOT EXISTS (SELECT FROM pg_catalog.pg_roles WHERE rolname = ''${EXPORTER_DB_USERNAME}'') THEN
	      EXECUTE format(''CREATE USER %I WITH PASSWORD %L'', ''${EXPORTER_DB_USERNAME}'', ''${EXPORTER_DB_PASSWORD}'');
	      EXECUTE format(''GRANT pg_monitor TO %I'', ''${EXPORTER_DB_USERNAME}'');
	      RAISE NOTICE ''✅ SUCCESS: Restricted database monitoring user [%] created!'', ''${EXPORTER_DB_USERNAME}'';
	    ELSE
	      EXECUTE format(''ALTER USER %I WITH PASSWORD %L'', ''${EXPORTER_DB_USERNAME}'', ''${EXPORTER_DB_PASSWORD}'');
	      RAISE NOTICE ''Monitoring role [%] already exists. Password synchronized.'', ''${EXPORTER_DB_USERNAME}'';
	    END IF;
	  END
	  ';
EOF

echo "🛑 Gracefully shutting down temporary background database cluster..."
gosu postgres pg_ctl -D "${PGDATA}" stop -m fast

echo "🛡️ Database schema formatted securely. Aggressively scrubbing RAM workspace..."
rm -f /run/secrets/postgres_creds.env

unset POSTGRES_USER POSTGRES_PASSWORD ECO_USER_NAME ECO_USER_PASSWORD PGPASSWORD DB_EXISTS

chown -R 999:999 /var/lib/postgresql/data
chmod 700 "$PGDATA"

echo "✅ Initialization task complete. Exiting container wrapper cleanly."
