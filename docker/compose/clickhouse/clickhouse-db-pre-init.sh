#!/bin/bash
set -euo pipefail

SECRETS_FILE="/run/secrets/clickhouse_creds.env"
CONFIG_FILE="/etc/clickhouse-server/users.xml"

if [ -f "$SECRETS_FILE" ]; then
    set +u
    set -a
		# shellcheck source=/run/secrets/clickhouse_creds.env
    source "$SECRETS_FILE"
    set +a
    set -u
    echo "✅ Env variables loaded from secrets."
else
    echo "❌ Error: Secrets file not found at $SECRETS_FILE"
    exit 1
fi

if [ -z "${CLICKHOUSE_USER:-}" ] || [ -z "${CLICKHOUSE_SHA256_PASSWORD:-}" ] || [ -z "${CLICKHOUSE_SHA256_DEFAULT_PASSWORD:-}" ]; then
    echo "❌ Error: Missing required credentials (CLICKHOUSE_USER, CLICKHOUSE_SHA256_PASSWORD, or CLICKHOUSE_SHA256_DEFAULT_PASSWORD) in secrets file."
    exit 1
fi

if [ ! -f "$CONFIG_FILE" ]; then
    echo "❌ Error: Core configuration file not found at $CONFIG_FILE"
    exit 1
fi

cat << EOF > "$CONFIG_FILE"
<clickhouse>
    <users>
        <default>
            <password_sha256_hex>${CLICKHOUSE_SHA256_DEFAULT_PASSWORD}</password_sha256_hex>
            <access_management>0</access_management>
            <named_collection_control>0</named_collection_control>
            <networks>
                <ip>127.0.0.1</ip>
            </networks>
            <profile>default</profile>
            <quota>default</quota>
            <grants>
                <grant>REVOKE ALL ON *.*</grant>
                <grant>REVOKE SET DEFINER ON *</grant>
                <grant>REVOKE TABLE ENGINE ON *</grant>
                <grant>REVOKE CHECK, SHOW, SELECT, INSERT, ALTER, CREATE, DROP, UNDROP TABLE, TRUNCATE, OPTIMIZE, BACKUP, KILL QUERY, KILL TRANSACTION, MOVE PARTITION BETWEEN SHARDS, SYSTEM, dictGet, displaySecretsInShowAndSelect, INTROSPECTION, CLUSTER, FILE, URL, REMOTE, MONGO, REDIS, MYSQL, POSTGRES, SQLITE, ODBC, JDBC, HDFS, S3, HIVE, AZURE, KAFKA, NATS, RABBITMQ, YTSAURUS, ARROW FLIGHT, SOURCES ON *.*</grant>
            </grants>
        </default>
        <${CLICKHOUSE_USER}>
            <password_sha256_hex>${CLICKHOUSE_SHA256_PASSWORD}</password_sha256_hex>
						<access_management>0</access_management>
            <networks>
                <ip>0.0.0.0/0</ip>
            </networks>
            <grants>
                <grant>GRANT ALL ON *.* WITH GRANT OPTION</grant>
            </grants>
        </${CLICKHOUSE_USER}>
    </users>

    <profiles>
        <default></default>
        <readonly>
            <readonly>1</readonly>
        </readonly>
    </profiles>

    <quotas>
        <default>
            <interval>
                <duration>3600</duration>
                <queries>0</queries>
                <errors>0</errors>
                <result_rows>0</result_rows>
                <read_rows>0</read_rows>
                <execution_time>0</execution_time>
            </interval>
        </default>
    </quotas>
</clickhouse>
EOF

echo "✅ Successfully updated users.xml with custom admin credentials and secure default user parameters."

unset CLICKHOUSE_DB
unset CLICKHOUSE_USER
unset CLICKHOUSE_PASSWORD
unset CLICKHOUSE_SHA256_PASSWORD
unset CLICKHOUSE_DEFAULT_PASSWORD
unset CLICKHOUSE_SHA256_DEFAULT_PASSWORD

exec /entrypoint.sh clickhouse-server --config-file=/etc/clickhouse-server/config.xml
