#!/bin/bash
set -euo pipefail

echo "📡 Loading JAAS from secure RAM volume..."
while [ ! -f /run/secrets/schema_registry_jaas ]; do
    sleep 0.5
done

(
    echo "⏳ Waiting for Schema Registry engine to boot up on port 8081..."
    while ! timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/8081" 2>/dev/null; do
        sleep 1
    done

    sleep 3

    echo "🔒 Schema Registry engine online. Securely scrubbing JAAS source file from RAM..."
    rm -f /run/secrets/schema_registry_jaas
) &

exec /etc/confluent/docker/run
