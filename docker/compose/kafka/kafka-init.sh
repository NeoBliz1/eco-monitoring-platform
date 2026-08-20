#!/bin/bash
set -euo pipefail

echo "📡 Loading variables from secure RAM volume..."
while [ ! -f /run/secrets/kafka_scram_credentials.env ]; do
	sleep 1
done

source /run/secrets/kafka_scram_credentials.env
echo "Configuring Kafka..."
/etc/confluent/docker/configure
echo "Formatting storage for Node ${NODE_ID} using Vault tokens..."
kafka-storage format \
	--cluster-id "MkU3OEVBNTcwNTJENDM2Qk" \
	--config /etc/kafka/kafka.properties \
	--add-scram "SCRAM-SHA-512=[name=${KAFKA_ADMIN},password=${KAFKA_ADMIN_PASSWORD}]" \
	--add-scram "SCRAM-SHA-512=[name=${KAFKA_REGISTRY},password=${KAFKA_REGISTRY_PASSWORD}]" \
	--add-scram "SCRAM-SHA-512=[name=${KAFKA_VECTOR},password=${KAFKA_VECTOR_PASSWORD}]" \
	--add-scram "SCRAM-SHA-512=[name=${KAFKA_CLIENT},password=${KAFKA_CLIENT_PASSWORD}]" &&
	echo "✅ SUCCESS: KRaft storage formatting finalized!"


unset KAFKA_ADMIN KAFKA_ADMIN_PASSWORD KAFKA_REGISTRY KAFKA_REGISTRY_PASSWORD KAFKA_VECTOR KAFKA_VECTOR_PASSWORD KAFKA_CLIENT KAFKA_CLIENT_PASSWORD

# Background worker loop to scrub JAAS configuration as soon as the broker socket turns online
(
	while ! timeout 1 bash -c "cat < /dev/null > /dev/tcp/127.0.0.1/9092" 2>/dev/null; do
		sleep 1
	done
	echo "🔒 Port 9092 online. Securely scrubbing JAAS source file..."
	rm -f /run/secrets/kafka_server_jaas
	rm -f /run/secrets/kafka_scram_credentials.env
) &

exec /etc/confluent/docker/run
