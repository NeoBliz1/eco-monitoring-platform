#!/bin/bash
set -euo pipefail

log_time() {
  date -u +"[%Y-%m-%d %H:%M:%S UTC]"
}

CREDENTIALS_FILE="/run/secrets/kafka_scram_credentials.env"

echo "$(log_time) 📡 Waiting for KAFKA_ADMIN variable configuration..."

while :; do
    if [ -f "$CREDENTIALS_FILE" ]; then
        # Temporarily turn off exit-on-error to safely check variable injection
        set +e
        # shellcheck disable=SC1090
        source "$CREDENTIALS_FILE" 2>/dev/null
        set -e
    fi

    if [ -n "${KAFKA_ADMIN:-}" ]; then
        break
    fi

    sleep 0.5
done

echo "$(log_time) ✅ KAFKA_ADMIN detected. Proceeding..."

source /run/secrets/kafka_scram_credentials.env

echo "Creating runtime properties for cluster admin authentication..."
cat <<EOF > /tmp/admin.properties
security.protocol=SASL_PLAINTEXT
sasl.mechanism=SCRAM-SHA-512
sasl.jaas.config=org.apache.kafka.common.security.scram.ScramLoginModule required username="${KAFKA_ADMIN}" password="${KAFKA_ADMIN_PASSWORD}";
EOF

rm -f /run/secrets/kafka_server_jaas
rm -f /run/secrets/kafka_scram_credentials.env

echo "Waiting 5 seconds for KRaft node elections to finalize..."
sleep 5

echo "Validating broker connection and provisioning custom business topics..."
kafka-topics --create --if-not-exists \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --topic ${KAFKA_WEATHER_LIVE_TOPIC} \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=604800000

kafka-topics --create --if-not-exists \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --topic ${KAFKA_WEATHER_RAW_TOPIC} \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=delete \
  --config retention.ms=86400000

kafka-topics --create --if-not-exists \
  --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --topic ${KAFKA_WEATHER_HISTORY_TOPIC} \
  --partitions 6 \
  --replication-factor 3 \
  --config min.insync.replicas=2 \
  --config cleanup.policy=compact \
  --config retention.ms=-1

echo "🔍 VERIFICATION: Fetching live cluster structure for confirmation..."
kafka-topics --bootstrap-server kafka-1:9092 \
  --command-config /tmp/admin.properties \
  --describe \
  --topic ${KAFKA_WEATHER_LIVE_TOPIC},${KAFKA_WEATHER_RAW_TOPIC},${KAFKA_WEATHER_HISTORY_TOPIC}

echo "📡 Announcing Kafka Broker 1 to Consul..."
curl --request PUT \
--url ${CONSUL_URL} \
--header 'Content-Type: application/json' \
--data '{
  "ID": "kafka-broker-1",
  "Name": "kafka",
  "Tags": ["secure", "node-1"],
  "Address": "kafka-1",
  "Port": 9094,
  "Check": {
    "TCP": "kafka-1:9092", 
    "Interval": "10s",
    "Timeout": "5s"
  }
}'

echo "📡 Announcing Kafka Broker 2 to Consul..."
curl --request PUT \
  --url ${CONSUL_URL} \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "kafka-broker-2",
    "Name": "kafka",
    "Tags": ["secure", "node-2"],
    "Address": "kafka-2",
    "Port": 9194,
    "Check": {
      "TCP": "kafka-2:9092",
      "Interval": "10s",
      "Timeout": "5s"
    }
}'

echo "📡 Announcing Kafka Broker 3 to Consul..."
curl --request PUT \
  --url ${CONSUL_URL} \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "kafka-broker-3",
    "Name": "kafka",
    "Tags": ["secure", "node-3"],
    "Address": "kafka-3",
    "Port": 9294,
    "Check": {
      "TCP": "kafka-3:9092",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 Announcing Confluent Schema Registry to Consul Discovery catalog..."
curl --request PUT \
  --url ${CONSUL_URL} \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "schema-registry-1",
    "Name": "schema-registry",
    "Tags": ["avro", "serialization-core"],
    "Address": "schema-registry",
    "Port": 8085,
    "Check": {
      "TCP": "schema-registry:8081",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 Announcing Vector Ingestion Sidecar to Consul Discovery catalog..."
curl --request PUT \
  --url ${CONSUL_URL} \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "vector-sidecar-1",
    "Name": "vector-sidecar",
    "Tags": ["ingestion", "sidecar", "protobuf-relay"],
    "Address": "vector-sidecar",
    "Port": 6000,
    "Check": {
      "TCP": "vector-sidecar:6000",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 Announcing Redis Cache to Consul Discovery catalog..."
curl --request PUT \
  --url ${CONSUL_URL} \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "redis-cache-1",
    "Name": "redis-cache",
    "Tags": ["cache", "reactive-state", "rocksdb-mirror"],
    "Address": "redis-cache",
    "Port": 6379,
    "Check": {
      "TCP": "redis-cache:6379",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "📡 Announcing PostgreSQL Matrix Instance to Consul Discovery catalog..."
curl --request PUT \
  --url "${CONSUL_URL}" \
  --header 'Content-Type: application/json' \
  --data '{
    "ID": "postgres-matrix-1",
    "Name": "pg-db",
    "Tags": ["timeseries-optimized", "history-backend", "acid-storage"],
    "Address": "pg-db",
    "Port": 5432,
    "Check": {
      "TCP": "pg-db:5432",
      "Interval": "10s",
      "Timeout": "5s"
    }
  }'

echo "✅ Infrastructure discovery sync sequence complete."
