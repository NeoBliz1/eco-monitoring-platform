#!/bin/sh
set -eu

VECTOR_CREDS="/run/secrets/vector_creds.env"

echo "⏳ Awaiting single-use Vault agent sidecar to write credentials file..."

while [ ! -f "$VECTOR_CREDS" ]; do
	sleep 0.5
done

echo "🚀 Booting Vector telemetry routing sandbox with dynamic credentials..."
MEM_SECRETS=$(tr -d '\r' < "$VECTOR_CREDS" | xargs)
rm -f "$VECTOR_CREDS"
# shellcheck disable=SC2046
# shellcheck disable=SC2086
exec env $MEM_SECRETS vector --config /etc/vector/vector.yaml