#!/bin/sh
set -eu

VAULT_SECRETS_FILE="/run/secrets/vector_creds.env"

echo "📡 No active state found. This is a Fresh Installation (First Boot)."
echo "⏳ Awaiting single-use Vault agent sidecar to write credentials file..."

while [ ! -f "$VAULT_SECRETS_FILE" ]; do
	sleep 0.5
done

echo "📥 Loading unprivileged environment contexts into active shell memory..."
while IFS='=' read -r key value; do
	[ -z "$key" ] && continue
	clean_value=$(echo "$value" | tr -d ' \r\n"')
	export "$key"="$clean_value"
done <"$VAULT_SECRETS_FILE"

echo "🔒 Vaporizing plaintext configuration files from named volume memory layers..."
rm -f "$VAULT_SECRETS_FILE"

echo "🚀 Booting Vector telemetry routing sandbox with dynamic credentials..."
exec vector --config /etc/vector/vector.yaml
