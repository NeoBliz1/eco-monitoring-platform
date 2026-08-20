#!/bin/sh
set -eu

PERSISTENCE_DB="/data/dump.rdb"
PASS_FILE="/run/secrets/redis_pass_only"

if [ -f "$PERSISTENCE_DB" ]; then
	echo "🔄 Redis persistent data storage located. This is a container restart (Second Boot)."
	echo "🔒 Skipping file parsing loop. Initializing engine natively using secure memory-locked parameters..."

	exec redis-server --dir /data --dbfilename dump.rdb
else
	echo "📡 No persistence database found. This is a Fresh Installation (First Boot)."
	echo "⏳ Awaiting single-use Vault agent sidecar to write credentials file..."
	while [ ! -f "$PASS_FILE" ]; do
		sleep 0.5
	done

	LOCAL_PASS=$(tr -d ' \n\r' < "$PASS_FILE")

	echo "🔒 Vaporizing plaintext configuration files from named volume memory layers..."
	rm -f "$PASS_FILE" 2>/dev/null || true

	echo "🚀 Booting Redis caching master engine via secure inline process injection..."
	exec redis-server \
		--bind 0.0.0.0 \
		--port 6379 \
		--requirepass "$LOCAL_PASS" \
		--dir /data \
		--dbfilename dump.rdb \
		--protected-mode yes \
		--appendonly yes \
		--appendfsync everysec \
		--no-appendfsync-on-rewrite yes \
		--auto-aof-rewrite-percentage 100 \
		--auto-aof-rewrite-min-size 64mb
fi
