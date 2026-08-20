#!/bin/bash
set -euo pipefail

GR_CREDS="/run/secrets/grafana_creds.env"
DB_FILE="/var/lib/grafana/grafana.db"

echo "📡 Waiting for secure dynamic vault configurations to write to memory..."
while [ ! -f /run/secrets/grafana_creds.env ]; do sleep 1; done

if [ ! -f "$DB_FILE" ]; then
	echo "📥 Loading unprivileged environment contexts into active shell memory..."
	set -a
	# shellcheck disable=SC1090
	source "$GR_CREDS"
	rm -f "$GR_CREDS"

	set +a
	echo "🔒 Vaporizing plain-text token file from named volume memory layers..."
	echo "🚀 Initializing fresh Grafana secure admin profile database..."
	exec grafana server \
		--homepath=/usr/share/grafana \
		--config=/etc/grafana/grafana.ini \
		cfg:security.admin_user="$GF_SECURITY_ADMIN_USER" \
		cfg:security.admin_password="$GF_SECURITY_ADMIN_PASSWORD" \
		cfg:default.paths.data=/var/lib/grafana \
		cfg:default.paths.logs=/tmp \
		cfg:default.paths.plugins=/var/lib/grafana/plugins \
		cfg:default.paths.provisioning=/etc/grafana/provisioning \
		cfg:users.allow_sign_up="false"
fi

rm -f "$GR_CREDS"

echo "🚀 Booting Grafana visualization dashboard sandbox..."
exec grafana server \
	--homepath=/usr/share/grafana \
	--config=/etc/grafana/grafana.ini \
	cfg:default.paths.data=/var/lib/grafana \
	cfg:default.paths.logs=/tmp \
	cfg:default.paths.plugins=/var/lib/grafana/plugins \
	cfg:default.paths.provisioning=/etc/grafana/provisioning \
	cfg:users.allow_sign_up="false"
