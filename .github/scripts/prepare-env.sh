#!/usr/bin/env bash
# Creates the git-ignored env files that the build and tests read, using the committed examples.
set -euo pipefail

cp .env.example .env
cp dev_creds.env.example dev_creds.env
cp docker/.env.example docker/.env
cp docker/compose/vault/.env.example docker/compose/vault/.env
[ -f gateway-service/.env.example ] && cp gateway-service/.env.example gateway-service/.env

# Containers must reach the host by its network IP, not localhost
HOST_IP=$(hostname -I | awk '{print $1}')
sed -i "s/^HOST_IP=.*/HOST_IP=${HOST_IP}/" docker/.env
echo "HOST_IP=${HOST_IP}"
