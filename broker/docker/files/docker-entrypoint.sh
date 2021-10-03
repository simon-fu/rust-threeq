#!/bin/ash

# from https://raw.githubusercontent.com/eclipse/mosquitto/master/docker/2.0-openssl/docker-entrypoint.sh

set -e

# Set permissions
# user="$(id -u)"
# if [ "$user" = '0' ]; then
# 	[ -d "/work" ] && chown -R root:root /work || true
# fi

exec "$@"


