#!/bin/sh
set -e

# get path of script
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
echo SCRIPT_DIR:$SCRIPT_DIR

# Get "global" variables
. "${SCRIPT_DIR}"/vars.sh

echo "Installing/starting NATS broker on ${SCOUTAM_HOST}..."
ssh "${SCOUTAM_HOST}" "
	if ! systemctl is-active --quiet nats-server 2>/dev/null; then
		if ! command -v nats-server >/dev/null 2>&1; then
			curl -sL https://github.com/nats-io/nats-server/releases/download/v${SCOUTAM_NATS_VERSION}/nats-server-v${SCOUTAM_NATS_VERSION}-linux-amd64.tar.gz \
				| sudo tar xz -C /tmp
			sudo cp /tmp/nats-server-v${SCOUTAM_NATS_VERSION}-linux-amd64/nats-server /usr/local/bin/
			sudo restorecon -v /usr/local/bin/nats-server 2>/dev/null || true
		fi
		printf '%s\n' \
			'[Unit]' \
			'Description=NATS Server' \
			'After=network.target' \
			'' \
			'[Service]' \
			\"ExecStart=/usr/local/bin/nats-server -m ${SCOUTAM_NATS_MONITOR_PORT}\" \
			'Restart=on-failure' \
			'User=nobody' \
			'' \
			'[Install]' \
			'WantedBy=multi-user.target' \
			| sudo tee /etc/systemd/system/nats-server.service >/dev/null
		sudo systemctl daemon-reload
		sudo systemctl enable --now nats-server
	fi
"

echo "Registering ScoutAM stage notification backend (${SCOUTAM_NOTIFY_NAME}) if not already present..."
ssh "${SCOUTAM_HOST}" "sudo samcli notify --list | grep -q '${SCOUTAM_NOTIFY_NAME}' || \
	sudo samcli notify nats -n ${SCOUTAM_NOTIFY_NAME} -s 127.0.0.1:${SCOUTAM_NATS_PORT} \
		-j ${SCOUTAM_NATS_DEFAULT_SUBJECT} --stage"

echo "Creating and archiving test data under ${SCOUTAM_TEST_DIR}..."
ssh "${SCOUTAM_HOST}" "sudo mkdir -p ${SCOUTAM_TEST_DIR} && \
	sudo dd if=/dev/urandom of=${SCOUTAM_TEST_DIR}/testfile1.bin bs=1M count=5 2>/dev/null && \
	sudo samcli file archive -i -W ${SCOUTAM_TEST_DIR}/testfile1.bin"

cat <<EOF

Test environment ready. Point conduit-fta's scoutam plugin config at:

plugins:
  scoutam:
    api-base-url: https://${SCOUTAM_HOST}:8080
    api-insecure-skip-verify: true
    nats-servers:
      - ${SCOUTAM_HOST}:${SCOUTAM_NATS_PORT}

Test file: ${SCOUTAM_TEST_DIR}/testfile1.bin (release it with
'samcli file release ${SCOUTAM_TEST_DIR}/testfile1.bin' to exercise the offline/stage path)
EOF
