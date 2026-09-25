#!/bin/sh
set -e

# get path of script
SCRIPT_DIR=$( cd -- "$( dirname -- "$0" )" &> /dev/null && pwd )
echo SCRIPT_DIR:$SCRIPT_DIR

# Get "global" variables
. "${SCRIPT_DIR}"/vars.sh

echo "Stopping NATS broker on ${SCOUTAM_HOST}..."
ssh "${SCOUTAM_HOST}" "sudo systemctl disable --now nats-server 2>/dev/null || true"

echo "Removing test data under ${SCOUTAM_TEST_DIR}..."
ssh "${SCOUTAM_HOST}" "sudo rm -rf ${SCOUTAM_TEST_DIR}"

# The nats-server binary/service and the registered `samcli notify nats` backend are left in place
# since they're harmless when stopped (ScoutAM just logs a connection failure) and re-running
# run.sh is idempotent. Delete the notify backend manually with `samcli notify --delete <id>`
# (see `samcli notify --list`) if desired.
