#!/bin/sh

# ssh target for the ScoutAM host. Expected to already have ScoutFS + ScoutAM
# installed and a filesystem mounted. Defaults to the conduit-scoutam lab VM
# (see docs/operations/scoutam-test-lab-setup.md).
export SCOUTAM_HOST="${SCOUTAM_HOST:-conduit-scoutam}"

# nats-server is installed as a plain binary + systemd service
# (see docs/operations/scoutam-test-lab-setup.md)
export SCOUTAM_NATS_VERSION="${SCOUTAM_NATS_VERSION:-2.15.0}"
export SCOUTAM_NATS_PORT="${SCOUTAM_NATS_PORT:-4222}"
export SCOUTAM_NATS_MONITOR_PORT="${SCOUTAM_NATS_MONITOR_PORT:-8222}"

# name ScoutAM registers this NATS stage-notification backend under (see `samcli notify nats`)
export SCOUTAM_NOTIFY_NAME="${SCOUTAM_NOTIFY_NAME:-conduit-stage}"
# default/fallback subject; conduit-fta always overrides this per-request via BatchStageRequest.topic
export SCOUTAM_NATS_DEFAULT_SUBJECT="${SCOUTAM_NATS_DEFAULT_SUBJECT:-scoutam.stage}"

# directory used for test data. IMPORTANT: this must fall under a path matched by one of the
# host's configured archsets (see `samcli archset`) or files placed here will never get archived.
export SCOUTAM_TEST_DIR="${SCOUTAM_TEST_DIR:-/mnt/scoutfs/conduit-test}"
