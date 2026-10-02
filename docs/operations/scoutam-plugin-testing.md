# ScoutAM Plugin Testing

This page documents how the `scoutam` FTA plugin (`internal/fta/plugins/scoutam`) is tested against
a real Versity ScoutFS/ScoutAM deployment, the test cases that have been validated, and what's
needed to reproduce the setup.

For the full 3-VM lab (ScoutAM + conduit-fta + conduit-master, wired together end-to-end) see
[ScoutAM Test Lab Setup](scoutam-test-lab-setup.md).

## Why not just samcli?

`samcli` (and direct gRPC access to `scoutamd`) can't be assumed to be available on FTA hosts. The
plugin instead talks to ScoutAM's REST API, which is the same API `samcli`/the web dashboard use
under the hood. It uses:

- `POST /v1/security/login` - exchange an account/password for a bearer JWT
- `PUT /v1/request/batchstage` - request a batch of files be staged, with a caller-supplied NATS
  `topic` for completion notifications
- `GET /v1/filesystems` - resolve the `fsid` for the mounted ScoutFS filesystem a path belongs to

The plugin assumes the ScoutFS mount itself *is* directly reachable from the FTA host (so it can
walk directories locally with `filepath.WalkDir` to enumerate files), it just can't assume `samcli`
or ScoutAM's gRPC port are reachable/authorized.

The FTA's NFS mount need not use the same path as ScoutAM. Set the archive filesystem's
`custom-plugin-config.scoutam-api-root` to the server-side ScoutFS directory represented by its
`fta-root-fs-path`. The plugin then submits server-side absolute paths to ScoutAM. For
example, if `/client/nfs/scoutfs` mounts `server:/server/scoutfs/dir`, configure
`fta-root-fs-path: /client/nfs/scoutfs` and `scoutam-api-root: /server/scoutfs/dir`; a local path
`/client/nfs/scoutfs/a/b` is submitted as `/server/scoutfs/dir/a/b`.

## Stage notifications

ScoutAM can publish stage-completion events to a message bus (NATS or Kafka; see `samcli notify
nats --stage` / `samcli notify kafka --stage`). Each `BatchStageRequest` can carry its own `topic`,
which overrides the backend's default subject for that request's completion notifications ONLY
(see `versity/scoutam` `notify/nats.go`: `subject := m.Topic; if subject == "" { subject = n.subject }`).

The plugin uses this to give every `Setup()` call its own topic (`<prefix>.<random uuid>`, not the
transfer ID - a transfer can have many source/destination paths whose `Setup()` calls run
concurrently, so the topic can't be shared or notifications would cross-talk). Because the topic is
unique per call, every message received on it belongs to that call - no need to also filter by
inode.

Important behavior confirmed against a real ScoutAM instance:

- A stage request for a file that's already online still produces an immediate completion
  notification (`RequestTime == CompleteTime`, `Error: ""`). This means the plugin does **not**
  need to pre-check online/offline status before staging - it can always request staging for every
  file and rely on the notification either way, which saves an API round trip.
- Stage completion notifications for files that actually need to come off tape/S3 can take from a
  few seconds up to ~30s or more depending on backend media, well within the default
  `stage-timeout`.
- The `Filename` in a stage notification is the path **relative to the ScoutFS mount** (e.g.
  `conduit-test/pipeline/f1.bin` for `/mnt/scoutfs/conduit-test/pipeline/f1.bin`), not the
  absolute path submitted in the `batchstage` request. `Setup()` only uses it for error-message
  context; `Transfer()` keys pending files by their mount-relative path so each notification can be
  matched to the file to copy (absolute names are accepted too).

## Staging modes

The plugin can stage in one of two places, chosen by the archive filesystem's `plugin-stages`:

| Mode | Config | Behavior |
|---|---|---|
| Stage then copy | `setup-src: scoutam`, `transfer-src/dst: [rsync]` (or pftool) | `Setup()` stages every file of every source and waits for all notifications; only then does the transfer plugin start copying. `stage-timeout` bounds the whole wait. |
| Stage and copy in a pipeline | `setup-src: posix`, `transfer-src: [scoutam]`, and `scoutam` in the **destination** filesystem's `transfer-dst` | `Transfer()` walks the sources, submits `batchstage` requests, and copies each file with rsync as soon as its notification arrives (up to `batch-size` files per rsync run, one run at a time). `stage-timeout` is how long to go with no notification while files are still pending. |

In pipeline mode:

- Directories, symlinks and special files don't need staging and are copied straight away.
  Directory entries (and finally the source root itself) are copied last, so later file copies
  don't change their mtimes.
- Sources on filesystems that don't list `scoutam` in `transfer-src` are copied without staging, so
  mixed archive + non-archive sources still work.
- Destination layout follows posix validation: if the destination is an existing directory, each
  source lands inside it as `<dest>/<basename>`; otherwise the destination becomes the copy.
- rsync runs with `--links --perms --times --group --owner --specials`, the same options the rsync
  plugin uses. Directory sources are copied with `--files-from`.
- A stage error or a failed rsync batch doesn't stop the other files; all failures are reported
  together at the end, and the transfer fails.

## Test environment

The primary, currently-documented test environment is the 3-VM lab (`conduit-scoutam` +
`conduit-fta` + `conduit-master`) described in full in
[ScoutAM Test Lab Setup](scoutam-test-lab-setup.md) - that's the one to follow for a from-scratch
reproduction, and the one these `examples/scoutam/` scripts default to. To replicate the ScoutAM
side of it (independent of conduit-fta/conduit-master) you need:

1. A host with ScoutFS + ScoutAM installed and at least one filesystem mounted (`samcli fs`,
   `samcli status` to confirm).
2. A ScoutAM account (the lab uses `admin`/`versity`) that can authenticate against the REST API on
   port 8080 (HTTPS; self-signed cert in the test environment, hence
   `api-insecure-skip-verify: true`).
3. At least one archset whose `policy.path` regex matches wherever you put test data (see
   `/etc/scoutam/archset.yaml` and `samcli archset`). `conduit-scoutam`'s archset matches `.*`
   (everything under the mount), so test data just goes under `/mnt/scoutfs/conduit-test/`; earlier
   validation against a different, previously-existing ScoutAM host (`ben-el98-n0`) needed a
   `^s3/`-prefixed path to match that host's archset instead - check yours with `samcli archset`.
4. A NATS broker for stage notifications - `examples/scoutam/run.sh` installs `nats-server` as a
   plain binary + systemd service (no docker/podman required or assumed).
5. Network access from the FTA host (or your dev machine, for testing) to the ScoutAM host's port
   8080 (REST API) and the NATS port (4222 by default) - `firewalld` blocks these by default on the
   lab VMs (see the lab setup doc's firewall steps).

If you need to build a ScoutAM/ScoutFS cluster from scratch some other way, the
`versity/scoutam-tests` repository contains Vagrant + Ansible automation for exactly that (single-
or multi-node, with etcd/tape library/S3 backends). A virtualization host with `vagrant`,
`vagrant-libvirt`, and KVM/QEMU (such as a dedicated lab machine like `nuc`) is required; see that
repository's README for `EL_MAJOR_VER`, `SCOUTFS_INSTALL`, `SCOUTAM_INSTALL`, and related
environment variables. This path additionally needs the ScoutFS/ScoutAM RPMs and, for the base
Vagrant boxes, VPN access to Versity's internal Vagrant box server.

### Bringing up/tearing down the NATS + test data environment

[examples/scoutam/](https://github.com/lanl/conduit/tree/main/examples/scoutam) has scripts to
manage the NATS broker and test data on an existing ScoutAM host:

```sh
# defaults to SCOUTAM_HOST=conduit-scoutam; override any variable in vars.sh via the environment
export SCOUTAM_HOST=my-scoutam-host
./examples/scoutam/run.sh      # install/start nats-server, register it with ScoutAM, archive a test file
./examples/scoutam/destroy.sh  # stop nats-server and remove test data
```

These can be run from any machine with SSH access to `SCOUTAM_HOST` (directly, over VPN, or via a
jump host). No docker/podman is required on `SCOUTAM_HOST` - `nats-server` is installed directly.

## Test cases validated

All of the following were exercised directly against the `Setup()` function (SOURCE lease type) via
a throwaway harness that constructs a `ScoutAMPlugin`, sets viper config, and calls `Setup()`:

| Scenario | Expected behavior | Result |
|---|---|---|
| Single file, already online | No stage delay; immediate notification | ~0.1s, success |
| Single file, offline (released) | Waits for real stage completion | ~16s, success |
| Directory with a mix of online/offline files | Single batch request covering all files, waits for all notifications | ~16s, success |
| Directory, all files already online (re-run) | Fast path, no real staging work | ~90ms, success |
| Nonexistent path | Returns `ERROR_STAT_FAILED` with a descriptive error | Fails as expected |
| Batch of 20 offline files (41MB) submitted in one `batchstage` call | Submission returns immediately; real staging happens later, asynchronously | Submission: 14ms. Scheduler started the batch 16s later, finished 14s after that (see below) |
| Directory of 8 offline files, `batch-size: 3` (3 requests: 3, 3, 2) | Notifications for an earlier batch can arrive while later batches are still being submitted; none get lost | All 8 completed, success |

## Configuration reference

See the `scoutam` section of
[docs/configs/conduit-fta-full-reference-config.yaml](../configs/conduit-fta-full-reference-config.yaml)
for the full set of `plugins.scoutam` options (`api-base-url`, `api-username`, `api-password`,
`api-insecure-skip-verify`, `nats-servers`, `nats-stage-topic-prefix`, `stage-timeout`,
`batch-size`, `rsync-path`, `cancel-stage-on-timeout`).

## Cancelling stages on timeout

With `cancel-stage-on-timeout: true`, when `stage-timeout` fires (in either staging mode) the plugin
asks ScoutAM to drop every stage request it made that hasn't reported back yet, then fails as
before. The timeout error says how many requests were cancelled or couldn't be.

- It uses `PUT /v1/scheduler/stagecancelfiles` with `{"filenames": [<server-side paths>], "fsid"}`.
  `/v1/request/cancelbatchstage` is deprecated and always returns an error.
- The `/v1/scheduler/*` endpoints need an **operator** (or higher) ScoutAM role; `batchstage` works
  for any account.
- ScoutAM removes the file from waiting and pending stage jobs and clears its stage flags. A file
  whose stage job is already running can't be cancelled; it finishes staging and its notification
  is ignored.
- No notification is published for a cancelled file.
- ScoutAM stops a batch cancel at the first file it can't resolve (for example one deleted since
  the request), so a failed batch is retried one file at a time.
- Cancellation is per file, not per request: another transfer or user waiting on the same file
  loses its stage too. That's why this is off by default.
- It only covers the plugin's own timeout. A transfer aborted from outside (`conduit abort`, an
  error elsewhere, lease expiry) ends the `conduit-fta` process without giving the plugin a chance
  to cancel.

## Production considerations

- `Setup()` (`internal/fta/plugins/scoutam/setup.go`) walks the requested path with
  `walkFilePaths` and submits a `batchstage` API call every `batch-size` files (default 1000, one
  `client.batchStage(...)` call per chunk), rather than one request per file or one giant request
  for the whole directory. This lets ScoutAM start staging early files while later ones are still
  being enumerated, and keeps any single request body bounded regardless of directory size. Since
  conduit-fta already runs `Setup()`/`Teardown()` in parallel across the source/destination paths
  of a transfer, avoid adding any code path that loops single-file stage requests - concurrency
  across paths plus chunked batching within a path is what lets many stage requests be in flight at
  once without one blocking the others.
- Stage-completion notifications for an earlier batch can (and do) arrive before later batches for
  the same directory have even been submitted, since walking/submitting and waiting happen
  concurrently (the NATS subscription is created before any batch is sent). To avoid losing or
  blocking on those early messages, notifications are pushed into `stageEventQueue`
  (`internal/fta/plugins/scoutam/eventqueue.go`), an unbounded, mutex-guarded FIFO queue - `push`
  never blocks the NATS callback, and no capacity limit means no notification can be dropped
  regardless of how far ahead of the wait loop they arrive. Confirmed with a 3-request batch
  (`batch-size: 3` over 8 files: 3/3/2) - all 8 completions were correctly collected.
- **Confirmed the submission call is fully decoupled from completion**, so many files can be
  "in flight" (requested but not yet staged) at once: 20 offline files (41MB) submitted in a
  single `batchstage` call returned in **14ms**. ScoutAM's own scheduler didn't start moving that
  batch until **16s later** and took another **14s** to finish (`journalctl -u scoutam`: `start
  packet (S 10) ... entries: 20` -> `done packet (S 10)`, ~30s after submission). `Setup()`'s wait
  loop only blocks on the NATS notifications, never on the HTTP submission itself - so this holds
  regardless of batch size.
- The bearer token is fetched fresh on every `Setup()` call rather than cached/reused. This is
  simple and correct but adds one login round trip per call; if that overhead becomes measurable at
  scale, it's a candidate to cache (with re-login on expiry) at the plugin level.
