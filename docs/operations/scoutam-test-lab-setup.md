# ScoutAM Test Lab Setup (nuc VMs)

This is a step-by-step walkthrough of the 3-VM conduit + ScoutAM test lab, kept up to date so it
can be replicated (or walked through with a customer) later. It complements
[ScoutAM Plugin Testing](scoutam-plugin-testing.md), which covers the `scoutam` FTA plugin's design
and the earlier single-host (`ben-el98-n0`) validation.

## Topology

Three VMs on a libvirt/vagrant host (referred to here as `nuc`), all on the same
`192.168.121.0/24` NAT network:

| VM | Role | IP |
|---|---|---|
| `conduit-scoutam` | ScoutFS + ScoutAM, NFS server, NATS stage-notification broker | 192.168.121.220 |
| `conduit-fta` | conduit-runner + conduit-fta, NFS client, `/scratch` (the "online" filesystem) | 192.168.121.129 |
| `conduit-master` | etcd, rqlite, conduit-server, conduit CLI | 192.168.121.144 |

All three are `generic/rocky9` (Rocky Linux 9.3) boxes. VM disk images live in a dedicated libvirt
storage pool at `/mnt/xfs/images` (not the root filesystem). `conduit-scoutam` additionally has two
extra 64G virtio disks (`vdb`/`vdc`) for ScoutFS metadata/data.

### Provisioning the VMs (on `nuc`)

1. Libvirt needs a working default NAT network and a storage pool pointed at `/mnt/xfs/images`
   (neither existed by default on a fresh host):
   ```sh
   virsh -c qemu:///system net-define /usr/share/libvirt/networks/default.xml
   virsh -c qemu:///system net-start default && virsh -c qemu:///system net-autostart default
   virsh -c qemu:///system pool-define-as images dir --target /mnt/xfs/images
   virsh -c qemu:///system pool-build images && virsh -c qemu:///system pool-start images
   virsh -c qemu:///system pool-autostart images
   ```
   Note `virsh` without `-c qemu:///system` defaults to the unrelated, empty `qemu:///session`.
2. `vagrant box add --provider libvirt generic/rocky9` (the `rockylinux/9` Vagrant Cloud box 404s;
   `generic/rocky9` is the working equivalent of the `generic/rocky8` box already used elsewhere).
3. Each VM is its own Vagrant project (`~/conduit-vms/{scoutam,fta,master}/Vagrantfile`) with
   `config.vm.provider(:libvirt) { |lv| lv.storage_pool_name = "images" }`. The scoutam VM's
   Vagrantfile additionally adds two `lv.storage :file, size: 64, type: "qcow2", pool: "images"`
   entries.
4. `vagrant up --provider=libvirt` in each directory. SSH access from a dev machine: copy each
   VM's `.vagrant/machines/default/libvirt/private_key` locally and add `Host` entries to
   `~/.ssh/config` (`ProxyJump nuc`, `user vagrant`).

## 1. ScoutFS / ScoutAM (`conduit-scoutam`)

This was configured directly by hand (not automated): ScoutFS formatted across `/dev/vdb`
(metadata) + `/dev/vdc` (data), mounted at `/mnt/scoutfs`, ScoutAM installed with a default archset:

```
Policies:
  path=.*
Copies:
  Number: 1
  Target: s3pool-a   (S3 endpoint)
```

i.e. every file placed under `/mnt/scoutfs` gets one archive copy to S3.

### NATS stage-notification broker

The `scoutam` FTA plugin needs ScoutAM to publish stage-completion events to NATS (see
[ScoutAM Plugin Testing](scoutam-plugin-testing.md) for why). This VM had no docker/podman, so NATS
was installed as a plain binary:

```sh
curl -sL https://github.com/nats-io/nats-server/releases/download/v2.15.0/nats-server-v2.15.0-linux-amd64.tar.gz \
  | tar xz -C /tmp
sudo cp /tmp/nats-server-*/nats-server /usr/local/bin/
```

Systemd unit (`/etc/systemd/system/nats-server.service`):

```ini
[Unit]
Description=NATS Server
After=network.target

[Service]
ExecStart=/usr/local/bin/nats-server -m 8222
Restart=on-failure
User=nobody

[Install]
WantedBy=multi-user.target
```

`sudo systemctl daemon-reload && sudo systemctl enable --now nats-server`

Register it with ScoutAM (idempotent - `samcli notify --list` first to check it isn't already
there):

```sh
sudo samcli notify nats -n conduit-stage -s 127.0.0.1:4222 -j scoutam.stage --stage
```

(`[examples/scoutam/run.sh](https://github.com/lanl/conduit/tree/main/examples/scoutam)` automates
this exact NATS setup + test-data step - it defaults to `SCOUTAM_HOST=conduit-scoutam` and needs no
docker/podman on the target host.)

### REST API

ScoutAM's REST API listens on `https://<host>:8080` (self-signed cert). Test credentials:
`admin`/`password`. No changes needed here beyond what ScoutAM ships with by default.

### NFS export (for conduit-fta)

```sh
sudo dnf install -y nfs-utils
echo "/mnt/scoutfs 192.168.121.0/24(rw,sync,no_root_squash,no_subtree_check)" | sudo tee /etc/exports
sudo systemctl enable --now nfs-server
sudo exportfs -ra
```

### Firewall

`firewalld` is active by default on these VMs and blocks everything not explicitly opened:

```sh
sudo firewall-cmd --permanent --add-service=nfs --add-service=rpc-bind --add-service=mountd
sudo firewall-cmd --permanent --add-port=8080/tcp --add-port=4222/tcp   # ScoutAM REST API + NATS
sudo firewall-cmd --reload
```

## 2. conduit-fta VM

### Mount the archive over NFS, prepare the online filesystem

```sh
sudo dnf install -y nfs-utils rsync
sudo mkdir -p /mnt/scoutfs /scratch
sudo mount -t nfs 192.168.121.220:/mnt/scoutfs /mnt/scoutfs
echo "192.168.121.220:/mnt/scoutfs /mnt/scoutfs nfs4 _netdev,rw 0 0" | sudo tee -a /etc/fstab
sudo chown vagrant:vagrant /scratch
```

`/mnt/scoutfs` here is the "archive" (ScoutFS/ScoutAM-backed) filesystem; `/scratch` is a plain
local directory standing in for the "online" filesystem test data gets copied to/from.

### Firewall

```sh
sudo firewall-cmd --permanent --add-port=23457/tcp   # conduit-runner listener
sudo firewall-cmd --reload
```

## 3. Certificates (generated once, on `conduit-master`)

No Kerberos/LDAP for this lab - `conduit-server`'s keytab load failure is logged but non-fatal
(`AllowAnonymous: true` on the gRPC Kerberos interceptor), and the mTLS admin client cert is
sufficient to drive transfers. See [Generating Certificates](cert-generation.md) for the general
reference; this is the minimal subset actually used here:

```sh
mkdir -p /etc/conduit/keys && cd /etc/conduit/keys

conduit-server internal-ca -d \
  --internal-ca-cert conduit-internal-ca.pem --internal-ca-key conduit-internal-key.pem

conduit-server external-ca -d \
  --external-ca-cert conduit-external-ca.pem --external-ca-key conduit-external-key.pem

# covers etcd AND rqlite, both listening on conduit-master
conduit-server internal-server-cert -d \
  --internal-ca-cert conduit-internal-ca.pem --internal-ca-key conduit-internal-key.pem \
  --separate-cert-key --cert-name etcd-rqlite-server-cert.pem --key-name etcd-rqlite-server-key.pem \
  --output ./ --server-ip 127.0.0.1,192.168.121.144 --server-hostname conduit-master,localhost \
  --server-commonname conduit-master

conduit-server external-client-cert -d \
  --external-ca-cert conduit-external-ca.pem --external-ca-key conduit-external-key.pem \
  --separate-cert-key --cert-name conduit-admin-cert.pem --key-name conduit-admin-key.pem \
  --output ./ --client-commonname conduit-admin --expiration 365
```

Copy `conduit-internal-ca.pem` **and** `conduit-internal-key.pem` to `conduit-fta`'s
`/etc/conduit/keys/` - `conduit-runner` needs the internal CA's private key too, since it mints its
own server cert on startup (see `conduit-runner --help`).

## 4. etcd + rqlite (single-node, on `conduit-master`)

Binaries installed the same way as NATS (download release tarball, copy to `/usr/local/bin`).
Versions used: etcd v3.6.11 (matching `examples/docker/docker-compose.yaml`), rqlite v10.3.6.

**Important:** binaries copied in over `scp` land with SELinux context `user_tmp_t` on these
Rocky 9 VMs (SELinux is `Enforcing`) and fail to exec (`203/EXEC` in `systemctl status`). Fix with
`sudo restorecon -v /usr/local/bin/<binary>` after copying anything into `/usr/local/bin` or
`/etc/conduit`.

`/etc/systemd/system/etcd.service`:

```ini
[Service]
User=vagrant
ExecStart=/usr/local/bin/etcd \
  --name=etcd-1 --data-dir=/var/lib/etcd/data \
  --listen-client-urls=https://192.168.121.144:2379,https://127.0.0.1:2379 \
  --advertise-client-urls=https://192.168.121.144:2379 \
  --listen-peer-urls=https://192.168.121.144:2380 \
  --initial-advertise-peer-urls=https://192.168.121.144:2380 \
  --initial-cluster=etcd-1=https://192.168.121.144:2380 \
  --initial-cluster-token=conduit-lab --initial-cluster-state=new \
  --cert-file=/etc/conduit/keys/etcd-rqlite-server-cert.pem \
  --key-file=/etc/conduit/keys/etcd-rqlite-server-key.pem \
  --client-cert-auth --trusted-ca-file=/etc/conduit/keys/conduit-internal-ca.pem \
  --peer-cert-file=/etc/conduit/keys/etcd-rqlite-server-cert.pem \
  --peer-key-file=/etc/conduit/keys/etcd-rqlite-server-key.pem \
  --peer-trusted-ca-file=/etc/conduit/keys/conduit-internal-ca.pem --peer-client-cert-auth
Restart=on-failure
```

`/etc/systemd/system/rqlite.service`:

```ini
[Service]
User=vagrant
ExecStart=/usr/local/bin/rqlited \
  -node-id=1 \
  -http-ca-cert=/etc/conduit/keys/conduit-internal-ca.pem \
  -http-cert=/etc/conduit/keys/etcd-rqlite-server-cert.pem \
  -http-key=/etc/conduit/keys/etcd-rqlite-server-key.pem -http-verify-client=true \
  -http-addr=192.168.121.144:4001 -http-adv-addr=192.168.121.144:4001 \
  -raft-addr=192.168.121.144:4002 -raft-adv-addr=192.168.121.144:4002 \
  -node-cert=/etc/conduit/keys/etcd-rqlite-server-cert.pem \
  -node-key=/etc/conduit/keys/etcd-rqlite-server-key.pem \
  -node-ca-cert=/etc/conduit/keys/conduit-internal-ca.pem -node-verify-client=true \
  /var/lib/rqlite/file
Restart=on-failure
```

`sudo systemctl daemon-reload && sudo systemctl enable --now etcd rqlite`

## 5. conduit-server (`conduit-master`)

Build (from a dev machine, cross-compiled - none of these VMs have Go installed):

```sh
GOOS=linux GOARCH=amd64 go build -o conduit-server ./cmd/server
GOOS=linux GOARCH=amd64 go build -o conduit ./cmd/cli
```

`/etc/conduit/conduit-server-config.yaml` (minimal, single-node, no Kerberos/LDAP):

```yaml
auth:
  external-ca-cert: /etc/conduit/keys/conduit-external-ca.pem
  external-ca-key: /etc/conduit/keys/conduit-external-key.pem
  internal-ca-cert: /etc/conduit/keys/conduit-internal-ca.pem
  internal-ca-key: /etc/conduit/keys/conduit-internal-key.pem
  keytab: /etc/conduit/conduit.keytab # intentionally absent; load failure is non-fatal
  requested-cert-lifetime: 24h
etcd:
  - hostname: conduit-master
    ip: 192.168.121.144
    port: 2379
rqlite:
  - hostname: conduit-master
    ip: 192.168.121.144
    port: 4001
node-allocations:
  setup: {memory: 10MB, nodes: 1}
  teardown: {memory: 10MB, nodes: 1}
  transfer: {memory: 500MB, nodes: 1}
  validation: {memory: 10MB, nodes: 1}
nodes:
  fta1:
    address: 192.168.121.129
    port: 23457
    min-memory: 1GB
    max-jobs: 4
server:
  hostname: [conduit-master]
  ip: [192.168.121.144]
  port: 23456
  concurrency: {schedulers: 1, transfer-workers: 1, watchdogs: 1}
test: true # lab only - never in production
transfer:
  expiry-advance: 60s
  max-source-bytes: 4000
```

`/etc/systemd/system/conduit-server.service`:

```ini
[Service]
User=vagrant
ExecStart=/usr/local/bin/conduit-server -d --config /etc/conduit/conduit-server-config.yaml
Restart=on-failure
```

### Firewall

```sh
sudo firewall-cmd --permanent --add-port=23456/tcp --add-port=2379/tcp --add-port=2380/tcp \
  --add-port=4001/tcp --add-port=4002/tcp
sudo firewall-cmd --reload
```

## 6. conduit-runner + conduit-fta (`conduit-fta`)

Build & copy `conduit-fta`/`conduit-runner` the same way, plus `conduit-internal-ca.pem` +
`conduit-internal-key.pem` from the master into `/etc/conduit/keys/` (see certs section above).

`/etc/conduit/conduit-runner-config.yaml`:

```yaml
auth:
  internal-ca-cert: /etc/conduit/keys/conduit-internal-ca.pem
  internal-ca-key: /etc/conduit/keys/conduit-internal-key.pem
etcd:
  - hostname: conduit-master
    ip: 192.168.121.144
    port: 2379
fta:
  environment:
    PATH: /bin:/usr/bin:/usr/local/bin
  options: ["-d"]
  path: /usr/local/bin/conduit-fta
server:
  hostname: [conduit-fta]
  ip: [192.168.121.129]
  port: 23457
```

`/etc/conduit/conduit-fta-config.yaml` - this is where the "archive vs online filesystem" split
lives, matched by regex against the requested path:

```yaml
auth:
  internal-ca-cert: /etc/conduit/keys/conduit-internal-ca.pem
transfer:
  expiry-advance: 5m
  expiry-interval: 30s
etcd:
  - hostname: conduit-master
    ip: 192.168.121.144
    port: 2379
filesystems:
  archive: # anything under the ScoutFS/NFS mount
    user-path: "^/mnt/scoutfs/.*"
    fta-path: $0
    fta-root-fs-path: /mnt/scoutfs
    plugin-stages:
      validation: posix
      setup-src: posix # scoutam stages in the transfer stage instead (see below)
      setup-dst: posix
      transfer-src: [scoutam, rsync] # <-- the plugin under test
      transfer-dst: [rsync]
      teardown-src: posix
      teardown-dst: posix
  default: # everything else, e.g. /scratch
    user-path: .*
    fta-path: $0
    fta-root-fs-path: /
    plugin-stages:
      validation: posix
      setup-src: posix
      setup-dst: posix
      transfer-src: [rsync]
      transfer-dst: [scoutam, rsync] # lets scoutam copy archive sources here
      teardown-src: posix
      teardown-dst: posix
fta:
  verify-retry-count: 20
  verify-sleep-duration: 5s
plugins:
  rsync:
    rsync-path: rsync
  scoutam:
    api-base-url: https://192.168.121.220:8080
    api-username: admin
    api-password: password
    api-insecure-skip-verify: true
    nats-servers: ["192.168.121.220:4222"]
    nats-stage-topic-prefix: conduit.stage
    stage-timeout: 30m
    batch-size: 3 # small on purpose so a test directory spans several stage requests/rsync runs
    rsync-path: rsync
```

This runs the `scoutam` plugin in **pipeline mode**: during the transfer stage it requests staging
in batches while walking the sources and copies each file with rsync as soon as ScoutAM reports it
staged. Three settings make this work:

- `setup-src: posix` on `archive`, so setup doesn't stage everything up front.
- `scoutam` first in `archive`'s `transfer-src`.
- `scoutam` in the destination filesystem's `transfer-dst`. conduit only uses a transfer plugin
  that both the source and destination filesystems list.

In this mode `stage-timeout` is how long to go without any stage notification while files are
still pending, not a limit on the whole transfer.

To stage everything before copying instead (the original mode), set `setup-src: scoutam` and
`transfer-src: [rsync]` on `archive` and drop `scoutam` from `default`'s `transfer-dst`. See
[Staging modes](scoutam-plugin-testing.md#staging-modes) for a comparison.

`/etc/systemd/system/conduit-runner.service` - **must run as root**, not the `vagrant` user:
`conduit-runner` drops privileges to the requesting user before spawning `conduit-fta`
(`syscall.Credential`), which requires `CAP_SETUID`/`CAP_SETGID` even when the target user happens
to already match the running user.

```ini
[Service]
User=root
ExecStart=/usr/local/bin/conduit-runner -d --config /etc/conduit/conduit-runner-config.yaml
Restart=on-failure
```

## 7. Test transfer

From `conduit-master` (or anywhere with the admin cert and network access to port 23456):

```sh
conduit cp -d --user vagrant \
  --ca /etc/conduit/keys/conduit-external-ca.pem \
  --cert /etc/conduit/keys/conduit-admin-cert.pem \
  --key /etc/conduit/keys/conduit-admin-key.pem \
  -i 192.168.121.144 -p 23456 \
  /mnt/scoutfs/conduit-test/testfile1.bin /scratch/testfile1-copy.bin
```

`--user` is required with the admin cert (there's no Kerberos identity to fall back to); it
determines which UID/GID `conduit-fta` runs as on `conduit-fta`. Expect the transfer to reach
`TRANSFER_FINALIZED`, and `md5sum` on both ends to match.

### Pipelined directory transfer

Create a directory of archived, released (offline) files on `conduit-scoutam`:

```sh
d=/mnt/scoutfs/conduit-test/pipeline
sudo mkdir -p $d/sub $d/empty
for i in 1 2 3 4 5; do sudo dd if=/dev/urandom of=$d/f$i.bin bs=1M count=5; done
for i in 6 7 8; do sudo dd if=/dev/urandom of=$d/sub/f$i.bin bs=1M count=5; done
sudo ln -s f1.bin $d/link-to-f1 && sudo chown -R vagrant:vagrant $d
(cd $d && md5sum $(find . -type f | sort) > /tmp/pipeline.md5)
for f in $(find $d -type f); do sudo samcli file archive -i -W $f; sudo samcli file release $f; done
for f in $(find $d -type f); do sudo scoutfs stat -s offline_blocks $f; done # non-zero = offline
```

Take checksums **before** releasing: reading a released file (for example with `md5sum`) stages it
back online.

Then copy it recursively from `conduit-master`:

```sh
conduit cp -r --user vagrant \
  --ca /etc/conduit/keys/conduit-external-ca.pem \
  --cert /etc/conduit/keys/conduit-admin-cert.pem \
  --key /etc/conduit/keys/conduit-admin-key.pem \
  -i 192.168.121.144 -p 23456 \
  /mnt/scoutfs/conduit-test/pipeline /scratch/pipeline-copy
```

Watching `/scratch/pipeline-copy` on `conduit-fta` during the transfer shows files arriving while
others are still staging. In the validation run (8 x 5MB offline files), the first file landed
~18s after submission and the transfer was `Finalized` ~7s later. Checksums matched
`/tmp/pipeline.md5`, and the symlink, empty directory, permissions, ownership and mtimes matched
the source.

## Troubleshooting notes

- **`no route to host` from conduit-fta to the ScoutAM API/NATS**: `firewalld` on
  `conduit-scoutam` blocking 8080/4222 - see the firewall step above.
- **`fork/exec ... operation not permitted`** from conduit-runner: it's running as a non-root user
  and can't drop privileges - run it as root.
- **`203/EXEC` in `systemctl status` for any conduit/etcd/rqlite/nats binary**: SELinux context is
  wrong on a binary copied in via `scp`/`mv` from `/tmp` - `sudo restorecon -v <path>`.
- **`no user provided in request`** from `conduit cp`: pass `--user <name>` when authenticating
  with the admin cert.
- **`CONDUIT_FTA_SOCKET environment variable is not set`** during validation: `conduit-fta` is
  newer than `conduit-runner`. Build and deploy `conduit-fta`, `conduit-runner`, `conduit-server`
  and the `conduit` CLI from the same commit.
- **Pipeline transfer stays at 0B and eventually times out** although ScoutAM staged the files
  (`journalctl -u scoutam` shows `done packet`): the stage notifications aren't matching requested
  files. The `conduit-fta` stderr in the `conduit-runner` journal shows `Ignoring ScoutAM stage
  notification for unrequested file ...` with the name ScoutAM reported. Names are expected to be
  relative to the ScoutFS mount (for example `conduit-test/pipeline/f1.bin`).
