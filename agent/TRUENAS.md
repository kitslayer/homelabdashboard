# Deploying fleet-agent on TrueNAS SCALE

TrueNAS SCALE keeps `/usr/local/bin` read-only, so the regular `install.sh`
won't work. Use the Docker image instead.

## 1. Copy the prebuilt image tar to TrueNAS

On the laptop:

```bash
ssh truenas_admin@<TRUENAS_HOST> 'mkdir -p /mnt/ssd-pool/apps/fleet-agent/state'
scp /home/miles/Stats/agent/fleet-agent-0.1.0.tar \
    truenas_admin@<TRUENAS_HOST>:/mnt/ssd-pool/apps/fleet-agent/image.tar
```

> If you don't use `ssd-pool`, pick any persistent pool. Update the
> `volumes:` and the image path in step 2 to match.

## 2. Load the image into Docker on TrueNAS

In the TrueNAS web UI: **System Settings → Shell**, then:

```bash
docker load -i /mnt/ssd-pool/apps/fleet-agent/image.tar
docker images | grep fleet-agent
# fleet-agent     0.1.0   ...   145MB
```

## 3. Create the Custom App

In the TrueNAS web UI: **Apps → Discover Apps → Custom App** (top-right).

- App name: `fleet-agent`
- Paste the contents of `truenas-compose.yml` into the YAML editor.
- Save and start.

After a minute the agent should register. Verify on the laptop:

```bash
curl -sH "Authorization: Bearer $FLEET_ADMIN_TOKEN" \
    http://<LAN_VIP>/api/fleet/v1/hosts | jq '.hosts[] | .display_name'
```

Look for `TrueNAS` in the list.

## 4. (Optional) Strip the bootstrap token

Once the agent is registered, the persistent `state.json` under
`/mnt/ssd-pool/apps/fleet-agent/state/` holds the long-lived api_key. You
can blank `FLEET_BOOTSTRAP_TOKEN` in the compose env and restart the app.

## What the agent will collect on TrueNAS

- CPU / memory / disk / network (via host `/proc` mount)
- ZFS pools — `zpool list -H` is run from inside the container; works
  because `/dev/zfs` is mapped in and the container has `zfsutils-linux`.
- SMART for every disk visible at `/dev/*` (smartmontools bundled).
- Docker containers running on the TrueNAS host — but only ones launched
  by TrueNAS Apps, since the container can see the host's Docker daemon
  only when its socket is mapped. Not mapped by default; uncomment the
  `/var/run/docker.sock` line in the compose yml to include this.
- systemd services — yes, because pid host + privileged.

## Troubleshooting

- **Image not found**: re-run `docker load -i …image.tar` and check
  `docker images`.
- **Host shows as `truenas` but no ZFS data**: check the container logs
  for `zpool: command not found` (means image was built wrong) or
  `Permission denied accessing /dev/zfs` (need `privileged: true`).
- **Agent registers as `localhost`**: pass `FLEET_DISPLAY_NAME` to give
  it a friendly label even if the kernel hostname is generic.
