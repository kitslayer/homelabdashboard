# fleet-agent container — for hosts where the systemd/openrc installer
# isn't an option (e.g. TrueNAS SCALE with a read-only /usr/local/bin).
#
# Image contains zfsutils-linux + smartmontools so the agent's optional
# collectors can pick up ZFS pool and SMART data when the container is
# run with appropriate host mounts.
FROM debian:bookworm-slim

RUN sed -i 's/main$/main contrib/' /etc/apt/sources.list.d/debian.sources \
    && apt-get update \
    && apt-get install -y --no-install-recommends \
        ca-certificates \
        zfsutils-linux \
        smartmontools \
        util-linux \
        iproute2 \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /var/lib/fleet-agent

COPY fleet-agent-linux-amd64 /usr/local/bin/fleet-agent
RUN chmod +x /usr/local/bin/fleet-agent

ENV FLEET_STATE=/var/lib/fleet-agent/state.json

ENTRYPOINT ["/usr/local/bin/fleet-agent"]
