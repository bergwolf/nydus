#!/bin/bash
# Copyright (c) 2026. Nydus Developers. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

set -eEuo pipefail

function start_dockerd {
    echo "starting dockerd"
    # do not ever start again
    [ -e /var/run/docker.sock ] && return 0
    dockerd > /var/log/dockerd.log 2>&1 &
    # try to prepare testing images
    sleep 5 # let dockerd start
    docker run -d -it --rm -p $REGISTRY_PORT:5000 registry:2 2>&1 &
    docker pull ubuntu:latest 2>&1 &
    docker pull redis:7.0.1 2>&1 &
    docker pull redis:7.0.2 2>&1 &
    docker pull redis:7.0.3 2>&1 &
    docker pull nginx:latest 2>&1 &
}

function start_nydus_snapshotter {
    echo "starting nydus snapshotter"
    [ -e /run/containerd-nydus/containerd-nydus-grpc.sock ] && return 0
    /usr/local/bin/containerd-nydus-grpc --config /etc/nydus/snapshotter_config.toml > /var/log/nydus-snapshotter.log 2>&1 &
}

function start_containerd {
    echo "starting containerd"
    [ -e /run/containerd/containerd.sock ] && return 0
    /usr/bin/containerd --config /etc/containerd/config.toml > /var/log/containerd.log 2>&1 &
}

start_dockerd
start_nydus_snapshotter
start_containerd

export NYDUS_STABLE_VERSION=$(curl https://api.github.com/repos/Dragonflyoss/nydus/releases/latest | jq -r '.tag_name')
export NYDUS_STABLE_VERSION_EXPORT="${NYDUS_STABLE_VERSION//./_}"

versions=(v0.1.0 ${NYDUS_STABLE_VERSION} latest)
version_exports=(v0_1_0 ${NYDUS_STABLE_VERSION_EXPORT} latest)
for i in ${!version_exports[@]}; do
  version=${versions[$i]}
  version_export=${version_exports[$i]}
  export NYDUS_BUILDER_$version_export=/usr/bin/nydus-$version/nydus-image
  export NYDUS_NYDUSD_$version_export=/usr/bin/nydus-$version/nydusd
  export NYDUS_NYDUSIFY_$version_export=/usr/bin/nydus-$version/nydusify
done


if [ "$#" -eq 0 ]; then
    tail -f /dev/null
else
    exec "$@"
fi
