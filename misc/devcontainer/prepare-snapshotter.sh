#!/bin/bash
# Copyright (c) 2026. Nydus Developers. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

readonly SNAPSHOTTER_VERSION=`curl -s https://api.github.com/repos/containerd/nydus-snapshotter/releases/latest | grep tag_name | cut -f4 -d "\""`
readonly NERDCTL_VERSION=`curl -s https://api.github.com/repos/containerd/nerdctl/releases/latest | grep tag_name | cut -f4 -d "\"" | sed 's/^v//g'`
readonly CNI_PLUGINS_VERSION=`curl -s https://api.github.com/repos/containernetworking/plugins/releases/latest | grep tag_name | cut -f4 -d "\""`

# setup nerdctl and nydusd env
mkdir -p /opt/cni/bin
mkdir -p /tmp/setup
cd /tmp/setup
wget https://github.com/containerd/nydus-snapshotter/releases/download/$SNAPSHOTTER_VERSION/nydus-snapshotter-$SNAPSHOTTER_VERSION-linux-amd64.tar.gz
tar zxvf nydus-snapshotter-$SNAPSHOTTER_VERSION-linux-amd64.tar.gz
install -D -m 755 bin/containerd-nydus-grpc /usr/local/bin

wget https://github.com/containerd/nerdctl/releases/download/v$NERDCTL_VERSION/nerdctl-$NERDCTL_VERSION-linux-amd64.tar.gz
tar -xzvf nerdctl-$NERDCTL_VERSION-linux-amd64.tar.gz -C /usr/local/bin

wget https://github.com/containernetworking/plugins/releases/download/$CNI_PLUGINS_VERSION/cni-plugins-linux-amd64-$CNI_PLUGINS_VERSION.tgz 
tar -xzvf cni-plugins-linux-amd64-$CNI_PLUGINS_VERSION.tgz -C /opt/cni/bin

# cleanup
rm -rf /tmp/setup
