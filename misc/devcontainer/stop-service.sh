#!/bin/bash
# Copyright (c) 2026. Nydus Developers. All rights reserved.
#
# SPDX-License-Identifier: Apache-2.0

# kill dockerd
ps aux|grep dockerd|grep -v grep |awk '{print $2}'|xargs kill
# kill nydus snapshotter
ps aux|grep containerd-nydus-grpc |grep -v grep |awk '{print $2}'|xargs kill
# kill containerd
ps aux|grep "containerd --config /etc/containerd/config.toml" |grep -v grep |awk '{print $2}'|xargs kill
