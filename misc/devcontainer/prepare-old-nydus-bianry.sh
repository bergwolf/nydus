#!/bin/bash

export NYDUS_STABLE_VERSION=$(curl https://api.github.com/repos/Dragonflyoss/nydus/releases/latest | jq -r '.tag_name')

versions=(v0.1.0 ${NYDUS_STABLE_VERSION})
version_archs=(v0.1.0-x86_64 ${NYDUS_STABLE_VERSION}-linux-amd64)
mkdir -p /tmp/nydusd
cd /tmp/nydusd
for i in ${!versions[@]}; do
  version=${versions[$i]}
  version_arch=${version_archs[$i]}

  wget -q https://github.com/dragonflyoss/nydus/releases/download/$version/nydus-static-$version_arch.tgz
  sudo mkdir nydus-$version /usr/bin/nydus-$version
  sudo tar xzf nydus-static-$version_arch.tgz -C nydus-$version
  sudo cp -r nydus-$version/nydus-static/* /usr/bin/nydus-$version/
done
rm -rf /tmp/nydusd
