#! /bin/bash

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

