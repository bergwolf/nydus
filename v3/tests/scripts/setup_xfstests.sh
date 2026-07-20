#!/bin/bash
# setup_xfstests.sh — Install dependencies and build xfstests.
#
# This script handles environment setup only (package installation, git clone,
# and compilation). The actual test execution is driven by Go integration tests.
#
# Usage: sudo bash setup_xfstests.sh

set -euo pipefail

XFSTESTS_DIR="/tmp/xfstests-dev"
XFSTESTS_TAG="v2023.12.10"

echo "====> Installing system dependencies..."
apt-get update -qq
apt-get install -y -qq acl attr automake bc dbench dump e2fsprogs fio gawk \
    gcc git indent libacl1-dev libaio-dev libcap-dev libgdbm-dev libtool \
    libtool-bin liburing-dev libuuid1 lvm2 make psmisc python3 quota sed \
    uuid-dev uuid-runtime xfsprogs linux-headers-"$(uname -r)" sqlite3 \
    exfatprogs f2fs-tools ocfs2-tools udftools xfsdump \
    xfslibs-dev fuse3 2>/dev/null

echo "====> Building xfstests (${XFSTESTS_TAG})..."
if [ ! -d "${XFSTESTS_DIR}" ]; then
    git clone -b "${XFSTESTS_TAG}" \
        git://git.kernel.org/pub/scm/fs/xfs/xfstests-dev.git \
        "${XFSTESTS_DIR}"
fi
cd "${XFSTESTS_DIR}"
make -j"$(nproc)"
make install

echo "====> xfstests ready at ${XFSTESTS_DIR}"
