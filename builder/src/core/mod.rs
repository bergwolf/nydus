// Copyright 2020 Ant Group. All rights reserved.
//
// SPDX-License-Identifier: Apache-2.0

pub(crate) mod blob;
pub(crate) mod bootstrap;
pub(crate) mod chunk_dict;
pub(crate) mod context;
pub(crate) mod feature;
pub(crate) mod layout;
pub(crate) mod node;
pub(crate) mod overlay;
pub(crate) mod prefetch;
pub(crate) mod tree;
pub(crate) mod v5;
pub(crate) mod v6;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_module_structure() {
        // Verify key types are accessible from the core module
        let _ = std::mem::size_of::<node::Node>();
        let _ = std::mem::size_of::<node::NodeInfo>();
        let _ = std::mem::size_of::<node::NodeChunk>();
        let _ = std::mem::size_of::<node::ChunkSource>();
        let _ = std::mem::size_of::<overlay::Overlay>();
        let _ = std::mem::size_of::<overlay::WhiteoutSpec>();
        let _ = std::mem::size_of::<tree::Tree>();
        let _ = std::mem::size_of::<bootstrap::Bootstrap>();
        let _ = std::mem::size_of::<feature::Features>();
    }
}
