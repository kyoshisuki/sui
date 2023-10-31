// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use crate::store::{DbPackageStore, PackageStore};
use crate::Result;
use crate::{Package, ResolutionContext};
use lru::LruCache;
use move_core_types::account_address::AccountAddress;
use move_core_types::language_storage::TypeTag;
use move_core_types::value::MoveTypeLayout;
use std::num::NonZeroUsize;
use std::sync::{Arc, Mutex};
use sui_indexer::indexer_reader::IndexerReader;
use sui_types::is_system_package;

const PACKAGE_CACHE_SIZE: NonZeroUsize = unsafe { NonZeroUsize::new_unchecked(1024) };

/// Cache to answer queries that depend on information from move packages: listing a package's
/// modules, a module's structs and functions, the definitions or layouts of types, etc.
///
/// Queries that cannot be answered by the cache are served by loading the relevant package as an
/// object and parsing its contents.
pub struct PackageCache {
    packages: Mutex<LruCache<AccountAddress, Arc<Package>>>,
    store: Box<dyn PackageStore + Send + Sync>,
}

impl PackageCache {
    pub fn new(reader: IndexerReader) -> Self {
        Self::with_store(Box::new(DbPackageStore(reader)))
    }

    pub fn with_store(store: Box<dyn PackageStore + Send + Sync>) -> Self {
        let packages = Mutex::new(LruCache::new(PACKAGE_CACHE_SIZE));
        Self { packages, store }
    }

    /// Return the type layout corresponding to the given type tag.  The layout always refers to
    /// structs in terms of their defining ID (i.e. their package ID always points to the first
    /// package that introduced them).
    pub async fn type_layout(&self, mut tag: TypeTag) -> Result<MoveTypeLayout> {
        let mut context = ResolutionContext::default();

        // (1). Fetch all the information from this cache that is necessary to resolve types
        // referenced by this tag.
        context.add_type_tag(&mut tag, self).await?;

        // (2). Use that information to resolve the tag into a layout.
        context.resolve_type_tag(&tag)
    }

    /// Return a deserialized representation of the package with ObjectID `id` on-chain.  Attempts
    /// to fetch this package from the cache, and if that fails, fetches it from the underlying data
    /// source and updates the cache.
    pub async fn package(&self, id: AccountAddress) -> Result<Arc<Package>> {
        let candidate = {
            // Release the lock after getting the package
            let mut packages = self.packages.lock().unwrap();
            packages.get(&id).map(Arc::clone)
        };

        // System packages can be invalidated in the cache if a newer version exists.
        match candidate {
            Some(package) if !is_system_package(id) => return Ok(package),
            Some(package) if self.store.version(id).await? <= package.version => {
                return Ok(package)
            }
            Some(_) | None => { /* nop */ }
        }

        let package = Arc::new(self.store.fetch(id).await?);

        // Try and insert the package into the cache, accounting for races.  In most cases the
        // racing fetches will produce the same package, but for system packages, they may not, so
        // favour the package that has the newer version, or if they are the same, the package that
        // is already in the cache.

        let mut packages = self.packages.lock().unwrap();
        Ok(match packages.peek(&id) {
            Some(prev) if package.version <= prev.version => {
                let package = prev.clone();
                packages.promote(&id);
                package
            }

            Some(_) | None => {
                packages.push(id, package.clone());
                package
            }
        })
    }

    pub async fn update_store(&self, object: &sui_types::object::Object) -> Result<()> {
        self.store.update(object).await
    }
}
