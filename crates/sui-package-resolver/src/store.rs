// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;
use std::path::Path;
use std::sync::Arc;

use crate::error::Error;
use crate::Result;
use crate::{Module, Package};
use async_trait::async_trait;
use diesel::{ExpressionMethods, OptionalExtension, QueryDsl, RunQueryDsl};
use move_binary_format::{access::ModuleAccess, errors::Location, CompiledModule};
use move_core_types::account_address::AccountAddress;
use sui_indexer::{indexer_reader::IndexerReader, schema_v2::objects};
use sui_rest_api::Client;
use sui_types::base_types::ObjectID;
use sui_types::{base_types::SequenceNumber, move_package::TypeOrigin, object::Object};
use typed_store::rocks::{DBMap, MetricConf};
use typed_store::traits::TableSummary;
use typed_store::traits::TypedStoreDebug;
use typed_store::Map;
use typed_store_derive::DBMapUtils;

/// Interface to abstract over access to a store of live packages.  Used to override the default
/// store during testing.
#[async_trait]
pub trait PackageStore {
    /// Latest version of the object at `id`.
    async fn version(&self, id: AccountAddress) -> Result<SequenceNumber>;

    /// Read package contents. Fails if `id` is not an object, not a package, or is malformed in
    /// some way.
    async fn fetch(&self, id: AccountAddress) -> Result<Package>;

    /// Store package `object` in the underlying store
    async fn update(&self, object: &Object) -> Result<()>;
}

pub struct DbPackageStore(pub(crate) IndexerReader);

#[async_trait]
impl PackageStore for DbPackageStore {
    async fn version(&self, id: AccountAddress) -> Result<SequenceNumber> {
        let query = objects::dsl::objects
            .select(objects::dsl::object_version)
            .filter(objects::dsl::object_id.eq(id.to_vec()));

        let Some(version) = self
            .0
            .run_query_async(move |conn| query.get_result::<i64>(conn).optional())
            .await?
        else {
            return Err(Error::PackageNotFound(id));
        };

        Ok(SequenceNumber::from_u64(version as u64))
    }

    async fn fetch(&self, id: AccountAddress) -> Result<Package> {
        let query = objects::dsl::objects
            .select((
                objects::dsl::object_version,
                objects::dsl::serialized_object,
            ))
            .filter(objects::dsl::object_id.eq(id.to_vec()));

        let Some((version, bcs)) = self
            .0
            .run_query_async(move |conn| query.get_result::<(i64, Vec<u8>)>(conn).optional())
            .await?
        else {
            return Err(Error::PackageNotFound(id));
        };

        let version = SequenceNumber::from_u64(version as u64);
        let object = bcs::from_bytes::<Object>(&bcs)?;
        make_package(id, version, &object)
    }

    async fn update(&self, _object: &Object) -> Result<()> {
        unimplemented!("Package update is not implemented")
    }
}

#[derive(DBMapUtils)]
pub struct PackageStoreTables {
    pub(crate) packages: DBMap<ObjectID, Object>,
}

impl PackageStoreTables {
    pub fn new(path: &Path) -> Arc<Self> {
        Arc::new(Self::open_tables_read_write(
            path.to_path_buf(),
            MetricConf::default(),
            None,
            None,
        ))
    }
    pub(crate) fn update(&self, package: &Object) -> Result<()> {
        let mut batch = self.packages.batch();
        batch
            .insert_batch(&self.packages, std::iter::once((package.id(), package)))
            .map_err(Error::TypedStore)?;
        batch.write().map_err(Error::TypedStore)?;
        Ok(())
    }
}

/// Store which keeps package objects in a local rocksdb store. It is expected that this store is
/// kept updated with latest version of package objects while iterating over checkpoints. If the
/// local db is missing (or gets deleted), packages are fetched from a full node and local store is
/// updated
pub struct LocalDBPackageStore {
    package_store_tables: Arc<PackageStoreTables>,
    fallback_client: Client,
}

impl LocalDBPackageStore {
    pub fn new(path: &Path, rest_url: &str) -> Self {
        let rest_api_url = format!("{}/rest", rest_url);
        Self {
            package_store_tables: PackageStoreTables::new(path),
            fallback_client: Client::new(rest_api_url),
        }
    }

    pub fn update(&self, object: &Object) -> Result<()> {
        let Some(_package) = object.data.try_as_package() else {
            return Ok(());
        };
        self.package_store_tables.update(object)?;
        Ok(())
    }

    pub async fn get(&self, id: AccountAddress) -> Result<Object> {
        let object = if let Some(object) = self
            .package_store_tables
            .packages
            .get(&ObjectID::from(id))
            .map_err(Error::TypedStore)?
        {
            object
        } else {
            let object = self
                .fallback_client
                .get_object(ObjectID::from(id))
                .await
                .map_err(|_| Error::PackageNotFound(id))?;
            self.update(&object)?;
            object
        };
        Ok(object)
    }
}

#[async_trait]
impl PackageStore for LocalDBPackageStore {
    async fn version(&self, id: AccountAddress) -> Result<SequenceNumber> {
        Ok(self.get(id).await?.version())
    }

    async fn fetch(&self, id: AccountAddress) -> Result<Package> {
        let object = self.get(id).await?;
        let package = make_package(AccountAddress::from(object.id()), object.version(), &object)?;
        Ok(package)
    }

    async fn update(&self, object: &Object) -> Result<()> {
        self.update(object)
    }
}

fn make_package(id: AccountAddress, version: SequenceNumber, object: &Object) -> Result<Package> {
    let Some(package) = object.data.try_as_package() else {
        return Err(Error::NotAPackage(id));
    };

    let mut type_origins: BTreeMap<String, BTreeMap<String, AccountAddress>> = BTreeMap::new();
    for TypeOrigin {
        module_name,
        struct_name,
        package,
    } in package.type_origin_table()
    {
        type_origins
            .entry(module_name.to_string())
            .or_default()
            .insert(struct_name.to_string(), AccountAddress::from(*package));
    }

    let mut runtime_id = None;
    let mut modules = BTreeMap::new();
    for (name, bytes) in package.serialized_module_map() {
        let origins = type_origins.remove(name).unwrap_or_default();
        let bytecode = CompiledModule::deserialize_with_defaults(bytes)
            .map_err(|e| Error::Deserialize(e.finish(Location::Undefined)))?;

        runtime_id = Some(*bytecode.address());

        let name = name.clone();
        match Module::read(bytecode, origins) {
            Ok(module) => modules.insert(name, module),
            Err(struct_) => return Err(Error::NoTypeOrigin(id, name, struct_)),
        };
    }

    let Some(runtime_id) = runtime_id else {
        return Err(Error::EmptyPackage(id));
    };

    let linkage = package
        .linkage_table()
        .iter()
        .map(|(&dep, linkage)| (dep.into(), linkage.upgraded_id.into()))
        .collect();

    Ok(Package {
        storage_id: id,
        runtime_id,
        version,
        modules,
        linkage,
    })
}
