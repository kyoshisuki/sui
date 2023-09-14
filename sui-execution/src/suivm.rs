// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use std::{collections::HashSet, sync::Arc};

use move_binary_format::CompiledModule;
use move_vm_config::verifier::VerifierConfig;
use sui_protocol_config::ProtocolConfig;
use sui_types::{
    base_types::{ObjectRef, SuiAddress, TxContext},
    committee::EpochId,
    digests::TransactionDigest,
    effects::TransactionEffects,
    error::{ExecutionError, SuiError, SuiResult},
    execution::TypeLayoutStore,
    execution_mode::{self, ExecutionResult},
    gas::SuiGasStatus,
    inner_temporary_store::InnerTemporaryStore,
    metrics::{BytecodeVerifierMetrics, LimitsMetrics},
    storage::BackingStore,
    transaction::{InputObjects, ProgrammableTransaction, TransactionKind},
    type_resolver::LayoutResolver,
};

use move_vm_runtime_suivm::move_vm::MoveVM;
use sui_adapter_suivm::adapter::{
    default_verifier_config, new_move_vm, run_metered_move_bytecode_verifier,
};
use sui_adapter_suivm::execution_engine::{
    execute_genesis_state_update, execute_transaction_to_effects,
};
use sui_adapter_suivm::type_layout_resolver::TypeLayoutResolver;
use sui_move_natives_suivm::all_natives;
use sui_verifier_suivm::meter::SuiVerifierMeter;

use crate::executor;
use crate::verifier;

pub(crate) struct Executor(Arc<MoveVM>);

pub(crate) struct Verifier<'m> {
    config: VerifierConfig,
    metrics: &'m Arc<BytecodeVerifierMetrics>,
    meter: SuiVerifierMeter,
}

impl Executor {
    pub(crate) fn new(
        protocol_config: &ProtocolConfig,
        paranoid_type_checks: bool,
        silent: bool,
    ) -> Result<Self, SuiError> {
        Ok(Executor(Arc::new(new_move_vm(
            all_natives(silent),
            protocol_config,
            paranoid_type_checks,
        )?)))
    }
}

impl<'m> Verifier<'m> {
    pub(crate) fn new(
        protocol_config: &ProtocolConfig,
        is_metered: bool,
        metrics: &'m Arc<BytecodeVerifierMetrics>,
    ) -> Self {
        let config = default_verifier_config(protocol_config, is_metered);
        let meter = SuiVerifierMeter::new(&config);
        Verifier {
            config,
            metrics,
            meter,
        }
    }
}

impl executor::Executor for Executor {
    fn execute_transaction_to_effects<'backing>(
        &self,
        store: Arc<dyn BackingStore + Send + Sync + 'backing>,
        protocol_config: &ProtocolConfig,
        metrics: Arc<LimitsMetrics>,
        enable_expensive_checks: bool,
        certificate_deny_set: &HashSet<TransactionDigest>,
        epoch_id: &EpochId,
        epoch_timestamp_ms: u64,
        input_objects: InputObjects,
        gas_coins: Vec<ObjectRef>,
        gas_status: SuiGasStatus,
        transaction_kind: TransactionKind,
        transaction_signer: SuiAddress,
        transaction_digest: TransactionDigest,
    ) -> (
        InnerTemporaryStore,
        TransactionEffects,
        Result<(), ExecutionError>,
    ) {
        execute_transaction_to_effects::<execution_mode::Normal>(
            store,
            input_objects,
            gas_coins,
            gas_status,
            transaction_kind,
            transaction_signer,
            transaction_digest,
            &self.0,
            epoch_id,
            epoch_timestamp_ms,
            protocol_config,
            metrics,
            enable_expensive_checks,
            certificate_deny_set,
        )
    }

    fn dev_inspect_transaction(
        &self,
        store: Arc<dyn BackingStore + Send + Sync>,
        protocol_config: &ProtocolConfig,
        metrics: Arc<LimitsMetrics>,
        enable_expensive_checks: bool,
        certificate_deny_set: &HashSet<TransactionDigest>,
        epoch_id: &EpochId,
        epoch_timestamp_ms: u64,
        input_objects: InputObjects,
        gas_coins: Vec<ObjectRef>,
        gas_status: SuiGasStatus,
        transaction_kind: TransactionKind,
        transaction_signer: SuiAddress,
        transaction_digest: TransactionDigest,
    ) -> (
        InnerTemporaryStore,
        TransactionEffects,
        Result<Vec<ExecutionResult>, ExecutionError>,
    ) {
        execute_transaction_to_effects::<execution_mode::DevInspect>(
            store,
            input_objects,
            gas_coins,
            gas_status,
            transaction_kind,
            transaction_signer,
            transaction_digest,
            &self.0,
            epoch_id,
            epoch_timestamp_ms,
            protocol_config,
            metrics,
            enable_expensive_checks,
            certificate_deny_set,
        )
    }

    fn update_genesis_state(
        &self,
        store: Arc<dyn BackingStore + Send + Sync>,
        protocol_config: &ProtocolConfig,
        metrics: Arc<LimitsMetrics>,
        tx_context: &mut TxContext,
        input_objects: InputObjects,
        pt: ProgrammableTransaction,
    ) -> Result<InnerTemporaryStore, ExecutionError> {
        execute_genesis_state_update(
            store,
            protocol_config,
            metrics,
            &self.0,
            tx_context,
            input_objects,
            pt,
        )
    }

    fn type_layout_resolver<'r, 'vm: 'r, 'store: 'r>(
        &'vm self,
        store: Box<dyn TypeLayoutStore + 'store>,
    ) -> Box<dyn LayoutResolver + 'r> {
        Box::new(TypeLayoutResolver::new(&self.0, store))
    }
}

impl<'m> verifier::Verifier for Verifier<'m> {
    fn meter_compiled_modules(
        &mut self,
        _protocol_config: &ProtocolConfig,
        modules: &[CompiledModule],
    ) -> SuiResult<()> {
        run_metered_move_bytecode_verifier(modules, &self.config, &mut self.meter, self.metrics)
    }
}
