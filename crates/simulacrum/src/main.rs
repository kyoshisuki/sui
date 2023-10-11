// Copyright (c) Mysten Labs, Inc.
// SPDX-License-Identifier: Apache-2.0

use simulacrum::Simulacrum;
use std::sync::Arc;

use shared_crypto::intent::Intent;
use sui_types::{
    base_types::SuiAddress,
    effects::TransactionEffectsAPI,
    gas_coin::GasCoin,
    programmable_transaction_builder::ProgrammableTransactionBuilder,
    transaction::{GasData, Transaction, TransactionData, TransactionKind},
};

#[tokio::main]
async fn main() {
    let sim = transfer();
    sui_rest_api::start_service(
        "127.0.0.1:8080".parse().unwrap(),
        Arc::new(sim),
        "/rest".to_owned(),
    )
    .await;
}

fn transfer() -> Simulacrum {
    let mut sim = Simulacrum::new();
    let recipient = SuiAddress::generate(sim.rng());
    let (sender, key) = sim.keystore().accounts().next().unwrap();
    let sender = *sender;

    let object = sim
        .store()
        .owned_objects(sender)
        .find(|object| object.is_gas_coin())
        .unwrap();
    let gas_coin = GasCoin::try_from(object).unwrap();
    let gas_id = object.id();
    let transfer_amount = gas_coin.value() / 2;

    gas_coin.value();
    let pt = {
        let mut builder = ProgrammableTransactionBuilder::new();
        builder.transfer_sui(recipient, Some(transfer_amount));
        builder.finish()
    };

    let kind = TransactionKind::ProgrammableTransaction(pt);
    let gas_data = GasData {
        payment: vec![object.compute_object_reference()],
        owner: sender,
        price: sim.reference_gas_price(),
        budget: 1_000_000_000,
    };
    let tx_data = TransactionData::new_with_gas_data(kind, sender, gas_data);
    let tx = Transaction::from_data_and_signer(tx_data, Intent::sui_transaction(), vec![key]);

    let effects = sim.execute_transaction(tx).unwrap();
    let gas_summary = effects.gas_cost_summary();
    let gas_paid = gas_summary.net_gas_usage();

    assert_eq!(
        (transfer_amount as i64 - gas_paid) as u64,
        sim.store()
            .get_object(&gas_id)
            .and_then(|object| GasCoin::try_from(object).ok())
            .unwrap()
            .value()
    );

    assert_eq!(
        transfer_amount,
        sim.store()
            .owned_objects(recipient)
            .next()
            .and_then(|object| GasCoin::try_from(object).ok())
            .unwrap()
            .value()
    );

    let checkpoint = sim.create_checkpoint();

    assert_eq!(&checkpoint.epoch_rolling_gas_cost_summary, gas_summary);
    assert_eq!(checkpoint.network_total_transactions, 2); // genesis + 1 txn
    let checkpoint = sim.create_checkpoint();

    let checkpoint = sim.create_checkpoint();

    sim
}
