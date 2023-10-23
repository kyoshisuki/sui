// Copyright (c) The Diem Core Contributors
// Copyright (c) The Move Contributors
// SPDX-License-Identifier: Apache-2.0

use std::collections::BTreeMap;

use crate::{
    account_address::AccountAddress,
    ident_str,
    identifier::Identifier,
    language_storage::{StructTag, TypeTag},
    value::{MoveDataType, MoveDataTypeLayout, MoveFieldLayout, MoveTypeLayout, MoveValue},
};
use serde_json::json;

#[test]
fn struct_deserialization() {
    let struct_type = StructTag {
        address: AccountAddress::ZERO,
        name: ident_str!("MyStruct").to_owned(),
        module: ident_str!("MyModule").to_owned(),
        type_params: vec![],
    };
    let values = vec![MoveValue::U64(7), MoveValue::Bool(true)];
    let fields = vec![ident_str!("f").to_owned(), ident_str!("g").to_owned()];
    let field_values: Vec<(Identifier, MoveValue)> =
        fields.into_iter().zip(values.clone()).collect();

    // test each deserialization scheme
    let runtime_value = MoveDataType::Runtime(values);
    let ser = MoveValue::DataType(runtime_value.clone())
        .simple_serialize()
        .unwrap();
    println!("serialized: {:?}", ser);
    assert_eq!(
        serde_json::to_value(&runtime_value).unwrap(),
        json!([7, true])
    );

    let fielded_value = MoveDataType::WithFields(field_values.clone());
    assert_eq!(
        serde_json::to_value(&fielded_value).unwrap(),
        json!({ "f": 7, "g": true })
    );

    let typed_value = MoveDataType::with_types(struct_type, field_values);
    assert_eq!(
        serde_json::to_value(&typed_value).unwrap(),
        json!({
                "fields": { "f": 7, "g": true },
                "type": "0x0::MyModule::MyStruct"
            }
        )
    );
}

#[test]
fn enum_deserialization() {
    let enum_type = StructTag {
        address: AccountAddress::ZERO,
        name: ident_str!("MyEnum").to_owned(),
        module: ident_str!("MyModule").to_owned(),
        type_params: vec![],
    };

    let values1 = vec![MoveValue::U64(7), MoveValue::Bool(true)];
    let fields1 = vec![ident_str!("f").to_owned(), ident_str!("g").to_owned()];
    let field_values1: Vec<(Identifier, MoveValue)> =
        fields1.into_iter().zip(values1.clone()).collect();

    let values2 = vec![MoveValue::U64(8), MoveValue::Bool(false), MoveValue::U8(0)];
    let fields2 = vec![
        ident_str!("f2").to_owned(),
        ident_str!("g2").to_owned(),
        ident_str!("h2").to_owned(),
    ];
    let field_values2: Vec<(Identifier, MoveValue)> =
        fields2.into_iter().zip(values2.clone()).collect();

    let enum_runtime_layout = {
        let variant_layout1 = vec![MoveTypeLayout::U64, MoveTypeLayout::Bool];
        let variant_layout2 = vec![
            MoveTypeLayout::U64,
            MoveTypeLayout::Bool,
            MoveTypeLayout::U8,
        ];
        let enum_layout = MoveDataTypeLayout::EnumRuntime {
            variants: vec![variant_layout1, variant_layout2],
        };
        MoveTypeLayout::Struct(enum_layout)
    };

    let enum_with_types_layout = {
        let variant_layout1 = vec![
            MoveFieldLayout::new(ident_str!("f").to_owned(), MoveTypeLayout::U64),
            MoveFieldLayout::new(ident_str!("g").to_owned(), MoveTypeLayout::Bool),
        ];
        let variant_layout2 = vec![
            MoveFieldLayout::new(ident_str!("f2").to_owned(), MoveTypeLayout::U64),
            MoveFieldLayout::new(ident_str!("g2").to_owned(), MoveTypeLayout::Bool),
            MoveFieldLayout::new(ident_str!("h2").to_owned(), MoveTypeLayout::U8),
        ];
        let variant_map = vec![
            (ident_str!("Variant1").to_owned(), 0),
            (ident_str!("Variant2").to_owned(), 1),
        ]
        .into_iter()
        .zip(vec![variant_layout1, variant_layout2].into_iter())
        .collect();
        let enum_layout = MoveDataTypeLayout::EnumWithFields {
            variants: variant_map,
        };
        MoveTypeLayout::Struct(enum_layout)
    };

    // test each deserialization scheme
    let runtime_value = MoveDataType::VariantRuntime {
        tag: 0,
        fields: values1,
    };
    let v = serde_json::to_value(&runtime_value).unwrap();
    assert_eq!(v, json!([0, [7, true]]));

    let ser = MoveValue::DataType(runtime_value.clone())
        .simple_serialize()
        .unwrap();
    assert_eq!(
        MoveValue::simple_deserialize(&ser, &enum_runtime_layout).unwrap(),
        MoveValue::DataType(runtime_value),
    );

    let runtime_value = MoveDataType::VariantRuntime {
        tag: 1,
        fields: values2,
    };
    assert_eq!(
        serde_json::to_value(&runtime_value).unwrap(),
        json!([1, [8, false, 0]])
    );

    let fielded_value = MoveDataType::VariantWithFields {
        variant_tag: 0,
        variant_name: ident_str!("Variant1").to_owned(),
        fields: field_values1.clone(),
    };
    assert_eq!(
        serde_json::to_value(&fielded_value).unwrap(),
        json!({ "variant_name": "Variant1", "variant_tag": 0, "fields": { "f": 7, "g": true } })
    );

    let fielded_value = MoveDataType::VariantWithFields {
        variant_tag: 1,
        variant_name: ident_str!("Variant2").to_owned(),
        fields: field_values2.clone(),
    };
    assert_eq!(
        serde_json::to_value(&fielded_value).unwrap(),
        json!({ "variant_name": "Variant2", "variant_tag": 1, "fields": { "f2": 8, "g2": false, "h2": 0} })
    );

    let typed_value = MoveDataType::VariantWithTypes {
        type_: enum_type.clone(),
        variant_name: ident_str!("Variant1").to_owned(),
        variant_tag: 0,
        fields: field_values1,
    };
    assert_eq!(
        serde_json::to_value(&typed_value).unwrap(),
        json!({
            "type": "0x0::MyModule::MyEnum",
            "variant_name": "Variant1",
            "variant_tag": 0,
            "fields": {
                "f": 7,
                "g": true,
            }
        })
    );

    let typed_value = MoveDataType::VariantWithTypes {
        type_: enum_type,
        variant_name: ident_str!("Variant2").to_owned(),
        variant_tag: 1,
        fields: field_values2,
    };

    assert_eq!(
        serde_json::to_value(&typed_value).unwrap(),
        json!({
            "type": "0x0::MyModule::MyEnum",
            "variant_name": "Variant2",
            "variant_tag": 1,
            "fields": {
                "f2": 8,
                "g2": false,
                "h2": 0
            }
        })
    );
}

/// A test which verifies that the BCS representation of
/// a struct with a single field is equivalent to the BCS
/// of the value in this field. It also tests
/// that BCS serialization of utf8 strings is equivalent
/// to the BCS serialization of vector<u8> of the bytes of
/// the string.
#[test]
fn struct_one_field_equiv_value() {
    let val = MoveValue::Vector(vec![
        MoveValue::U8(1),
        MoveValue::U8(22),
        MoveValue::U8(13),
        MoveValue::U8(99),
    ]);
    let s1 = MoveValue::DataType(MoveDataType::Runtime(vec![val.clone()]))
        .simple_serialize()
        .unwrap();
    let s2 = val.simple_serialize().unwrap();
    assert_eq!(s1, s2);

    let utf8_str = "çå∞≠¢õß∂ƒ∫";
    let vec_u8 = MoveValue::Vector(
        utf8_str
            .as_bytes()
            .iter()
            .map(|c| MoveValue::U8(*c))
            .collect(),
    );
    assert_eq!(
        bcs::to_bytes(utf8_str).unwrap(),
        vec_u8.simple_serialize().unwrap()
    )
}

#[test]
fn nested_typed_struct_deserialization() {
    let struct_type = StructTag {
        address: AccountAddress::ZERO,
        name: ident_str!("MyStruct").to_owned(),
        module: ident_str!("MyModule").to_owned(),
        type_params: vec![],
    };
    let nested_struct_type = StructTag {
        address: AccountAddress::ZERO,
        name: ident_str!("NestedStruct").to_owned(),
        module: ident_str!("NestedModule").to_owned(),
        type_params: vec![TypeTag::U8],
    };

    // test each deserialization scheme
    let nested_runtime_struct = MoveValue::DataType(MoveDataType::Runtime(vec![MoveValue::U64(7)]));
    let runtime_value = MoveDataType::Runtime(vec![nested_runtime_struct]);
    assert_eq!(serde_json::to_value(&runtime_value).unwrap(), json!([[7]]));

    let nested_fielded_struct = MoveValue::DataType(MoveDataType::with_fields(vec![(
        ident_str!("f").to_owned(),
        MoveValue::U64(7),
    )]));
    let fielded_value = MoveDataType::with_fields(vec![(
        ident_str!("inner").to_owned(),
        nested_fielded_struct,
    )]);
    assert_eq!(
        serde_json::to_value(&fielded_value).unwrap(),
        json!({ "inner": { "f": 7 } })
    );

    let nested_typed_struct = MoveValue::DataType(MoveDataType::with_types(
        nested_struct_type,
        vec![(ident_str!("f").to_owned(), MoveValue::U64(7))],
    ));
    let typed_value = MoveDataType::with_types(
        struct_type,
        vec![(ident_str!("inner").to_owned(), nested_typed_struct)],
    );
    assert_eq!(
        serde_json::to_value(&typed_value).unwrap(),
        json!({
            "fields": {
                "inner": {
                    "fields": { "f": 7},
                    "type": "0x0::NestedModule::NestedStruct<u8>",
                }
            },
            "type": "0x0::MyModule::MyStruct"
        })
    );
}
