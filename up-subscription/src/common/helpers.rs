/********************************************************************************
 * Copyright (c) 2024 Contributors to the Eclipse Foundation
 *
 * See the NOTICE file(s) distributed with this work for additional
 * information regarding copyright ownership.
 *
 * This program and the accompanying materials are made available under the
 * terms of the Apache License Version 2.0 which is available at
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * SPDX-License-Identifier: Apache-2.0
 ********************************************************************************/

use log::*;
use std::future::Future;
use std::sync::Once;
use tokio::task;

use up_rust::{
    communication::{ServiceInvocationError, UPayload},
    UAttributes, UUri,
};

static INIT: Once = Once::new();

pub fn init_once() {
    INIT.call_once(env_logger::init);
}

type SpawnResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;

pub(crate) fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = SpawnResult<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            error!("{}", e)
        }
    })
}

// Internal helper for basic input validation, as used by every Rpc request handler
pub(crate) fn extract_inputs<T>(
    expected_resource_id: u16,
    resource_id: u16,
    payload: &Option<UPayload>,
    message_attributes: &UAttributes,
) -> Result<(T, UUri), ServiceInvocationError>
where
    T: protobuf::MessageFull,
{
    if resource_id != expected_resource_id {
        return Err(ServiceInvocationError::InvalidArgument(format!(
            "Wrong resource ID (expected {}, got {})",
            expected_resource_id, resource_id
        )));
    }
    let Some(payload) = payload else {
        return Err(ServiceInvocationError::InvalidArgument(
            "No request payload".to_string(),
        ));
    };
    let request: T = payload.extract_protobuf().map_err(|e| {
        ServiceInvocationError::InvalidArgument(
            format!("Expected NotificationsRequest payload, error when unpacking {e}").to_string(),
        )
    })?;
    let Some(source) = message_attributes.source.as_ref() else {
        return Err(ServiceInvocationError::InvalidArgument(
            "No request source uri".to_string(),
        ));
    };

    Ok((request, source.clone()))
}
