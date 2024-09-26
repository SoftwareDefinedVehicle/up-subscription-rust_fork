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
use up_rust::{UCode, UStatus, UUri};

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

/// Subscription topics must
/// - not be empty
/// - not contain any wildcars
// [impl->req~usubscribe-uri-not-empty~1]
// [impl->req~usubscribe-uri-authority~1]
// [impl->req~usubscribe-uri-entity-id~1]
// [impl->req~usubscribe-uri-resource-id~1]
// [impl->req~usubscribe-uri-version-major~1]
pub(crate) fn is_valid_topic(topic: &UUri) -> Result<(), UStatus> {
    if topic.is_empty() {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Empty subscription topic",
        ));
    }
    if topic.has_wildcard_authority()
        || topic.has_wildcard_entity_id()
        || topic.has_wildcard_resource_id()
        || topic.has_wildcard_version()
    {
        return Err(UStatus::fail_with_code(
            UCode::INVALID_ARGUMENT,
            "Subscription topics are not allowed to contain wildcards",
        ));
    }

    Ok(())
}
