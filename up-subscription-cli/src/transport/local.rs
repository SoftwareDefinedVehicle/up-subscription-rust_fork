/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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

use std::sync::Arc;

use tracing::info;
use up_rust::{local_transport::LocalTransport, UTransport};

pub(crate) async fn get_local_transport() -> Result<Arc<dyn UTransport>, Box<dyn std::error::Error>>
{
    info!("Using in-memory local uProtocol transport");
    Ok(Arc::new(LocalTransport::default()))
}
