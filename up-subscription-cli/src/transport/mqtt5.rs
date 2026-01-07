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

use std::sync::Arc;

use tracing::info;
use up_rust::UTransport;
use up_transport_mqtt5::{Mqtt5Transport, Mqtt5TransportOptions};

pub(crate) async fn get_mqtt5_transport(
    authority_name: &str,
    mqtt5_args: Mqtt5TransportOptions,
) -> Result<Arc<dyn UTransport>, Box<dyn std::error::Error>> {
    info!("Using MQTT 5 uProtocol transport");
    Ok(Mqtt5Transport::new(mqtt5_args, authority_name)
        .await
        .map(Arc::new)?)
}
