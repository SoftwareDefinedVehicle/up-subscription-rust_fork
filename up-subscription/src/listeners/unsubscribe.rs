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

use async_trait::async_trait;
use std::sync::Arc;
use tokio::task;

use up_rust::core::usubscription::{USubscription, UnsubscribeRequest};
use up_rust::{UListener, UMessage, UMessageBuilder, UStatus, UUID};

use crate::USubscriptionService;

#[derive(Clone)]
pub struct UnsubscribeListener {
    up_subscription: Arc<USubscriptionService>,
}

impl UnsubscribeListener {
    pub fn new(up_subscription: Arc<USubscriptionService>) -> Self {
        UnsubscribeListener { up_subscription }
    }
}

#[async_trait]
impl UListener for UnsubscribeListener {
    // Perform any business logic related to a unsubscribe request, deferred into a task
    // Implements https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#52-unsubscribe
    async fn on_receive(&self, msg: UMessage) {
        let unsubscribe_request: UnsubscribeRequest = msg
            .extract_protobuf()
            .expect("Expected UnsubscribeRequest payload");

        let message_attributes_cloned = msg.attributes.get_or_default().clone();
        let up_subscription_cloned = self.up_subscription.clone();
        let up_transport_cloned = self.up_subscription.up_transport.clone();

        task::spawn(async move {
            // 1. Check with bookkeeping
            let status = match up_subscription_cloned
                .unsubscribe(unsubscribe_request.clone())
                .await
            {
                Ok(()) => UStatus::ok(),
                Err(status) => status,
            };

            let message = UMessageBuilder::response_for_request(&message_attributes_cloned)
                .with_message_id(UUID::build())
                .build_with_protobuf_payload(&status)
                .expect("Error building response message");

            // 2. Respond to caller immediately
            up_transport_cloned
                .send(message)
                .await
                .expect("Error sending response message");
        });
    }

    async fn on_error(&self, err: UStatus) {
        panic!("Internal Error: {err:?}");
    }
}

#[cfg(test)]
mod tests {
    use test_case::test_case;
    use up_rust::UUri;

    #[test_case(UUri::default(); "Special listen UUri")]
    #[tokio::test]
    async fn test_subscribe_listener(_uuri: UUri) {}
}
