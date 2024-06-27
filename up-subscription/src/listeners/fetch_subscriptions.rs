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

use up_rust::core::usubscription::{FetchSubscriptionsRequest, USubscription};
use up_rust::{UCode, UListener, UMessage, UMessageBuilder, UStatus, UUID};

use crate::USubscriptionService;

#[derive(Clone)]
pub struct FetchSubscriptionsListener {
    up_subscription: Arc<USubscriptionService>,
}

impl FetchSubscriptionsListener {
    pub fn new(up_subscription: Arc<USubscriptionService>) -> Self {
        FetchSubscriptionsListener { up_subscription }
    }
}

#[async_trait]
impl UListener for FetchSubscriptionsListener {
    // Perform any business logic related to a `FetchSubscriptionsRequest`
    async fn on_receive(&self, msg: UMessage) {
        let fetchsubscriptions_request: FetchSubscriptionsRequest = msg
            .extract_protobuf()
            .expect("Expected FetchSubscriptionsRequest payload");

        // 1. Check with backend
        let message = match self
            .up_subscription
            .fetch_subscriptions(fetchsubscriptions_request.clone())
            .await
        {
            Ok(response) => UMessageBuilder::response_for_request(msg.attributes.get_or_default())
                .with_message_id(UUID::build())
                .with_comm_status(UCode::OK)
                .build_with_protobuf_payload(&response)
                .expect("Error building response message"),
            Err(status) => UMessageBuilder::response_for_request(msg.attributes.get_or_default())
                .with_message_id(UUID::build())
                .with_comm_status(status.code.enum_value_or(UCode::UNKNOWN))
                .build_with_protobuf_payload(&status)
                .expect("Error building response message"),
        };

        // 2. Respond to caller
        self.up_subscription
            .up_transport
            .send(message)
            .await
            .expect("Error sending response message");
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
