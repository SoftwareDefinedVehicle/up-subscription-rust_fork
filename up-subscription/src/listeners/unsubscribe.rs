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
    use std::str::FromStr;
    use test_case::test_case;

    use up_rust::core::usubscription::{SubscriptionResponse, UnsubscribeResponse};
    use up_rust::UUri;

    use super::*;
    use crate::helpers;
    use crate::tests::{test_lib, test_objects};
    use crate::usubscription;

    #[test_case(UUri::default(); "Standard local subscription")]
    #[tokio::test]
    async fn test_unsubscribe_listener(_uuri: UUri) {
        // setup
        test_lib::before_test();
        let (usubscription, mut send_receiver) =
            test_objects::get_mock_for_listeners::<UnsubscribeResponse>();
        let listener = UnsubscribeListener::new(usubscription.clone());

        // create request object(s)
        let unsubscribe_request = test_objects::unsubscribe_request(
            test_objects::local_topic1_uri(),
            test_objects::subscriber_info1(),
        );
        let msg = UMessageBuilder::request(
            usubscription.subscribe_uuri(),
            UUri::from_str(test_objects::UENTITY_OWN_URI).unwrap(),
            usubscription::UP_REMOTE_TTL,
        )
        .build_with_protobuf_payload(&unsubscribe_request)
        .unwrap();

        // send request
        listener.on_receive(msg).await;

        // receive and assert response
        let received = send_receiver.recv().await;
        assert!(
            received.is_some(),
            "Expected to receive some subscribe response"
        );
    }
}
