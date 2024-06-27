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

use up_rust::core::usubscription::{NotificationsRequest, USubscription};
use up_rust::{UCode, UListener, UMessage, UMessageBuilder, UStatus, UUID};

use crate::USubscriptionService;

#[derive(Clone)]
pub struct RegisterForNotificationsListener {
    up_subscription: Arc<USubscriptionService>,
}

impl RegisterForNotificationsListener {
    pub fn new(up_subscription: Arc<USubscriptionService>) -> Self {
        RegisterForNotificationsListener { up_subscription }
    }
}

#[async_trait]
impl UListener for RegisterForNotificationsListener {
    // Perform any business logic related to a `NotificationsRequest`
    async fn on_receive(&self, msg: UMessage) {
        let notifications_request: NotificationsRequest = msg
            .extract_protobuf()
            .expect("Expected NotificationsRequest payload");

        // 1. Check with backend
        let message = match self
            .up_subscription
            .register_for_notifications(notifications_request.clone())
            .await
        {
            Ok(()) => UMessageBuilder::response_for_request(msg.attributes.get_or_default())
                .with_message_id(UUID::build())
                .with_comm_status(UCode::OK)
                .build_with_protobuf_payload(&UStatus::ok())
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
    use std::str::FromStr;
    use test_case::test_case;

    use up_rust::core::usubscription::{NotificationsResponse, SubscriberInfo};
    use up_rust::UUri;

    use super::*;
    use crate::tests::{test_lib, test_objects};
    use crate::usubscription;

    // Test simple turnaround from request going into listener and getting a matching response out.
    // Note: This might generate an error message in passing, due the the mock not implementing some business logic.
    #[test_case(test_objects::subscriber_info1(), test_objects::notification_topic_uri(), UCode::OK; "Standard notification topic")]
    #[test_case(test_objects::subscriber_info1(), test_objects::remote_topic1_uri(), UCode::INVALID_ARGUMENT; "Remote notification topic")]
    #[test_case(SubscriberInfo::default(), test_objects::notification_topic_uri(), UCode::INVALID_ARGUMENT; "Empty subscriber")]
    #[test_case(test_objects::subscriber_info1(), UUri::default(), UCode::INVALID_ARGUMENT; "Empty notification topic UUri")]
    #[test_case(SubscriberInfo::default(), UUri::default(), UCode::INVALID_ARGUMENT; "Empty subscriber, empty notification topic UUri")]
    #[tokio::test]
    async fn test_register_for_notifications_listener(
        subscriber: SubscriberInfo,
        topic: UUri,
        expected_code: UCode,
    ) {
        // setup
        test_lib::before_test();
        let (usubscription, mut send_receiver) =
            test_objects::get_mock_for_listeners::<NotificationsResponse>();
        let listener = RegisterForNotificationsListener::new(usubscription.clone());

        // create request object(s)
        let register_request = NotificationsRequest {
            subscriber: Some(subscriber).into(),
            topic: Some(topic).into(),
            ..Default::default()
        };
        let msg = UMessageBuilder::request(
            usubscription.register_for_notifications_uuri(),
            UUri::from_str(test_objects::UENTITY_OWN_URI).unwrap(),
            usubscription::UP_REMOTE_TTL,
        )
        .build_with_protobuf_payload(&register_request)
        .unwrap();

        // send request
        listener.on_receive(msg).await;

        // receive and assert response
        let received = send_receiver
            .recv()
            .await
            .expect("Expected to receive some subscribe response")
            // A bit flaky - but this is to have a feedback channel for the specific condition where the response message sent from
            // the listener is missing the commstatus attribute - refer to `MockForListeners.send()`. Using UCode::UNKNOWN for this
            // condition, as this isn't returned anywhere else from the usubscription implementation.
            .inspect_err(|e| {
                if e.code == UCode::UNKNOWN.into() {
                    let msg = e.message.clone().unwrap();
                    panic!("Problem with return message sent by listener: {msg}");
                }
            });

        if expected_code == UCode::OK {
            assert!(received.is_ok(), "Expected positive/Ok response");
        } else {
            assert!(received.is_err(), "Expected negative/Err response");
            let status = received.err().unwrap();
            assert_eq!(status.code, expected_code.into());
        }
    }
}
