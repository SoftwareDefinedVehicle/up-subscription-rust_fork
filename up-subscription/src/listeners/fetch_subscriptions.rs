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

// MOVE THE BUSINESS LOGIC TESTING TO usubscription_tests.rs!!!


#[cfg(test)]
mod tests {
    use std::str::FromStr;
    use test_case::test_case;

    use up_rust::core::usubscription::{FetchSubscriptionsResponse, Request, SubscriberInfo};
    use up_rust::UUri;

    use super::*;
    use crate::tests::{test_lib, test_objects};
    use crate::usubscription;

    // Test simple turnaround from request going into listener and getting a matching response out.
    // Note: This might generate an error message in passing, due the the mock not implementing some business logic.
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri())], None, 1, UCode::OK; "Standard subscription, no filter")]
    #[test_case(vec![
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri())
                ],
            None, 1, UCode::OK; "More than one subscription for same subscriber-topic combo, no filter")]
    #[test_case(vec![
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                (test_objects::subscriber_info1(), test_objects::local_topic2_uri())
                ], 
            None, 2, UCode::OK; "More than one subscription for varied-topic combo, no filter")]
    #[test_case(vec![
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                (test_objects::subscriber_info2(), test_objects::local_topic1_uri())
                ], 
            None, 1, UCode::OK; "More than one subscription for varied-subscriber combo, no filter")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri())], Some(Request::Subscriber(test_objects::subscriber_info1())), 1, UCode::OK; "Standard subscription")]
    #[test_case(vec![
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri())
                ],
            Some(Request::Subscriber(test_objects::subscriber_info1())), 1, UCode::OK; "More than one subscription for same subscriber-topic combo")]
    #[test_case(vec![
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                (test_objects::subscriber_info1(), test_objects::local_topic2_uri())
                ], 
            Some(Request::Subscriber(test_objects::subscriber_info1())), 2, UCode::OK; "More than one subscription for varied-topic combo")]
    #[test_case(vec![
                (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                (test_objects::subscriber_info2(), test_objects::local_topic1_uri())
                ], 
            Some(Request::Subscriber(test_objects::subscriber_info1())), 1, UCode::OK; "More than one subscription for varied-subscriber combo")]
    #[tokio::test]
    async fn test_fetch_subscriptions_listener(
        subscriptions: Vec<(SubscriberInfo, UUri)>,
        request: Option<Request>,
        expected_count: usize,
        expected_code: UCode,
    ) {
        // setup
        test_lib::before_test();
        let (usubscription, mut send_receiver) =
            test_objects::get_mock_for_listeners::<FetchSubscriptionsResponse>();
        let listener = FetchSubscriptionsListener::new(usubscription.clone());

        // subscribe first - explicitly ignore potential errors, as we might try invalid data for some test scenarios
        for (subscriber, topic) in subscriptions {
            let _ = usubscription
                .subscribe(test_objects::subscription_request(topic, subscriber))
                .await;
        }

        // create request object(s)
        let fetch_susbcriptions_request = FetchSubscriptionsRequest {
            request: request.into(),
            ..Default::default()
        };
        let msg = UMessageBuilder::request(
            usubscription.fetch_subscriptions_uuri(),
            UUri::from_str(test_objects::UENTITY_OWN_URI).unwrap(),
            usubscription::UP_REMOTE_TTL,
        )
        .build_with_protobuf_payload(&fetch_susbcriptions_request)
        .unwrap();

        // send request
        listener.on_receive(msg).await;

        // receive and assert response
        let received = send_receiver
            .recv()
            .await
            .expect("Expected to receive some response")
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
            let subscriptions = received.unwrap().subscriptions;
            assert_eq!(subscriptions.len(), expected_count)
        } else {
            assert!(received.is_err(), "Expected negative/Err response");
            let status = received.err().unwrap();
            assert_eq!(status.code, expected_code.into());
        }
    }
}
