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

use up_rust::UMessage;

// This determines whether two UMessages are equivalent - meaning, whether they have the same
// properties and content EXCEPT for the message IDs.
// This is useful for comparing UMessages in test cases where the ID is random-generated and so cannot usefully be part
// of an assertion-comparison between UMessage objects built for the test case and the UMessage generated by tested code.
pub(crate) fn is_equivalent_umessage(first_message: &UMessage, other_message: &UMessage) -> bool {
    first_message.attributes.commstatus == other_message.attributes.commstatus
        && first_message.attributes.payload_format == other_message.attributes.payload_format
        && first_message.attributes.permission_level == other_message.attributes.permission_level
        && first_message.attributes.priority == other_message.attributes.priority
        && first_message.attributes.reqid == other_message.attributes.reqid
        && first_message.attributes.sink == other_message.attributes.sink
        && first_message.attributes.source == other_message.attributes.source
        && first_message.attributes.special_fields == other_message.attributes.special_fields
        && first_message.attributes.token == other_message.attributes.token
        && first_message.attributes.type_ == other_message.attributes.type_
        && first_message.attributes.ttl == other_message.attributes.ttl
        && first_message.payload == other_message.payload
        && first_message.special_fields == other_message.special_fields
}

// This module holds a variety of object mocks and associated helper methods, to be used in tests
#[cfg(test)]
pub(crate) mod mocks {
    use async_trait::async_trait;
    use mockall::mock;
    use std::sync::{Arc, Mutex};

    use up_rust::UMessageBuilder;
    use up_rust::{
        communication::{CallOptions, RpcClient, ServiceInvocationError, UPayload},
        MockTransport, UListener, UMessage, UStatus, UUri,
    };

    use crate::test_lib;

    mock! {
        pub(crate) RpcClientMock {}

        #[async_trait]
        impl RpcClient for RpcClientMock {
            async fn invoke_method(
                &self,
                method: UUri,
                call_options: CallOptions,
                payload: Option<UPayload>,
            ) -> Result<Option<UPayload>, ServiceInvocationError>;
        }
    }

    pub(crate) fn utransport_mock_for_notification_manager(
        expected_messages: Vec<UMessage>,
    ) -> MockTransport {
        let mut mock_transport = MockTransport::default();

        for expected_message in expected_messages {
            mock_transport
                .expect_do_send()
                .once()
                .withf(move |message| test_lib::is_equivalent_umessage(message, &expected_message))
                .return_const(Ok(()));
        }

        mock_transport
    }

    // This creates a mock UTransport object to be used for RPC testing scenarios where it's only important to
    // receive the expected response when calling `invoke(_proto)_method()`.
    //
    // The mock has the following behavior:
    //  - `register_listener()` can be called multiple times, each registered listener will receive the expected response(s)
    //  - each request can be called multiple times, each call will receive the assicated response message via all registered listeners
    pub(crate) async fn utransport_mock_for_rpc<T, R>(
        requests_responses: Vec<(T, R)>,
    ) -> MockTransport
    where
        T: protobuf::MessageFull,
        R: protobuf::MessageFull,
    {
        let test_list = Arc::new(Mutex::new(Vec::<Arc<dyn UListener>>::new()));

        // When listener is registered, we store it to send the expected responses to later
        let mut mock_transport = MockTransport::default();
        let test_list_clone = test_list.clone();
        mock_transport.expect_do_register_listener().returning(
            move |_source_filter, _sink_filter, listener| {
                test_list_clone.lock().unwrap().push(listener.clone());
                Ok(())
            },
        );

        // for each request-response tuple, set up intercept for send() calls, initiate sending of response UMessage
        for (expected_request, desired_response) in requests_responses {
            let test_list_clone = test_list.clone();

            mock_transport
                .expect_do_send()
                .withf(move |request_message: &UMessage| {
                    // We just compare the payload of the received messasge - this a pseudo transport for RPC,
                    // so we're only interested to get the payloads correct (and not the whole message)
                    let expected_payload =
                        match UPayload::try_from_protobuf(expected_request.clone()) {
                            Ok(payload) => payload,
                            Err(_) => return false,
                        };
                    request_message.payload.as_ref() == Some(&expected_payload.payload())
                })
                .returning(move |request_message| {
                    // Build response message
                    let res_msg =
                        UMessageBuilder::response_for_request(&request_message.attributes)
                            .build_with_protobuf_payload(&desired_response.clone())
                            .map_err(|e| {
                                UStatus::fail_with_code(
                                    up_rust::UCode::INTERNAL,
                                    format!("Error building response: {}", e),
                                )
                            })
                            .unwrap();

                    // Send the response to all registered listeners
                    test_list_clone.lock().unwrap().iter().for_each(|listener| {
                        let listener_clone = listener.clone();
                        let response_msg_clone = res_msg.clone();
                        tokio::spawn(async move {
                            listener_clone.on_receive(response_msg_clone).await;
                        });
                    });
                    Ok(())
                });
        }
        mock_transport
    }
}

// Various methods for constructing helper objects to be used in tests
#[cfg(test)]
pub(crate) mod helpers {
    use up_rust::core::usubscription::{
        SubscriberInfo, SubscriptionRequest, UnsubscribeRequest, USUBSCRIPTION_TYPE_ID,
        USUBSCRIPTION_VERSION_MAJOR,
    };
    use up_rust::UUri;

    pub(crate) const LOCAL_AUTHORITY: &str = "LOCAL";
    pub(crate) const REMOTE_AUTHORITY: &str = "REMOTE";

    const SUBSCRIBER1_ID: u32 = 0x0000_1000;
    const SUBSCRIBER1_VERSION: u32 = 0x0000_0001;
    const SUBSCRIBER1_RESOURCE: u32 = 0x000_1000;

    const SUBSCRIBER2_ID: u32 = 0x0000_2000;
    const SUBSCRIBER2_VERSION: u32 = 0x0000_0001;
    const SUBSCRIBER2_RESOURCE: u32 = 0x0000_1000;

    const SUBSCRIBER3_ID: u32 = 0x0000_3000;
    const SUBSCRIBER3_VERSION: u32 = 0x0000_0001;
    const SUBSCRIBER3_RESOURCE: u32 = 0x0000_1000;

    #[allow(dead_code)] // final decision on removing this to happen after functional spec alignment is complete
    const NOTIFICATION_TOPIC_ID: u32 = 0x001_0000;
    #[allow(dead_code)] // final decision on removing this to happen after functional spec alignment is complete
    const NOTIFICATION_TOPIC_VERSION: u32 = 0x0000_0001;
    #[allow(dead_code)] // final decision on removing this to happen after functional spec alignment is complete
    const NOTIFICATION_TOPIC_RESOURCE: u32 = 0x0000_8001;

    const TOPIC_LOCAL1_ID: u32 = 0x0010_0000;
    const TOPIC_LOCAL1_VERSION: u32 = 0x0000_0001;
    const TOPIC_LOCAL1_RESOURCE: u32 = 0x00A9_8AC7;

    const TOPIC_LOCAL2_ID: u32 = 0x0020_0000;
    const TOPIC_LOCAL2_VERSION: u32 = 0x0000_0001;
    const TOPIC_LOCAL2_RESOURCE: u32 = 0x0153_158E;

    const TOPIC_REMOTE1_ID: u32 = 0x0000_5000;
    const TOPIC_REMOTE1_VERSION: u32 = 0x0000_0001;
    const TOPIC_REMOTE1_RESOURCE: u32 = 0x2000_0000;

    #[allow(dead_code)] // final decision on removing this to happen after functional spec alignment is complete
    pub(crate) const UENTITY_OWN_URI: &str = "/7777/1/0";

    pub(crate) fn subscriber_uri1() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: SUBSCRIBER1_ID,
            ue_version_major: SUBSCRIBER1_VERSION,
            resource_id: SUBSCRIBER1_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn subscriber_uri2() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: SUBSCRIBER2_ID,
            ue_version_major: SUBSCRIBER2_VERSION,
            resource_id: SUBSCRIBER2_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn subscriber_uri3() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: SUBSCRIBER3_ID,
            ue_version_major: SUBSCRIBER3_VERSION,
            resource_id: SUBSCRIBER3_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn subscriber_info1() -> SubscriberInfo {
        SubscriberInfo {
            uri: Some(subscriber_uri1()).into(),
            ..Default::default()
        }
    }

    pub(crate) fn local_usubscription_service_uri() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: USUBSCRIPTION_TYPE_ID,
            ue_version_major: USUBSCRIPTION_VERSION_MAJOR as u32,
            ..Default::default()
        }
    }

    #[allow(dead_code)] // final decision on removing this to happen after functional spec alignment is complete
    pub(crate) fn notification_topic_uri() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: NOTIFICATION_TOPIC_ID,
            ue_version_major: NOTIFICATION_TOPIC_VERSION,
            resource_id: NOTIFICATION_TOPIC_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn local_topic1_uri() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: TOPIC_LOCAL1_ID,
            ue_version_major: TOPIC_LOCAL1_VERSION,
            resource_id: TOPIC_LOCAL1_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn local_topic2_uri() -> UUri {
        UUri {
            authority_name: LOCAL_AUTHORITY.into(),
            ue_id: TOPIC_LOCAL2_ID,
            ue_version_major: TOPIC_LOCAL2_VERSION,
            resource_id: TOPIC_LOCAL2_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn remote_topic1_uri() -> UUri {
        UUri {
            authority_name: REMOTE_AUTHORITY.into(),
            ue_id: TOPIC_REMOTE1_ID,
            ue_version_major: TOPIC_REMOTE1_VERSION,
            resource_id: TOPIC_REMOTE1_RESOURCE,
            ..Default::default()
        }
    }

    pub(crate) fn subscription_request(topic: UUri) -> SubscriptionRequest {
        SubscriptionRequest {
            topic: Some(topic).into(),
            ..Default::default()
        }
    }

    pub(crate) fn unsubscribe_request(topic: UUri) -> UnsubscribeRequest {
        UnsubscribeRequest {
            topic: Some(topic).into(),
            ..Default::default()
        }
    }
}
