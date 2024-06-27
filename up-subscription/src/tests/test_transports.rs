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
use protobuf::Message;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use up_rust::core::usubscription::{
    State, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus, UnsubscribeRequest,
    UnsubscribeResponse, Update,
};
use up_rust::{
    communication::CallOptions, communication::RpcClient, communication::ServiceInvocationError,
    communication::UPayload, UCode, UListener, UMessage, UStatus, UTransport, UUri,
};

use crate::common::helpers;
use crate::tests::NotificationTuple;

// TEST TRANSPORT FOR LOCAL NOTIFICATION FUNCTIONALITY
// This serves as both
// - a mock for testing local-usubscription subscription and notification management scenarios
//   - via implementation of the UTransport::send() method, which mirrors recieved data back to an outside Receiver (which would be the calling test case)
// - a mock for testing remote-subscribe and remote-unsubscribe behavior
//   - via implementation of the RpcClient::invoke_method() method with some simply dummy checking and response logic
//   - optionally (if given a corresponding Sender object), mirror received (remote-)subscribe data back from invoke_method() mock
//
pub(crate) struct TransportMock {
    own_uri: UUri,
    send_sender: Sender<NotificationTuple>,
    invoke_method_sender: Option<Sender<NotificationTuple>>,
}

impl TransportMock {
    pub(crate) fn new(
        remote_mock_uri: UUri,
        send_sender: Sender<NotificationTuple>,
        invoke_method_sender: Option<Sender<NotificationTuple>>,
    ) -> TransportMock {
        TransportMock {
            own_uri: remote_mock_uri,
            send_sender,
            invoke_method_sender,
        }
    }
}

#[async_trait]
impl UTransport for TransportMock {
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        let update: Update = message.extract_protobuf().unwrap();

        let tuple = (
            update.subscriber.unwrap_or_default(),
            update.topic.unwrap_or_default(),
            update.status.unwrap_or_default(),
        );
        self.send_sender
            .send(tuple)
            .await
            .expect("Error sending update properties");

        Ok(())
    }

    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        todo!()
    }

    async fn register_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        todo!()
    }

    async fn unregister_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        todo!()
    }
}

#[async_trait]
impl RpcClient for TransportMock {
    async fn invoke_method(
        &self,
        method: UUri,
        _call_options: CallOptions,
        payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        let payload = payload.expect("Expecting a payload here");

        match method.resource_id {
            crate::usubscription_uris::UP_SUBSCRIBE_ID => {
                let subscription_request: SubscriptionRequest = payload
                    .extract_protobuf()
                    .expect("Error unpacking SubscriptionRequest");

                let (subscriber, remote_topic) =
                    helpers::extract_subscriberinfo_topic(&subscription_request)
                        .expect("Error extracting SubscriptionRequest info");

                assert!(!remote_topic.authority_name.is_empty());
                assert_eq!(self.own_uri.authority_name, *remote_topic.authority_name);

                let susresp = SubscriptionResponse {
                    topic: subscription_request.topic.clone(),
                    status: Some(SubscriptionStatus {
                        state: State::SUBSCRIBED.into(),
                        ..Default::default()
                    })
                    .into(),
                    ..Default::default()
                };

                if let Some(remote_state_sender) = &self.invoke_method_sender {
                    let tuple = (
                        subscriber.clone(),
                        susresp.topic.as_ref().unwrap_or_default().clone(),
                        susresp.status.as_ref().unwrap_or_default().clone(),
                    );
                    remote_state_sender
                        .send(tuple)
                        .await
                        .expect("Error sending state update properties");
                }

                Ok(UPayload::try_from_protobuf(susresp).ok())
            }
            crate::usubscription_uris::UP_UNSUBSCRIBE_ID => {
                let unsubscribe_request: UnsubscribeRequest = payload
                    .extract_protobuf()
                    .expect("Error unpacking SubscriptionRequest");

                let (_subscriber, remote_topic) =
                    helpers::extract_unsubscriberinfo_topic(&unsubscribe_request)
                        .expect("Error extracting SubscriptionRequest info");

                assert!(!remote_topic.authority_name.is_empty());
                assert_eq!(self.own_uri.authority_name, *remote_topic.authority_name);

                Ok(UPayload::try_from_protobuf(UnsubscribeResponse::default()).ok())
            }

            _ => Err(ServiceInvocationError::RpcError(UStatus::fail_with_code(
                up_rust::UCode::UNIMPLEMENTED,
                "Not implemented".to_string(),
            ))),
        }
    }
}

// The idea here is to have a simple mirroring-transport, which just gives back whatever response object was sent in the first place
pub(crate) struct MockForListeners<T: Message> {
    send_sender: Sender<Result<T, UStatus>>,
}

impl<T> MockForListeners<T>
where
    T: Message,
{
    pub(crate) fn new(send_sender: Sender<Result<T, UStatus>>) -> MockForListeners<T> {
        MockForListeners::<T> { send_sender }
    }
}

#[async_trait]
impl<T> UTransport for MockForListeners<T>
where
    T: Message,
{
    async fn send(&self, message: UMessage) -> Result<(), UStatus> {
        if message.attributes.is_none() {
            return self
                .send_sender
                .send(Err(UStatus::fail_with_code(
                    UCode::UNKNOWN,
                    "UMessage is missing message attributes",
                )))
                .await
                .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()));
        }
        if message.attributes.as_ref().unwrap().commstatus.is_none() {
            return self
                .send_sender
                .send(Err(UStatus::fail_with_code(
                    UCode::UNKNOWN,
                    "UMessage is missing commstatus attribute",
                )))
                .await
                .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()));
        }

        let msg: Result<T, UStatus> = if message
            .attributes
            .commstatus
            .unwrap_or_default()
            .enum_value_or_default()
            == UCode::OK
        {
            Ok(message.extract_protobuf::<T>().unwrap())
        } else {
            Err(message.extract_protobuf::<UStatus>().unwrap())
        };

        self.send_sender
            .send(msg)
            .await
            .map_err(|e| UStatus::fail_with_code(UCode::INTERNAL, e.to_string()))
    }

    async fn receive(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
    ) -> Result<UMessage, UStatus> {
        todo!()
    }

    async fn register_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        todo!()
    }

    async fn unregister_listener(
        &self,
        _source_filter: &UUri,
        _sink_filter: Option<&UUri>,
        _listener: Arc<dyn UListener>,
    ) -> Result<(), UStatus> {
        todo!()
    }
}

#[async_trait]
impl<T> RpcClient for MockForListeners<T>
where
    T: Message,
{
    async fn invoke_method(
        &self,
        _method: UUri,
        _call_options: CallOptions,
        _payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        // We're doing nothing here
        Ok(None)
    }
}
