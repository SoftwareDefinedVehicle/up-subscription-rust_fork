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
use log::*;
use std::sync::Arc;
use tokio::sync::mpsc::Sender;

use crate::{helpers, notification_manager::NotificationEvent};

use up_rust::{
    communication::{RequestHandler, ServiceInvocationError, UPayload},
    core::usubscription::{
        NotificationsRequest, NotificationsResponse, RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
    },
    UAttributes,
};

pub(crate) struct RegisterNotificationsRequestHandler {
    notification_sender: Arc<Sender<NotificationEvent>>,
}

impl RegisterNotificationsRequestHandler {
    pub(crate) fn new(notification_sender: Arc<Sender<NotificationEvent>>) -> Self {
        Self {
            notification_sender,
        }
    }
}

#[async_trait]
impl RequestHandler for RegisterNotificationsRequestHandler {
    async fn handle_request(
        &self,
        resource_id: u16,
        message_attributes: &UAttributes,
        request_payload: Option<UPayload>,
    ) -> Result<Option<UPayload>, ServiceInvocationError> {
        let (register_for_notifications_request, source) =
            helpers::extract_inputs::<NotificationsRequest>(
                RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
                resource_id,
                &request_payload,
                message_attributes,
            )?;
        let Some(topic) = register_for_notifications_request.topic.as_ref() else {
            return Err(ServiceInvocationError::InvalidArgument(
                "No topic defined in request".to_string(),
            ));
        };

        // Interact with notification manager backend
        let se = NotificationEvent::AddNotifyee {
            subscriber: source.clone(),
            topic: topic.clone(),
        };
        if let Err(e) = self.notification_sender.send(se).await {
            error!("Error communicating with notification manager: {e}");
            return Err(ServiceInvocationError::Internal(
                "Error processing request".to_string(),
            ));
        }

        // Build and return result
        let response_payload = UPayload::try_from_protobuf(NotificationsResponse::default())
            .map_err(|e| {
                ServiceInvocationError::Internal(format!("Error building response payload: {e}"))
            })?;

        Ok(Some(response_payload))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::sync::mpsc::{self};

    use crate::{helpers, tests::test_lib};

    #[tokio::test]
    async fn test_register_notification_success() {
        helpers::init_once();

        // create request and other required object(s)
        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };
        let request_payload = UPayload::try_from_protobuf(notification_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (notification_sender, mut notification_receiver) =
            mpsc::channel::<NotificationEvent>(1);

        // create and spawn off handler, to make all the asnync goodness work
        let request_handler =
            RegisterNotificationsRequestHandler::new(Arc::new(notification_sender));
        tokio::spawn(async move {
            let result = request_handler
                .handle_request(
                    RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
                    &message_attributes,
                    Some(request_payload),
                )
                .await
                .unwrap();

            assert!(result
                .unwrap()
                .extract_protobuf::<NotificationsResponse>()
                .is_ok());
        });

        // validate subscription manager interaction
        let notification_event = notification_receiver.recv().await.unwrap();
        match notification_event {
            NotificationEvent::AddNotifyee { subscriber, topic } => {
                assert_eq!(subscriber, test_lib::helpers::subscriber_uri1());
                assert_eq!(topic, test_lib::helpers::local_topic1_uri());
            }
            _ => panic!("Wrong event type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_resource_id() {
        helpers::init_once();

        // create request and other required object(s)
        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };
        let request_payload = UPayload::try_from_protobuf(notification_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler =
            RegisterNotificationsRequestHandler::new(Arc::new(notification_sender));

        let result = request_handler
            .handle_request(
                up_rust::core::usubscription::RESOURCE_ID_UNREGISTER_FOR_NOTIFICATIONS,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    #[tokio::test]
    async fn test_no_source_uri() {
        helpers::init_once();

        // create request and other required object(s)
        let notification_request = NotificationsRequest {
            topic: Some(test_lib::helpers::local_topic1_uri()).into(),
            ..Default::default()
        };
        let request_payload = UPayload::try_from_protobuf(notification_request.clone()).unwrap();
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler =
            RegisterNotificationsRequestHandler::new(Arc::new(notification_sender));

        let result = request_handler
            .handle_request(
                RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
                &UAttributes::default(),
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    #[tokio::test]
    async fn test_no_request_payload() {
        helpers::init_once();

        // create request and other required object(s)
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler =
            RegisterNotificationsRequestHandler::new(Arc::new(notification_sender));

        let result = request_handler
            .handle_request(
                RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
                &message_attributes,
                None,
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }

    #[tokio::test]
    async fn test_wrong_request_payload_type() {
        helpers::init_once();

        // create request and other required object(s)
        let subscribe_request =
            test_lib::helpers::unsubscribe_request(test_lib::helpers::local_topic1_uri());
        let request_payload = UPayload::try_from_protobuf(subscribe_request.clone()).unwrap();
        let message_attributes = UAttributes {
            source: Some(test_lib::helpers::subscriber_uri1()).into(),
            ..Default::default()
        };
        let (notification_sender, _) = mpsc::channel::<NotificationEvent>(1);

        // create handler and perform tested operation
        let request_handler =
            RegisterNotificationsRequestHandler::new(Arc::new(notification_sender));

        let result = request_handler
            .handle_request(
                RESOURCE_ID_REGISTER_FOR_NOTIFICATIONS,
                &message_attributes,
                Some(request_payload),
            )
            .await;

        assert!(result.is_err());
        match result.unwrap_err() {
            ServiceInvocationError::InvalidArgument(_) => {}
            _ => panic!("Wrong error type"),
        }
    }
}
