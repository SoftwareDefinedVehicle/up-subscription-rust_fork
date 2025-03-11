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

use log::*;
#[cfg(test)]
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::{mpsc::Receiver, oneshot, Notify};

use up_rust::{
    core::usubscription::{
        usubscription_uri, SubscriberInfo, SubscriptionStatus, Update,
        RESOURCE_ID_SUBSCRIPTION_CHANGE,
    },
    UMessageBuilder, UTransport, UUri, UUID,
};

use crate::{helpers, persistency, usubscription, USubscriptionConfiguration};

// This is the core business logic for tracking and sending subscription update notifications. It is currently implemented as a single
// event-consuming function `notification_engine()`, which is supposed to be spawned into a task, and process the various notification
// `Events` that it can receive via tokio mpsc channel.

// This is the 'outside API' of notification handler
#[derive(Debug)]
pub(crate) enum NotificationEvent {
    AddNotifyee {
        subscriber: UUri,
        topic: UUri,
    },
    RemoveNotifyee {
        subscriber: UUri,
    },
    StateChange {
        subscriber: UUri,
        topic: UUri,
        status: SubscriptionStatus,
        respond_to: oneshot::Sender<()>,
    },
    // Purely for use during testing: get copy of current notifyee ledger
    #[cfg(test)]
    GetNotificationTopics {
        respond_to: oneshot::Sender<HashMap<UUri, UUri>>,
    },
    // Purely for use during testing: force-set new notifyees ledger
    #[cfg(test)]
    SetNotificationTopics {
        notification_topics_replacement: HashMap<UUri, UUri>,
        respond_to: oneshot::Sender<()>,
    },
}

// Keeps track of and sends subscription update notification to all registered update-notification channels.
// Interfacing with this purely works via channels.
pub(crate) async fn notification_engine(
    configuration: Arc<USubscriptionConfiguration>,
    up_transport: Arc<dyn UTransport>,
    mut events: Receiver<NotificationEvent>,
    shutdown: Arc<Notify>,
) {
    helpers::init_once();

    // keep track of which subscriber wants to be notified on which topic
    let mut notifications = persistency::NotificationStore::new(&configuration);

    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                None => {
                    error!("Problem with notification command channel, received None-event");
                    break
                },
                Some(event) => event,
            },
            _ = shutdown.notified() => {
                break
            },
        };
        match event {
            NotificationEvent::AddNotifyee { subscriber, topic } => {
                if !topic.is_event() {
                    error!("Topic UUri is not a valid event target");
                    break;
                }

                match notifications.add_notifyee(&subscriber, &topic) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Persistency failure {e}")
                    }
                };
            }
            NotificationEvent::RemoveNotifyee { subscriber } => {
                match notifications.remove_notifyee(&subscriber) {
                    Ok(_) => {}
                    Err(e) => {
                        error!("Persistency failure {e}")
                    }
                };
            }
            NotificationEvent::StateChange {
                subscriber,
                topic,
                status,
                respond_to,
            } => {
                let update = Update {
                    topic: Some(topic).into(),
                    subscriber: Some(SubscriberInfo {
                        uri: Some(subscriber.clone()).into(),
                        ..Default::default()
                    })
                    .into(),
                    status: Some(status).into(),
                    ..Default::default()
                };

                // Send Update message to general notification channel
                // as per usubscription.proto RegisterForNotifications(NotificationsRequest)
                match UMessageBuilder::publish(usubscription_uri(RESOURCE_ID_SUBSCRIPTION_CHANGE))
                    .with_message_id(UUID::build())
                    .build_with_protobuf_payload(&update)
                {
                    Err(e) => {
                        error!("Error building global update notification message: {e}");
                    }
                    Ok(update_msg) => {
                        if let Err(e) = up_transport.send(update_msg).await {
                            error!(
                                "Error sending global subscription-change update notification: {e}"
                            );
                        }
                    }
                }

                // Send Update message to any dedicated registered notification-subscribers
                if let Ok(topics) = notifications.get_topics() {
                    for topic_entry in topics {
                        debug!(
                            "Sending notification to ({}): topic {}, subscriber {}, status {}",
                            topic_entry.to_uri(usubscription::INCLUDE_SCHEMA),
                            update
                                .topic
                                .as_ref()
                                .unwrap_or_default()
                                .to_uri(usubscription::INCLUDE_SCHEMA),
                            update
                                .subscriber
                                .uri
                                .as_ref()
                                .unwrap_or_default()
                                .to_uri(usubscription::INCLUDE_SCHEMA),
                            update.status.as_ref().unwrap_or_default()
                        );

                        match UMessageBuilder::publish(topic_entry.clone())
                            .build_with_protobuf_payload(&update)
                        {
                            Ok(update_msg) => {
                                let _r = up_transport.send(update_msg).await.inspect_err(|e|
                                    warn!("Error sending susbcriber-specific subscription-change update notification: {e}")
                                );
                            }
                            Err(e) => {
                                error!("Error building susbcriber-specific update notification message: {e}");
                            }
                        }
                    }
                }

                let _r = respond_to.send(());
            }
            #[cfg(test)]
            NotificationEvent::GetNotificationTopics { respond_to } => {
                let _r = respond_to.send(
                    notifications
                        .get_data()
                        .expect("Error getting notification store contents"),
                );
            }
            #[cfg(test)]
            NotificationEvent::SetNotificationTopics {
                notification_topics_replacement,
                respond_to,
            } => {
                notifications
                    .set_data(notification_topics_replacement)
                    .expect("Error setting notification store contents");
                let _r = respond_to.send(());
            }
        }
    }
}
