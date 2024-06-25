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
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc::UnboundedReceiver;

use up_rust::core::usubscription::{SubscriberInfo, SubscriptionStatus, Update};
use up_rust::{UMessageBuilder, UTransport, UUri, UUID};

use crate::usubscription;

#[derive(Debug)]
pub(crate) enum Event {
    AddNotifyee {
        subscriber: UUri,
        topic: UUri,
    },
    RemoveNotifyee {
        subscriber: UUri,
    },
    StateChange {
        subscriber: SubscriberInfo,
        topic: UUri,
        state: SubscriptionStatus,
    },
}

// Keeps track of and sends subscription update notification to all registered update-notification channels.
pub(crate) async fn noticiation_engine(
    mut events: UnboundedReceiver<Event>,
    up_transport: Arc<dyn UTransport>,
) {
    // keep track of which subscriber wants to be notified on which topic
    #[allow(clippy::mutable_key_type)]
    let mut notification_topics: HashMap<UUri, UUri> = HashMap::new();

    loop {
        let event = tokio::select! {
            event = events.recv() => match event {
                None => break,
                Some(event) => event,
            },
        };
        match event {
            Event::AddNotifyee { subscriber, topic } => {
                notification_topics.insert(subscriber, topic);
            }
            Event::RemoveNotifyee { subscriber } => {
                notification_topics.remove(&subscriber);
            }
            Event::StateChange {
                subscriber,
                topic,
                state,
            } => {
                let update = Update {
                    topic: Some(topic).into(),
                    subscriber: Some(subscriber.clone()).into(),
                    status: Some(state).into(),
                    ..Default::default()
                };

                for notification_topic in notification_topics.values() {
                    debug!(
                        "Sending notification to ({}): topic {}, subscriber {}, status {}",
                        notification_topic.to_uri(usubscription::INCLUDE_SCHEMA),
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
                    if let Ok(update_msg) = UMessageBuilder::publish(notification_topic.clone())
                        .with_message_id(UUID::build())
                        .build_with_protobuf_payload(&update)
                    {
                        if let Err(e) = up_transport.send(update_msg).await {
                            error!("Error sending subscription-change update notification: {e}");
                        }
                    }
                }
            }
        }
    }
}
