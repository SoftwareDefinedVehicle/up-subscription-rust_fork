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

use async_mutex::Mutex as AsyncMutex;
use async_trait::async_trait;
use futures::executor::block_on;
use log::*;
use protobuf::MessageField;
use std::collections::HashMap;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc::{self, UnboundedSender};

use crate::helpers::{self, *};
use crate::usubscription_notification::{self, Event};

use up_rust::core::usubscription::{
    FetchSubscribersRequest, FetchSubscribersResponse, FetchSubscriptionsRequest,
    FetchSubscriptionsResponse, NotificationsRequest, State, SubscriberInfo, Subscription,
    SubscriptionRequest, SubscriptionResponse, SubscriptionStatus, USubscription,
    UnsubscribeRequest,
};
use up_rust::{
    communication::CallOptions, communication::RpcClient, UCode, UStatus, UTransport, UUri,
};

// Version used in USubscriptionService UEntity
const USUBSCRIPTION_NAME: &str = "core.usubscription";

// Whether to include 'up:'  uProtocol schema prefix in URIs in log and error messages
pub const INCLUDE_SCHEMA: bool = false;

// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation
const UP_MAX_FETCH_SUBSCRIBERS_LEN: usize = 100;
// Maximum number of `Subscriber` entries to be returned in a `FetchSusbcriptions´ operation
const UP_MAX_FETCH_SUBSCRIPTIONS_LEN: usize = 100;
// Remote-subscribe operation ttl; 5 minutes in milliseconds, as per https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#6-timeout--retry-logic
pub const UP_REMOTE_TTL: u32 = 300000;

/// Data type for managing list of topic - subscriptionstate relations
#[derive(Clone, Debug)]
struct TopicLedger {
    topics: HashMap<UUri, SubscriptionStatus>, // topic - status relation
}

impl TopicLedger {
    fn new(topic: UUri, status: SubscriptionStatus) -> TopicLedger {
        let mut sl = TopicLedger {
            topics: HashMap::new(),
        };
        sl.insert(topic, status);
        sl
    }

    fn insert(&mut self, topic: UUri, status: SubscriptionStatus) -> Option<SubscriptionStatus> {
        self.topics.insert(topic, status)
    }
}

#[derive(Clone)]
pub struct USubscriptionService {
    pub name: String,
    pub uri: UUri,

    pub(crate) up_transport: Arc<dyn UTransport>,
    pub(crate) up_client: Arc<dyn RpcClient>,

    notification_sender: UnboundedSender<usubscription_notification::Event>,

    // list of subscribers, with their respective list of subscribed topic-state combinations
    subscriber_status: Arc<Mutex<HashMap<SubscriberInfo, TopicLedger>>>,

    // list of remote topics with number of subscriptions each
    remote_topic_subscriptions: Arc<AsyncMutex<HashMap<UUri, u32>>>,
}

impl USubscriptionService {
    pub fn new<T>(
        name: Option<T>,
        uri: UUri,
        up_transport: Arc<dyn UTransport>,
        up_client: Arc<dyn RpcClient>,
    ) -> USubscriptionService
    where
        T: Into<String>,
    {
        let (sender, receiver) = mpsc::unbounded_channel::<usubscription_notification::Event>();

        let up_transport_cloned = up_transport.clone();
        let _notification_hub = tokio::spawn(usubscription_notification::noticiation_engine(
            receiver,
            up_transport_cloned,
        ));

        USubscriptionService {
            name: name.map_or_else(|| USUBSCRIPTION_NAME.into(), |n| n.into()),
            up_transport,
            up_client,
            uri,

            notification_sender: sender,

            subscriber_status: Arc::new(Mutex::new(HashMap::new())),
            remote_topic_subscriptions: Arc::new(AsyncMutex::new(HashMap::new())),
        }
    }

    /// Update status of subscriber-topic combination to `state`
    ///
    /// - Will grab the mutex around subscriber_status
    /// - Will send an update message to the Notification channel by calling `send_update()`
    ///
    /// # Returns
    ///
    /// An `Option` containing
    ///  - `true` if this operation changed an existing subscriber/topic state combination
    ///  - `false` if this operation did not change an existing subscriber/topic state combination
    ///  - `None` if this subscriber/topic combination did not exist before (no existing state)
    pub(crate) fn update_status(
        &self,
        subscriber_info: &SubscriberInfo,
        topic: &UUri,
        state: &SubscriptionStatus,
    ) -> Option<bool> {
        let is_known_entry: bool = {
            let mut guard = self
                .subscriber_status
                .lock()
                .expect("Error getting subscriber_status lock");

            if let Some(ledger) = guard.get_mut(subscriber_info) {
                // we have seen this subscriber before,
                if ledger
                    .topics
                    .get(topic)
                    .is_some_and(|current| current.eq(state))
                {
                    // and this isn't actually an update to the current state, so do nothing
                    debug!("No change to subscriber-topic status, continuing...",);

                    // "There exists Some entry for this sub-topic combo already, but no state change"
                    return Some(false);
                }

                // otherwise, insert (replace) topic status with new one
                ledger.insert(topic.clone(), state.clone());

                true
            } else {
                // subscriber not seen before, insert new entry
                guard.insert(
                    subscriber_info.clone(),
                    TopicLedger::new(topic.clone(), state.clone()),
                );

                false
            }
        };

        // Notify update channel
        // self.send_update(topic.clone(), subscriber_info, state.clone());
        if let Err(e) = self.notification_sender.send(Event::StateChange {
            subscriber: subscriber_info.clone(),
            topic: topic.clone(),
            state: state.clone(),
        }) {
            error!("Error initiating subscription-change update notification: {e}");
        }

        if is_known_entry {
            // "There exists Some entry for this sub-topic combo already, and we did a state change"
            Some(true)
        } else {
            // "There existed None entry for this sub-topic combo"
            None
        }
    }

    // This function performs interaction with a remote uSubscription service
    // Implements https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#512-between-udevices
    pub(crate) async fn remote_subscribe(
        &self,
        subscription_request: &SubscriptionRequest,
    ) -> Result<(), UStatus> {
        let (subscriber, topic) = extract_subscriberinfo_topic(subscription_request)?;

        // Get and hold lock for entire method, as we want to be coherent about remote-topic subscriber count
        let mut guard = self.remote_topic_subscriptions.lock().await;
        if let Some(count) = guard.get(subscription_request.topic.as_ref().unwrap_or_default()) {
            if *count > 0 {
                debug!("Remote topic already subscribed by {count} subscribers, continuing...",);

                // Even though remote topic is already subscribed, ensure that we have local bookeeping up to date
                // with (potentially) new subscriber
                self.update_status(
                    subscriber,
                    topic,
                    &helpers::subscription_status_subscribed(),
                );
                guard.entry(topic.clone()).and_modify(|e| *e += 1);

                return Ok(());
            }
        }

        // set our own UURI as subscriber uri
        let mut remote_subscription_request = subscription_request.clone();
        remote_subscription_request.subscriber = MessageField::from(Some(SubscriberInfo {
            uri: Some(self.uri.clone()).into(),
            ..Default::default()
        }));
        let remote_usubscription_uri = self
            .make_remote_subscribe_uuri(&subscription_request.topic)
            .expect("Remote subscription request to local UUri");

        debug!(
            "Remote subscription request - forwarding to {}",
            remote_usubscription_uri.to_uri(INCLUDE_SCHEMA)
        );

        let subscription_response: SubscriptionResponse = self
            .up_client
            .invoke_proto_method(
                remote_usubscription_uri,
                CallOptions::new(UP_REMOTE_TTL, None, None, None),
                remote_subscription_request,
            )
            .await
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Error invoking remote subscription request: {e}"),
                )
            })?;

        if let Some(status) = subscription_response.status.as_ref() {
            if is_response_code_state(&subscription_response, State::SUBSCRIBED) {
                debug!("Got remote subscription response, state SUBSCRIBED");

                self.update_status(subscriber, topic, status);
                *guard.entry(topic.clone()).or_insert(0) += 1;
            } else {
                debug!("Got remote subscription response, some other state");
            }
        }

        Ok(())
    }

    // This function performs interaction with a remote uSubscription service
    // Implements https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#522-between-udevices
    pub(crate) async fn remote_unsubscribe(
        &self,
        unsubscribe_request: &UnsubscribeRequest,
    ) -> Result<(), UStatus> {
        let (subscriber, topic) = extract_unsubscriberinfo_topic(unsubscribe_request)?;

        // Get and hold lock for entire method, as we want to be coherent about remote-topic subscriber count
        let mut guard = self.remote_topic_subscriptions.lock().await;
        if let Some(count) = guard.get(unsubscribe_request.topic.as_ref().unwrap_or_default()) {
            if *count > 1 {
                debug!("Remote topic still subscribed by {count} subscribers, continuing...",);

                // Even though remote topic is being unsubscribed, ensure that we have local bookeeping up to date
                self.update_status(
                    subscriber,
                    topic,
                    &helpers::subscription_status_unsubscribed(),
                );
                guard.entry(topic.clone()).and_modify(|e| *e -= 1);

                return Ok(());
            }
        }

        // set our own UURI as subscriber uri
        let mut remote_unsubscribe_request = unsubscribe_request.clone();
        remote_unsubscribe_request.subscriber = MessageField::from(Some(SubscriberInfo {
            uri: Some(self.uri.clone()).into(),
            ..Default::default()
        }));
        let remote_usubscription_uri = self
            .make_remote_unsubscribe_uuri(&unsubscribe_request.topic)
            .expect("Remote unsubscription request to local UUri");

        debug!(
            "Remote unsubscription request - forwarding to {}",
            remote_usubscription_uri.to_uri(INCLUDE_SCHEMA)
        );

        let unsubscribe_response: UStatus = self
            .up_client
            .invoke_proto_method(
                remote_usubscription_uri,
                CallOptions::new(UP_REMOTE_TTL, None, None, None),
                remote_unsubscribe_request,
            )
            .await
            .map_err(|e| {
                UStatus::fail_with_code(
                    UCode::INTERNAL,
                    format!("Error invoking remote subscription request: {e}"),
                )
            })?;

        match unsubscribe_response.code.enum_value_or(UCode::UNKNOWN) {
            UCode::OK => {
                debug!("Got OK remote unsubscribe response");
                self.update_status(
                    subscriber,
                    topic,
                    &SubscriptionStatus {
                        state: State::UNSUBSCRIBED.into(),
                        ..Default::default()
                    },
                );
                guard.entry(topic.clone()).and_modify(|e| *e -= 1);
            }
            code => {
                debug!("Got {:?} remote unsubscribe response", code);
                return Err(UStatus::fail_with_code(
                    code,
                    "Error during remote unsubscribe",
                ));
            }
        };

        Ok(())
    }

    // Check if subscription_request-state_list combination represents a change to current subscription state
    // Return None if current state is not part of `states` (i.e., the passed parameters represent a state change),
    // otherwise return the specific `State` this subscriber-topic combination is in (no state-change is implied by passed parameters).
    pub(crate) fn represents_current_state(
        &self,
        subscriber: &SubscriberInfo,
        topic: &UUri,
        states: Vec<State>,
    ) -> Option<State> {
        states
            .into_iter()
            .find(|&state| self.is_subscribe_status(subscriber, topic, &state))
    }

    // Check if subcriber-topic combination is already in status `state`
    // Will grab the mutex around subscriber_status
    pub(crate) fn is_subscribe_status(
        &self,
        subscriber: &SubscriberInfo,
        topic: &UUri,
        state: &State,
    ) -> bool {
        if let Ok(guard) = self.subscriber_status.lock() {
            if let Some(entry) = guard.get(subscriber) {
                if let Some(topic_status) = entry.topics.get(topic) {
                    if topic_status.state.enum_value_or_default().eq(state) {
                        return true;
                    }
                }
            }
        }
        false
    }

    // Check if subcriber-topic combination is already in status `state`
    // Will grab the mutex around subscriber_status
    pub(crate) fn subscribe_status(
        &self,
        subscriber: &SubscriberInfo,
        topic: &UUri,
    ) -> Option<State> {
        if let Ok(guard) = self.subscriber_status.lock() {
            if let Some(entry) = guard.get(subscriber) {
                if let Some(topic_status) = entry.topics.get(topic) {
                    return Some(topic_status.state.enum_value_or_default());
                }
            }
        }
        None
    }
    // Return number of subscribers for remote `topic`
    pub(crate) fn remote_topic_subscribers(&self, topic: &UUri) -> u32 {
        if let Some(count) = block_on(self.remote_topic_subscriptions.lock())
            .get(topic)
            .copied()
        {
            return count;
        }

        0
    }

    // Return number of subscribers in a specific `State` of for a `topic`
    pub(crate) fn count_in_state(&self, topic: &UUri, state: &State) -> usize {
        let mut count: usize = 0;
        if let Ok(entries) = self.subscriber_status.lock() {
            count = entries
                .values()
                .flat_map(|subscriber| subscriber.topics.iter())
                .filter(|(t, s)| topic.eq(t) && state.eq(&s.state.enum_value_or_default()))
                .count();
        }
        count
    }
}

/// Implementation of https://github.com/eclipse-uprotocol/up-spec/blob/main/up-l3/usubscription/v3/README.adoc#usubscription
#[async_trait]
impl USubscription for USubscriptionService {
    /// Implementation of https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#51-subscription
    async fn subscribe(
        &self,
        subscription_request: SubscriptionRequest,
    ) -> Result<SubscriptionResponse, UStatus> {
        let (subscriber, topic) = helpers::extract_subscriberinfo_topic(&subscription_request)?;

        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        debug!(
            "Got SubscriptionRequest for topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber.uri.to_uri(INCLUDE_SCHEMA)
        );

        let status = if let Some(state) = self.represents_current_state(
            subscriber,
            topic,
            vec![State::SUBSCRIBED, State::SUBSCRIBE_PENDING],
        ) {
            // If this subscription request is redundant (subscriber-topic is already SUBSCRIBED or SUBSCRIBE_PENDING), do nothing
            debug!("Topic is already registered by this subscriber, continuing...");

            SubscriptionStatus {
                state: state.into(),
                message: "Topic is already registered by this subscriber".to_string(),
                ..Default::default()
            }
        } else {
            let status: SubscriptionStatus;

            // Subscription request to local or remote topic?
            if self.is_local_topic(topic) {
                debug!("topic URI is local ({})", topic.to_uri(INCLUDE_SCHEMA));

                status = subscription_status_subscribed()
            } else {
                debug!("topic URI is remote ({})", topic.to_uri(INCLUDE_SCHEMA));

                status = subscription_status_subscribepending();

                // Spawn out remote usubscription interaction, so we can return to our caller immediately while remote subscription happens
                let usubscription_cloned = Arc::new(self.clone());
                let subscription_request_cloned = subscription_request.clone();

                spawn_and_log_error(async move {
                    usubscription_cloned
                        .remote_subscribe(&subscription_request_cloned)
                        .await?;
                    Ok(())
                });
            }

            // Update subscriber info (this will also send subscription update notification)
            self.update_status(subscriber, topic, &status);
            status
        };

        // Return status and config
        Ok(SubscriptionResponse {
            topic: Some(topic.clone()).into(),
            status: Some(status).into(),
            ..Default::default()
        })
    }

    /// Implementation of https://github.com/eclipse-uprotocol/up-spec/tree/main/up-l3/usubscription/v3#52-unsubscribe
    async fn unsubscribe(&self, unsubscribe_request: UnsubscribeRequest) -> Result<(), UStatus> {
        let (subscriber, topic) = helpers::extract_unsubscriberinfo_topic(&unsubscribe_request)?;

        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty topic UUri",
            ));
        }
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        debug!(
            "Got UnsubscribeRequest for topic {}, from subscriber {}",
            topic.to_uri(INCLUDE_SCHEMA),
            subscriber.uri.to_uri(INCLUDE_SCHEMA)
        );

        if let Some(_state) = self.represents_current_state(
            subscriber,
            topic,
            vec![State::UNSUBSCRIBED, State::UNSUBSCRIBE_PENDING],
        ) {
            debug!("Topic is already unsubscribed by this subscriber, continuing...");
        } else {
            // Unsubscribe request to remote topic?
            let status = if self.is_local_topic(topic) {
                debug!("topic URI is local ({})", topic.to_uri(INCLUDE_SCHEMA));

                subscription_status_unsubscribed()
            } else {
                debug!("topic URI is remote ({})", topic.to_uri(INCLUDE_SCHEMA));

                // Spawn out remote usubscription interaction, so we can return to our caller immediately while remote subscription happens
                let usubscription_cloned = Arc::new(self.clone());
                let unsubscribe_request_cloned = unsubscribe_request.clone();
                spawn_and_log_error(async move {
                    usubscription_cloned
                        .remote_unsubscribe(&unsubscribe_request_cloned)
                        .await?;
                    Ok(())
                });

                subscription_status_unsubscribepending()
            };

            // Update subscriber info, this will also send subscription update notification
            self.update_status(subscriber, topic, &status);
        }

        Ok(())
    }

    async fn fetch_subscriptions(
        &self,
        fetch_subscriptions_request: FetchSubscriptionsRequest,
    ) -> Result<FetchSubscriptionsResponse, UStatus> {
        debug!("Got FetchSubscriptionsRequest");

        let FetchSubscriptionsRequest { offset, .. } = fetch_subscriptions_request;
        let offset = offset.unwrap_or(0) as usize;

        // Lock the mutex to access the HashMap
        let subscriber_status_lock = self
            .subscriber_status
            .lock()
            .expect("Error getting mutex lock");

        let mut subscriptions: Vec<Subscription> = Vec::new();
        for subscriber in subscriber_status_lock.keys() {
            if let Some(subscriber_ledger) = subscriber_status_lock.get(subscriber) {
                for topic in subscriber_ledger.topics.keys() {
                    let status = subscriber_ledger.topics.get(topic).unwrap_or_default();

                    subscriptions.push(Subscription {
                        topic: Some(topic.clone()).into(),
                        status: Some(status.clone()).into(),
                        subscriber: Some((*subscriber).clone()).into(),
                        ..Default::default()
                    });
                }
            }
        }

        // Trying to make some use of the offset and has_more_records fields...
        subscriptions.drain(..offset);
        let mut has_more = false;
        if subscriptions.len() > UP_MAX_FETCH_SUBSCRIPTIONS_LEN {
            subscriptions.truncate(UP_MAX_FETCH_SUBSCRIPTIONS_LEN);
            has_more = true;
        }

        debug!("Returning {} subscription entries", subscriptions.len());
        Ok(FetchSubscriptionsResponse {
            subscriptions,
            has_more_records: Some(has_more),
            ..Default::default()
        })
    }

    async fn register_for_notifications(
        &self,
        notifications_register_request: NotificationsRequest,
    ) -> Result<(), UStatus> {
        debug!("Got RegisterForNotifications");

        let NotificationsRequest {
            subscriber, topic, ..
        } = notifications_register_request;

        if topic.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty notification UUri",
            ));
        }
        if !self.is_local_topic(&topic) {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Cannot use remote UUri for notifications",
            ));
        }
        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        if let Err(e) = self.notification_sender.send(Event::AddNotifyee {
            subscriber: subscriber.uri.get_or_default().clone(),
            topic: topic.get_or_default().clone(),
        }) {
            error!("Error adding subscription-change subscriber: {e}");
        }
        Ok(())
    }

    async fn unregister_for_notifications(
        &self,
        notifications_unregister_request: NotificationsRequest,
    ) -> Result<(), UStatus> {
        debug!("Got UnregisterForNotifications");

        let NotificationsRequest { subscriber, .. } = notifications_unregister_request;

        if subscriber.is_empty() || subscriber.uri.is_empty() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                "Empty SubscriberInfo or subscriber UUri",
            ));
        }

        if let Err(e) = self.notification_sender.send(Event::RemoveNotifyee {
            subscriber: subscriber.uri.get_or_default().clone(),
        }) {
            error!("Error removing subscription-change subscriber: {e}");
        }
        Ok(())
    }

    async fn fetch_subscribers(
        &self,
        fetch_subscribers_request: FetchSubscribersRequest,
    ) -> Result<FetchSubscribersResponse, UStatus> {
        debug!("Got FetchSubscribersRequest");

        let FetchSubscribersRequest { topic, offset, .. } = fetch_subscribers_request;
        let offset = offset.unwrap_or(0) as usize;
        let topic = topic.unwrap_or_default();

        // Lock the mutex to access the HashMap
        let subscriber_status_lock = self
            .subscriber_status
            .lock()
            .expect("Error getting mutex lock");

        let mut subscribers: Vec<SubscriberInfo> = Vec::new();
        for subscriber in subscriber_status_lock.keys() {
            if let Some(subscriber_ledger) = subscriber_status_lock.get(subscriber) {
                for (topic_uri, status) in &subscriber_ledger.topics {
                    if topic_uri.eq(&topic)
                        && status.state.enum_value_or_default().eq(&State::SUBSCRIBED)
                    {
                        subscribers.push((*subscriber).clone());
                    }
                }
            }
        }

        // Trying to make some use of the offset and has_more_records fields...
        subscribers.drain(..offset);
        let mut has_more = false;
        if subscribers.len() > UP_MAX_FETCH_SUBSCRIBERS_LEN {
            subscribers.truncate(UP_MAX_FETCH_SUBSCRIBERS_LEN);
            has_more = true;
        }

        debug!("Returning {} subscriber entries", subscribers.len());
        Ok(FetchSubscribersResponse {
            subscribers,
            has_more_records: Some(has_more),
            ..Default::default()
        })
    }
}
