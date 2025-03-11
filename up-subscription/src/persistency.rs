/********************************************************************************
 * Copyright (c) 2025 Contributors to the Eclipse Foundation
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

use pickledb::{PickleDb, PickleDbDumpPolicy, SerializationMethod};
use protobuf::{Enum, Message};
use serde::de::Error;
#[cfg(test)]
use std::collections::HashMap;
use std::collections::HashSet;
use std::{convert::TryInto, path::PathBuf};

use up_rust::{core::usubscription::State as TopicState, UUri};

use crate::USubscriptionConfiguration;

// Whether to include 'up:' in serialized UUris
const PERSIST_UP_SCHEMA: bool = true;

#[derive(Debug)]
pub(crate) enum PersistencyError {
    InternalError(String),
    SerializationError(String),
}

impl PersistencyError {
    pub(crate) fn serialization_error<T>(message: T) -> PersistencyError
    where
        T: Into<String>,
    {
        Self::SerializationError(message.into())
    }

    pub(crate) fn internal_error<T>(message: T) -> PersistencyError
    where
        T: Into<String>,
    {
        Self::InternalError(message.into())
    }
}

impl std::fmt::Display for PersistencyError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::SerializationError(e) => f.write_fmt(format_args!("Serialization error: {}", e)),
            Self::InternalError(e) => f.write_fmt(format_args!("Internal error: {}", e)),
        }
    }
}

impl std::error::Error for PersistencyError {}

/// Persistent store for tracking subscriber-topic relationships
pub(crate) struct SubscriptionsStore {
    persistency: PickleDb,
}

impl SubscriptionsStore {
    const SUBSCRIPTION_STORE_NAME: &str = ".subscriptions.store";

    pub(crate) fn new(configuration: &USubscriptionConfiguration) -> SubscriptionsStore {
        SubscriptionsStore {
            persistency: get_store(
                SubscriptionsStore::SUBSCRIPTION_STORE_NAME.to_string(),
                configuration.persistency_path.clone(),
                configuration.persistency_enabled,
            ),
        }
    }

    /// Add a new topic-subscriber relationship to persistent storage
    /// * any such relationship in this store implies a subscription state of SUBSCRIBED, except for remote topics (refer to `RemoteTopicsStore`)
    /// * returns `Ok(true)` if this is the first subscription to this topic, `Ok(false)` otherwise
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn add_subscription(
        &mut self,
        subscriber: &UUri,
        topic: &UUri,
    ) -> Result<bool, PersistencyError> {
        // serialize inputs to types used in persistency
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);
        let subscriber_string = &subscriber.to_uri(PERSIST_UP_SCHEMA);

        Ok(
            if let Some(mut subscriber_list) = self.persistency.get::<HashSet<String>>(topic_string)
            {
                subscriber_list.insert(subscriber_string.clone());
                self.persistency
                    .set(topic_string, &subscriber_list)
                    .map_err(|e| {
                        PersistencyError::internal_error(format!(
                            "Error updating topic-subscriber list {e}"
                        ))
                    })?;
                false
            } else {
                self.persistency
                    .set(topic_string, &HashSet::from([subscriber_string.clone()]))
                    .map_err(|e| {
                        PersistencyError::internal_error(format!(
                            "Error adding new topic-subscriber {e}"
                        ))
                    })?;
                true
            },
        )
    }

    /// Removes a topic-subscriber combination from persistent storage
    /// * returns `Ok(true)` if this was the last subscriber to the topic, `Ok(false)` otherwise
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn remove_subscription(
        &mut self,
        subscriber: &UUri,
        topic: &UUri,
    ) -> Result<bool, PersistencyError> {
        // serialize inputs to types used in persistency
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);
        let subscriber_string = &subscriber.to_uri(PERSIST_UP_SCHEMA);

        if let Some(mut subscriber_list) = self.persistency.get::<HashSet<String>>(topic_string) {
            subscriber_list.remove(subscriber_string);

            if subscriber_list.is_empty() {
                let _r = self.persistency.rem(topic_string).map_err(|e| {
                    PersistencyError::internal_error(format!(
                        "Error removing topic-subscriber list {e}"
                    ))
                })?;
                return Ok(true);
            } else {
                self.persistency
                    .set(topic_string, &subscriber_list)
                    .map_err(|e| {
                        PersistencyError::internal_error(format!(
                            "Error storing updated topic-subscriber list {e}"
                        ))
                    })?;
            }
        };
        Ok(false)
    }

    /// Return a list of all subscribers of given topic
    /// * returns `Vec<UUri>` that contains all subscriber UUris registered for the topic
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_topic_subscribers(
        &self,
        topic: &UUri,
    ) -> Result<Vec<UUri>, PersistencyError> {
        let topic_string = &topic.to_uri(PERSIST_UP_SCHEMA);
        let mut subscribers = vec![];

        // This will get *every* client that subscribed to `topic` - no matter whether (in the case of remote subscriptions)
        // the remote topic is already fully SUBSCRIBED, of still SUSBCRIBED_PENDING
        if let Some(list) = self.persistency.get::<HashSet<String>>(topic_string) {
            for entry in list {
                subscribers.push(UUri::try_from(entry).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing subscriber uri {e}"
                    ))
                })?);
            }
        }

        Ok(subscribers)
    }

    /// Return a list of all topics subscribed to by given subscriber
    /// * returns `Vec<UUri>` that contains all topics subscribed to by subscriber
    /// * returns a `PersistencyError` in case of problems with serialization of data or manipulation of persist storage
    pub(crate) fn get_subscriber_topics(
        &self,
        subscriber: &UUri,
    ) -> Result<Vec<UUri>, PersistencyError> {
        let subscriber_string = &subscriber.to_uri(PERSIST_UP_SCHEMA);
        let mut result_subs: Vec<UUri> = Vec::new();

        for entry in self.persistency.iter() {
            if let Some(subscribers) = entry.get_value::<HashSet<String>>() {
                if subscribers.contains(subscriber_string) {
                    result_subs.push(UUri::try_from(entry.get_key()).map_err(|e| {
                        PersistencyError::serialization_error(format!(
                            "Error deserializing topic uri {e}"
                        ))
                    })?);
                }
            }
        }

        Ok(result_subs)
    }

    #[cfg(test)]
    pub(crate) fn get_data(
        &self,
    ) -> Result<HashMap<UUri, HashSet<UUri>>, Box<dyn std::error::Error>> {
        #[allow(clippy::mutable_key_type)]
        let mut map: HashMap<UUri, HashSet<UUri>> = HashMap::new();

        for entry in self.persistency.iter() {
            #[allow(clippy::mutable_key_type)]
            let mut topic_subscribers = HashSet::new();

            if let Some(list) = entry.get_value::<HashSet<String>>() {
                for entry in list {
                    topic_subscribers.insert(UUri::try_from(entry).map_err(|e| {
                        PersistencyError::serialization_error(format!(
                            "Error deserializing subscriber uri {e}"
                        ))
                    })?);
                }
            }

            map.insert(
                UUri::try_from(entry.get_key()).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing topic uri {e}"
                    ))
                })?,
                topic_subscribers,
            );
        }
        Ok(map)
    }

    #[cfg(test)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn set_data(
        &mut self,
        map: HashMap<UUri, HashSet<UUri>>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (topic, subscribers) in map {
            self.persistency
                .set(
                    &topic.to_uri(PERSIST_UP_SCHEMA),
                    &subscribers
                        .iter()
                        .map(|u| u.to_uri(PERSIST_UP_SCHEMA))
                        .collect::<HashSet<String>>(),
                )
                .map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error storing topic-subscriber data in persistency {e}"
                    ))
                })?;
        }
        Ok(())
    }
}

/// Persistent store for tracking remote topic status
pub(crate) struct RemoteTopicsStore {
    persistency: PickleDb,
}

impl RemoteTopicsStore {
    const PERSIST_UP_SCHEMA: bool = true;
    const REMOTE_TOPICS_STORE_NAME: &str = ".remote_topics.store";

    pub(crate) fn new(configuration: &USubscriptionConfiguration) -> RemoteTopicsStore {
        RemoteTopicsStore {
            persistency: get_store(
                RemoteTopicsStore::REMOTE_TOPICS_STORE_NAME.to_string(),
                configuration.persistency_path.clone(),
                configuration.persistency_enabled,
            ),
        }
    }

    /// Get subscription state of topic
    /// * returns `Ok(Some(TopicState))` (with current TopicState value) if topic exists in store, otherwise returns `Ok(None)`
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn get_topic_state(
        &self,
        topic: &UUri,
    ) -> Result<Option<TopicState>, PersistencyError> {
        let topic_string = &topic.to_uri(Self::PERSIST_UP_SCHEMA);

        Ok(if self.persistency.exists(topic_string) {
            let bytes = self.persistency.get::<Vec<u8>>(topic_string).ok_or(
                PersistencyError::internal_error(
                    "Error retrieving remote topic state from persistency",
                ),
            )?;
            Some(deserialize_topic_state(&bytes).map_err(|e| {
                PersistencyError::serialization_error(format!(
                    "Error deserializing topic state {e}"
                ))
            })?)
        } else {
            None
        })
    }

    /// Update subscription state of topic in remote-topics store
    /// * returns `Ok(TopicState)` (with updated TopicState value) if state update is successful
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn set_topic_state(
        &mut self,
        topic: &UUri,
        state: TopicState,
    ) -> Result<TopicState, PersistencyError> {
        let topic_string = &topic.to_uri(Self::PERSIST_UP_SCHEMA);
        let bytes = serialize_topic_state(&state).map_err(|e| {
            PersistencyError::serialization_error(format!("Error serializing topic state {e}"))
        })?;

        self.persistency.set(topic_string, &bytes).map_err(|e| {
            PersistencyError::internal_error(format!(
                "Error setting remote topic state in persistency {e}"
            ))
        })?;

        Ok(state)
    }

    /// Get subscription state of remote topic, or adds new remote-topic with state TopicState::SUBSCRIBE_PENDING if topic is new
    /// * returns `Ok(TopicState)` (where TopicState is the new topic state)
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn add_topic_or_get_state(
        &mut self,
        topic: &UUri,
    ) -> Result<TopicState, PersistencyError> {
        let topic_string = &topic.to_uri(Self::PERSIST_UP_SCHEMA);

        // if remote topic already has been registered, retrieve state
        Ok(if self.persistency.exists(topic_string) {
            let bytes = self.persistency.get::<Vec<u8>>(topic_string).ok_or(
                PersistencyError::internal_error(
                    "Error retrieving remote topic state from persistency",
                ),
            )?;
            deserialize_topic_state(&bytes).map_err(|e| {
                PersistencyError::serialization_error(format!(
                    "Error deserializing topic state {e}"
                ))
            })?
        } else {
            self.set_topic_state(topic, TopicState::SUBSCRIBE_PENDING)?
        })
    }

    #[cfg(test)]
    pub(crate) fn get_data(&self) -> Result<HashMap<UUri, TopicState>, Box<dyn std::error::Error>> {
        #[allow(clippy::mutable_key_type)]
        let mut map: HashMap<UUri, TopicState> = HashMap::new();

        for kv in self.persistency.iter() {
            if let Some(bytes) = kv.get_value::<Vec<u8>>() {
                let value = deserialize_topic_state(&bytes)?;
                map.insert(UUri::try_from(kv.get_key())?, value);
            }
        }

        Ok(map)
    }

    #[cfg(test)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn set_data(
        &mut self,
        map: HashMap<UUri, TopicState>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (key, value) in map {
            let _r = self.persistency.set(
                &key.to_uri(Self::PERSIST_UP_SCHEMA),
                &serialize_topic_state(&value)?,
            );
        }
        Ok(())
    }
}

pub(crate) struct NotificationStore {
    persistency: PickleDb,
}

impl NotificationStore {
    const PERSIST_UP_SCHEMA: bool = true;
    const NOTIFICATION_STORE_NAME: &str = ".notification.store";

    pub(crate) fn new(configuration: &USubscriptionConfiguration) -> NotificationStore {
        NotificationStore {
            persistency: get_store(
                NotificationStore::NOTIFICATION_STORE_NAME.to_string(),
                configuration.persistency_path.clone(),
                configuration.persistency_enabled,
            ),
        }
    }

    /// Add subscriber to custom-notifications store
    /// * return `Ok(())` on success
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn add_notifyee(
        &mut self,
        subscriber: &UUri,
        topic: &UUri,
    ) -> Result<(), PersistencyError> {
        let subscriber_string = subscriber.to_uri(Self::PERSIST_UP_SCHEMA);
        let topic_bytes = serialize_uuri(topic)
            .map_err(|e| PersistencyError::serialization_error(e.to_string()))?;

        self.persistency
            .set(&subscriber_string, &topic_bytes)
            .map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error setting notification configuration in persistency {e}"
                ))
            })
    }

    /// Remove subscriber from custom-notifications store
    /// * return `Ok(())` on success
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn remove_notifyee(&mut self, subscriber: &UUri) -> Result<(), PersistencyError> {
        self.persistency
            .rem(&subscriber.to_uri(Self::PERSIST_UP_SCHEMA))
            .map_err(|e| {
                PersistencyError::internal_error(format!(
                    "Error setting notification configuration in persistency {e}"
                ))
            })?;

        Ok(())
    }

    /// Get a list of all topic keys from custom notification persistency
    /// * return a `Vec<UUri>` list of topic UUris
    /// * returns a `PersistencyError` in case something went wrong with data serialization or storage
    pub(crate) fn get_topics(&mut self) -> Result<Vec<UUri>, PersistencyError> {
        let mut result = vec![];

        for entry in self.persistency.iter() {
            if let Some(bytes) = entry.get_value::<Vec<u8>>() {
                let topic = deserialize_uuri(&bytes).map_err(|e| {
                    PersistencyError::serialization_error(format!(
                        "Error deserializing notification topic {e}"
                    ))
                })?;
                result.push(topic);
            }
        }
        Ok(result)
    }

    #[cfg(test)]
    pub(crate) fn get_data(&self) -> Result<HashMap<UUri, UUri>, Box<dyn std::error::Error>> {
        #[allow(clippy::mutable_key_type)]
        let mut map: HashMap<UUri, UUri> = HashMap::new();

        for kv in self.persistency.iter() {
            if let Some(bytes) = kv.get_value::<Vec<u8>>() {
                let value = deserialize_uuri(&bytes)?;
                map.insert(UUri::try_from(kv.get_key())?, value);
            }
        }

        Ok(map)
    }

    #[cfg(test)]
    #[allow(clippy::mutable_key_type)]
    pub(crate) fn set_data(
        &mut self,
        map: HashMap<UUri, UUri>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        for (key, value) in map {
            let _r = self
                .persistency
                .set(&key.to_uri(PERSIST_UP_SCHEMA), &serialize_uuri(&value)?);
        }
        Ok(())
    }
}

// custom serialization functions
fn serialize_uuri(uuri: &UUri) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(uuri.write_to_bytes()?)
}

fn deserialize_uuri(bytes: &[u8]) -> Result<UUri, Box<dyn std::error::Error>> {
    Ok(UUri::parse_from_bytes(bytes)?)
}

fn serialize_topic_state(state: &TopicState) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    Ok(state.value().to_le_bytes().to_vec())
}

fn deserialize_topic_state(bytes: &[u8]) -> Result<TopicState, Box<dyn std::error::Error>> {
    Ok(
        TopicState::from_i32(i32::from_le_bytes(bytes[..4].try_into()?))
            .ok_or_else(|| serde::de::value::Error::custom("Invalid TopicState value"))?,
    )
}

// Return a notification store instance, configured according to a USubscriptionConfiguration
fn get_store(name: String, path: PathBuf, persistency_enabled: bool) -> PickleDb {
    // duplicate policy returns, because there is no way to `clone()` this thing - and I need two instances below for load / new calls
    let (path, policy_load, policy_new) = {
        let path = validate_and_append_filename(&path, &name)
            .unwrap_or_else(|e| panic!("Problem with persistency, invalid storage file name: {e}"));

        if persistency_enabled {
            (
                path,
                PickleDbDumpPolicy::AutoDump,
                PickleDbDumpPolicy::AutoDump,
            )
        } else {
            (
                path,
                PickleDbDumpPolicy::NeverDump,
                PickleDbDumpPolicy::NeverDump,
            )
        }
    };

    PickleDb::load(&path, policy_load, SerializationMethod::Bin)
        .unwrap_or_else(|_| PickleDb::new(&path, policy_new, SerializationMethod::Bin))
}

// Check whether a filename contains any relative/path traversal characters, combine with directory if all is well
fn validate_and_append_filename(dir: &PathBuf, filename: &str) -> Result<PathBuf, &'static str> {
    // Check if filename contains any path separators or special directory components
    if filename.contains('/')
        || filename.contains('\\')
        || filename == "."
        || filename == ".."
        || filename.is_empty()
    {
        return Err("filename contains path components");
    }

    let mut full_path = dir.clone();
    full_path.push(filename);

    // Additional safety check - verify the resulting path is actually under the original directory
    if !full_path.starts_with(dir) {
        return Err("path traversal attempt detected");
    }

    Ok(full_path)
}
