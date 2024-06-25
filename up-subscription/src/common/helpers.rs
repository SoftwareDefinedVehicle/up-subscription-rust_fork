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
use std::future::Future;
use tokio::task;

use up_rust::core::usubscription::{
    State, SubscriberInfo, SubscriptionRequest, SubscriptionResponse, SubscriptionStatus,
    UnsubscribeRequest,
};
use up_rust::{UCode, UStatus, UUri};

type SpawnResult<T> = std::result::Result<T, Box<dyn std::error::Error + Send + Sync>>;
pub(crate) fn spawn_and_log_error<F>(fut: F) -> task::JoinHandle<()>
where
    F: Future<Output = SpawnResult<()>> + Send + 'static,
{
    task::spawn(async move {
        if let Err(e) = fut.await {
            error!("{}", e)
        }
    })
}

pub fn subscription_status_subscribed() -> SubscriptionStatus {
    SubscriptionStatus {
        state: State::SUBSCRIBED.into(),
        ..Default::default()
    }
}

pub fn subscription_status_subscribepending() -> SubscriptionStatus {
    SubscriptionStatus {
        state: State::SUBSCRIBE_PENDING.into(),
        ..Default::default()
    }
}

pub fn subscription_status_unsubscribed() -> SubscriptionStatus {
    SubscriptionStatus {
        state: State::UNSUBSCRIBED.into(),
        ..Default::default()
    }
}

pub fn subscription_status_unsubscribepending() -> SubscriptionStatus {
    SubscriptionStatus {
        state: State::UNSUBSCRIBE_PENDING.into(),
        ..Default::default()
    }
}

pub fn extract_subscriber_info(
    subscription_request: &SubscriptionRequest,
) -> Result<&SubscriberInfo, UStatus> {
    if let Some(subscriber) = subscription_request.subscriber.as_ref() {
        if let Some(uri) = subscriber.uri.as_ref() {
            if let Err(error) = uri.verify_no_wildcards() {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!("Subscriber URI must not contain wildcards: {}", error),
                ));
            }
        }

        return Ok(subscriber);
    }

    Err(UStatus::fail_with_code(
        UCode::INVALID_ARGUMENT,
        "SubscriberInfo is missing subscriber Uri",
    ))
}

pub fn extract_subscription_topic(
    subscription_request: &SubscriptionRequest,
) -> Result<&UUri, UStatus> {
    if let Some(topic) = subscription_request.topic.as_ref() {
        if let Err(error) = topic.verify_no_wildcards() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("Subscriber URI must not contain wildcards: {}", error),
            ));
        }

        return Ok(topic);
    }

    Err(UStatus::fail_with_code(
        UCode::INVALID_ARGUMENT,
        "SubscriberInfo is missing subscriber Uri",
    ))
}

// Extract SubscriberInfo and topic UUri from SubscriptionRequest
pub fn extract_subscriberinfo_topic(
    subscription_request: &SubscriptionRequest,
) -> Result<(&SubscriberInfo, &UUri), UStatus> {
    Ok((
        extract_subscriber_info(subscription_request)?,
        extract_subscription_topic(subscription_request)?,
    ))
}

// Extract subscriber UUri and topic UUri from SubscriptionRequest
pub fn extract_unsubscriber_topic(
    unsubscribe_request: &UnsubscribeRequest,
) -> Result<&UUri, UStatus> {
    if let Some(topic) = unsubscribe_request.topic.as_ref() {
        if let Err(error) = topic.verify_no_wildcards() {
            return Err(UStatus::fail_with_code(
                UCode::INVALID_ARGUMENT,
                format!("Subscriber URI must not contain wildcards: {}", error),
            ));
        }

        return Ok(topic);
    }

    Err(UStatus::fail_with_code(
        UCode::INVALID_ARGUMENT,
        "SubscriberInfo is missing subscriber Uri",
    ))
}

pub fn extract_unsubscriber_info(
    unsubscribe_request: &UnsubscribeRequest,
) -> Result<&SubscriberInfo, UStatus> {
    if let Some(subscriber) = unsubscribe_request.subscriber.as_ref() {
        if let Some(uri) = subscriber.uri.as_ref() {
            if let Err(error) = uri.verify_no_wildcards() {
                return Err(UStatus::fail_with_code(
                    UCode::INVALID_ARGUMENT,
                    format!("Subscriber URI must not contain wildcards: {}", error),
                ));
            }
        }

        return Ok(subscriber);
    }

    Err(UStatus::fail_with_code(
        UCode::INVALID_ARGUMENT,
        "SubscriberInfo is missing subscriber Uri",
    ))
}

// Extract SubscriberInfo and topic UUri from SubscriptionRequest
pub fn extract_unsubscriberinfo_topic(
    unsubscribe_request: &UnsubscribeRequest,
) -> Result<(&SubscriberInfo, &UUri), UStatus> {
    Ok((
        extract_unsubscriber_info(unsubscribe_request)?,
        extract_unsubscriber_topic(unsubscribe_request)?,
    ))
}

// Check if SubscriptionResponse is of type `code` and `state`
pub fn is_response_code_state(sr: &SubscriptionResponse, state: State) -> bool {
    if let Some(status) = sr.status.as_ref() {
        return status.state.enum_value_or_default().eq(&state);
    }

    false
}
