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

use up_rust::UUri;

use crate::usubscription::USubscriptionService;

// Constants for building UUris etc
pub(crate) const UP_SUBSCRIBE_ID: u32 = 1;
pub(crate) const UP_UNSUBSCRIBE_ID: u32 = 2;
pub(crate) const UP_FETCH_SUBSCRIPTIONS_ID: u32 = 3;
pub(crate) const UP_REGISTER_FOR_NOTIFICATONS_ID: u32 = 6;
pub(crate) const UP_UNREGISTER_FOR_NOTIFICATONS_ID: u32 = 7;
pub(crate) const UP_FETCH_SUBSCRIBERS_ID: u32 = 8;

pub(crate) const UP_NOTIFICATION_ID: u32 = 0x8000;
pub(crate) const USUBSCRIPTION_SERVICE_ID: u32 = 0;
pub(crate) const USUBSCRIPTION_SERVICE_VERSION_MAJOR: u32 = 3;

impl USubscriptionService {
    // This returns false if topic UUri and local(own) UUri do have a configured UAuthority,
    // and if the names of these authorities differ.
    pub(crate) fn is_local_topic(&self, topic: &UUri) -> bool {
        self.uri.authority_name == topic.authority_name
    }

    // This is supposed to make a remote-subscription service URI from a remote URI
    // Will return `None` if uri is not remote
    pub(crate) fn make_remote_subscribe_uuri(&self, uri: &UUri) -> Option<UUri> {
        if self.is_local_topic(uri) {
            return None;
        }

        Some(UUri {
            authority_name: uri.authority_name.clone(),
            ue_id: USUBSCRIPTION_SERVICE_ID,
            ue_version_major: USUBSCRIPTION_SERVICE_VERSION_MAJOR,
            resource_id: UP_SUBSCRIBE_ID,
            ..Default::default()
        })
    }

    // This is supposed to make a remote-unsubscription service URI from a remote URI
    // Will return `None` if uri is not remote
    pub(crate) fn make_remote_unsubscribe_uuri(&self, uri: &UUri) -> Option<UUri> {
        if self.is_local_topic(uri) {
            return None;
        }

        Some(UUri {
            authority_name: uri.authority_name.clone(),
            ue_id: USUBSCRIPTION_SERVICE_ID,
            ue_version_major: USUBSCRIPTION_SERVICE_VERSION_MAJOR,
            resource_id: UP_UNSUBSCRIBE_ID,
            ..Default::default()
        })
    }

    // Return UUri for a RPC Subscribe request to this service
    pub fn notification_uri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_NOTIFICATION_ID;
        uri
    }

    // Return UUri for a RPC Subscribe request to this service
    pub fn subscribe_uuri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_SUBSCRIBE_ID;
        uri
    }

    // Return UUri for a RPC Unsubscribe request to this service
    pub fn unsubscribe_uuri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_UNSUBSCRIBE_ID;
        uri
    }

    // Return UUri for a RPC FetchSubscriptions request to this service
    pub fn fetch_subscriptions_uuri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_FETCH_SUBSCRIPTIONS_ID;
        uri
    }

    // Return UUri for a RPC FetchSubscribers request to this service
    pub fn fetch_subscribers_uuri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_FETCH_SUBSCRIBERS_ID;
        uri
    }

    // Return UUri for a RPC RegisterForNotifications request to this service
    pub fn register_for_notifications_uuri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_REGISTER_FOR_NOTIFICATONS_ID;
        uri
    }

    // Return UUri for a RPC UnregisterForNotifications request to this service
    pub fn unregister_for_notifications_uuri(&self) -> UUri {
        let mut uri = self.uri.clone();
        uri.resource_id = UP_UNREGISTER_FOR_NOTIFICATONS_ID;
        uri
    }
}
