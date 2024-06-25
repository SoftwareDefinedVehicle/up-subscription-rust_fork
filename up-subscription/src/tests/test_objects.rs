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

use up_rust::core::usubscription::{SubscriberInfo, SubscriptionRequest, UnsubscribeRequest};
use up_rust::UUri;

const REMOTE_AUTHORITY: &str = "REMOTE";
const USUBSCRIPTION_SERVICE_ID: u32 = 0x0000_1111;
const USUBSCRIPTION_VERSION: u32 = 0x0000_0001;

const SUBSCRIBER1_ID: u32 = 0x0000_1000;
const SUBSCRIBER1_VERSION: u32 = 0x0000_0001;
const SUBSCRIBER1_RESOURCE: u32 = 0x000_1000;

const SUBSCRIBER2_ID: u32 = 0x0000_2000;
const SUBSCRIBER2_VERSION: u32 = 0x0000_0001;
const SUBSCRIBER2_RESOURCE: u32 = 0x0000_1000;

const SUBSCRIBER3_ID: u32 = 0x0000_3000;
const SUBSCRIBER3_VERSION: u32 = 0x0000_0001;
const SUBSCRIBER3_RESOURCE: u32 = 0x0000_1000;

const NOTIFICATION_TOPIC_ID: u32 = 0x001_0000;
const NOTIFICATION_TOPIC_VERSION: u32 = 0x0000_0001;
const NOTIFICATION_TOPIC_RESOURCE: u32 = 0x00BC_4FF2;

const TOPIC_LOCAL1_ID: u32 = 0x0010_0000;
const TOPIC_LOCAL1_VERSION: u32 = 0x0000_0001;
const TOPIC_LOCAL1_RESOURCE: u32 = 0x00A9_8AC7;

const TOPIC_LOCAL2_ID: u32 = 0x0020_0000;
const TOPIC_LOCAL2_VERSION: u32 = 0x0000_0001;
const TOPIC_LOCAL2_RESOURCE: u32 = 0x0153_158E;

const TOPIC_LOCAL3_ID: u32 = 0x0030_0000;
const TOPIC_LOCAL3_VERSION: u32 = 0x0000_0001;
const TOPIC_LOCAL3_RESOURCE: u32 = 0x01FC_A055;

const TOPIC_REMOTE1_ID: u32 = 0x0000_5000;
const TOPIC_REMOTE1_VERSION: u32 = 0x0000_0001;
const TOPIC_REMOTE1_RESOURCE: u32 = 0x2000_0000;

const TOPIC_REMOTE2_ID: u32 = 0x0000_6000;
const TOPIC_REMOTE2_VERSION: u32 = 0x0000_0001;
const TOPIC_REMOTE2_RESOURCE: u32 = 0x2000_0000;

pub(super) fn subscriber_info1() -> SubscriberInfo {
    SubscriberInfo {
        uri: Some(UUri {
            authority_name: String::default(),
            ue_id: SUBSCRIBER1_ID,
            ue_version_major: SUBSCRIBER1_VERSION,
            resource_id: SUBSCRIBER1_RESOURCE,
            ..Default::default()
        })
        .into(),
        ..Default::default()
    }
}

pub(super) fn subscriber_info2() -> SubscriberInfo {
    SubscriberInfo {
        uri: Some(UUri {
            authority_name: String::default(),
            ue_id: SUBSCRIBER2_ID,
            ue_version_major: SUBSCRIBER2_VERSION,
            resource_id: SUBSCRIBER2_RESOURCE,
            ..Default::default()
        })
        .into(),
        ..Default::default()
    }
}

pub(super) fn subscriber_info3() -> SubscriberInfo {
    SubscriberInfo {
        uri: Some(UUri {
            authority_name: String::default(),
            ue_id: SUBSCRIBER3_ID,
            ue_version_major: SUBSCRIBER3_VERSION,
            resource_id: SUBSCRIBER3_RESOURCE,
            ..Default::default()
        })
        .into(),
        ..Default::default()
    }
}

pub(super) fn bad_subscriber_info() -> SubscriberInfo {
    SubscriberInfo {
        uri: Some(UUri::default()).into(),
        ..Default::default()
    }
}

pub fn local_usubscription_service_uri() -> UUri {
    UUri {
        authority_name: String::default(),
        ue_id: USUBSCRIPTION_SERVICE_ID,
        ue_version_major: USUBSCRIPTION_VERSION,
        ..Default::default()
    }
}

pub fn remote_usubscription_service_uri() -> UUri {
    UUri {
        authority_name: REMOTE_AUTHORITY.to_string(),
        ue_id: USUBSCRIPTION_SERVICE_ID,
        ue_version_major: USUBSCRIPTION_VERSION,
        ..Default::default()
    }
}

pub fn notification_topic_uri() -> UUri {
    UUri {
        authority_name: String::default(),
        ue_id: NOTIFICATION_TOPIC_ID,
        ue_version_major: NOTIFICATION_TOPIC_VERSION,
        resource_id: NOTIFICATION_TOPIC_RESOURCE,
        ..Default::default()
    }
}

pub fn local_topic1_uri() -> UUri {
    UUri {
        authority_name: String::default(),
        ue_id: TOPIC_LOCAL1_ID,
        ue_version_major: TOPIC_LOCAL1_VERSION,
        resource_id: TOPIC_LOCAL1_RESOURCE,
        ..Default::default()
    }
}

pub fn local_topic2_uri() -> UUri {
    UUri {
        authority_name: String::default(),
        ue_id: TOPIC_LOCAL2_ID,
        ue_version_major: TOPIC_LOCAL2_VERSION,
        resource_id: TOPIC_LOCAL2_RESOURCE,
        ..Default::default()
    }
}

pub fn local_topic3_uri() -> UUri {
    UUri {
        authority_name: String::default(),
        ue_id: TOPIC_LOCAL3_ID,
        ue_version_major: TOPIC_LOCAL3_VERSION,
        resource_id: TOPIC_LOCAL3_RESOURCE,
        ..Default::default()
    }
}
pub(super) fn bad_local_topic_uri() -> UUri {
    UUri::default()
}

pub fn remote_topic1_uri() -> UUri {
    UUri {
        authority_name: REMOTE_AUTHORITY.into(),
        ue_id: TOPIC_REMOTE1_ID,
        ue_version_major: TOPIC_REMOTE1_VERSION,
        resource_id: TOPIC_REMOTE1_RESOURCE,
        ..Default::default()
    }
}

pub fn remote_topic2_uri() -> UUri {
    UUri {
        authority_name: REMOTE_AUTHORITY.into(),
        ue_id: TOPIC_REMOTE2_ID,
        ue_version_major: TOPIC_REMOTE2_VERSION,
        resource_id: TOPIC_REMOTE2_RESOURCE,
        ..Default::default()
    }
}

#[allow(dead_code)]
pub(super) fn subscription_request(topic: UUri, subscriber: SubscriberInfo) -> SubscriptionRequest {
    SubscriptionRequest {
        topic: Some(topic).into(),
        subscriber: Some(subscriber).into(),
        ..Default::default()
    }
}

#[allow(dead_code)]
pub(super) fn unsubscribe_request(topic: UUri, subscriber: SubscriberInfo) -> UnsubscribeRequest {
    UnsubscribeRequest {
        topic: Some(topic).into(),
        subscriber: Some(subscriber).into(),
        ..Default::default()
    }
}
