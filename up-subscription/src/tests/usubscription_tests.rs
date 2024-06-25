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

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};
    use std::sync::Arc;
    use std::time::Duration;
    use test_case::test_case;
    use tokio::{
        sync::mpsc::{self, Receiver},
        time::timeout,
    };

    use up_rust::core::usubscription::{
        NotificationsRequest, State, SubscriberInfo, SubscriptionRequest, SubscriptionStatus,
        USubscription, UnsubscribeRequest,
    };
    use up_rust::UUri;

    use crate::common::helpers;
    use crate::tests::test_transports::{NotificationTuple, TransportMock};
    use crate::tests::{test_lib, test_objects};
    use crate::USubscriptionService;

    // This is a second-scale value used by some tests to wait-timeout on receiving data from UTransport-mocks
    const RECEIVE_TIMEOUT_SECONDS: u64 = 1;

    fn get_usubscription_mock(
        remote_state_channel: bool,
    ) -> (
        Arc<USubscriptionService>,
        Receiver<NotificationTuple>,
        Option<Receiver<NotificationTuple>>,
    ) {
        let (notification_sender, notification_receiver) = mpsc::channel::<NotificationTuple>(1);

        let (remote_state_sender, remote_state_receiver) = if remote_state_channel {
            let (sender, receiver) = mpsc::channel::<NotificationTuple>(1);
            (Some(sender), Some(receiver))
        } else {
            (None, None)
        };

        let up_client_mock = Arc::new(TransportMock::new(
            test_objects::remote_usubscription_service_uri(),
            notification_sender,
            remote_state_sender,
        ));

        (
            Arc::new(USubscriptionService::new(
                Some("LocalMockUsub"),
                test_objects::local_usubscription_service_uri(),
                up_client_mock.clone(),
                up_client_mock.clone(),
            )),
            notification_receiver,
            remote_state_receiver,
        )
    }

    // Test basic state setting roundtrip
    // - internal subscriber/topic/status combination-setting working as expected
    #[test_case(helpers::subscription_status_subscribed(); "SubscriptionStatus SUBSCRIBED/OK")]
    #[test_case(helpers::subscription_status_subscribepending(); "SubscriptionStatus PENDING/OK")]
    #[test_case(helpers::subscription_status_unsubscribed(); "SubscriptionStatus UNSUBSCRIBED/INVALID")]
    #[tokio::test]
    async fn test_update_status(status: SubscriptionStatus) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        usubscription.update_status(
            &test_objects::subscriber_info1(),
            &test_objects::local_topic1_uri(),
            &status,
        );

        assert!(usubscription.is_subscribe_status(
            &test_objects::subscriber_info1(),
            &test_objects::local_topic1_uri(),
            &status.state.enum_value_or_default(),
        ));
    }

    // Test status setting scenarios
    // - multiple subscriber/topic/status combinations
    // - changes/overwrites to existing entries
    #[test_case(vec![(test_objects::subscriber_info1(),test_objects::local_topic1_uri(), helpers::subscription_status_subscribed()),
                     (test_objects::subscriber_info1(),test_objects::local_topic2_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info1(),test_objects::local_topic3_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info2(),test_objects::local_topic1_uri(), helpers::subscription_status_subscribed()),
                     (test_objects::subscriber_info2(),test_objects::local_topic2_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info2(),test_objects::local_topic3_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info3(),test_objects::local_topic1_uri(), helpers::subscription_status_subscribed()),
                     (test_objects::subscriber_info3(),test_objects::local_topic2_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info3(),test_objects::local_topic3_uri(), helpers::subscription_status_unsubscribed())]; "Various sub/topic/state combinations")]
    #[test_case(vec![(test_objects::subscriber_info1(),test_objects::local_topic1_uri(), helpers::subscription_status_subscribed()),
                     (test_objects::subscriber_info1(),test_objects::local_topic2_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info1(),test_objects::local_topic3_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info2(),test_objects::local_topic1_uri(), helpers::subscription_status_subscribed()),
                     (test_objects::subscriber_info2(),test_objects::local_topic2_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info2(),test_objects::local_topic3_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info1(),test_objects::local_topic1_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info2(),test_objects::local_topic2_uri(), helpers::subscription_status_subscribed())]; "Various sub/topic/state combinations, with overwrites")]
    #[tokio::test]
    async fn test_update_bookkeeping(
        subscriber_tuples: Vec<(SubscriberInfo, UUri, SubscriptionStatus)>,
    ) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        // tracking the "final" state sets for verification down below (as we might be overwriting existing subscriber-topic combinations)
        #[allow(clippy::mutable_key_type)]
        let mut to_verify: HashMap<(SubscriberInfo, UUri), SubscriptionStatus> = HashMap::new();

        for (sub, topic, status) in &subscriber_tuples {
            usubscription.update_status(sub, topic, status);
            to_verify.insert((sub.clone(), topic.clone()), status.clone());
        }

        for ((sub, topic), status) in to_verify {
            assert!(usubscription.is_subscribe_status(
                &sub,
                &topic,
                &status.state.enum_value_or_default(),
            ));
        }
    }

    // Test update notification handling
    // - update notifications working
    // - no notifications for non-status-changing events
    // - unregistering for update notifications
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_subscribed())]; "Basic status notification combo")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_unsubscribed()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_subscribepending()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri(), helpers::subscription_status_subscribed())]; "Status notification combo with non-changes")]
    #[tokio::test]
    async fn test_update_status_notification(
        mut subscriber_tuples: Vec<(SubscriberInfo, UUri, SubscriptionStatus)>,
    ) {
        test_lib::before_test();
        let (usubscription, mut notification_receiver, _) = get_usubscription_mock(false);

        let notifications_register_request = NotificationsRequest {
            topic: Some(test_objects::notification_topic_uri()).into(),
            subscriber: Some(test_objects::subscriber_info1()).into(),
            ..Default::default()
        };

        usubscription
            .register_for_notifications(notifications_register_request.clone())
            .await
            .expect("Update notification registration failed");

        // we loop over all the subscriber/topic/status combinations, setting each and then checking if we got matching information from the notification Update message
        for (sub, topic, status) in &subscriber_tuples {
            if let Some(false) = usubscription.update_status(sub, topic, status) {
                // this is the case where an existing status didn't change, so there's not going to be a state-change notifications
                let update_result = timeout(
                    Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                    notification_receiver.recv(),
                )
                .await;

                assert!(
                    update_result.is_err(),
                    "We shouldn't have received anything here"
                )
            } else {
                let update_tuple_result = timeout(
                    Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                    notification_receiver.recv(),
                )
                .await;

                assert!(
                    update_tuple_result.is_ok(),
                    "Timeout waiting for notification data"
                );

                let (recv_subscriber, recv_topic, recv_status) =
                    update_tuple_result.unwrap().unwrap();
                assert_eq!(*sub, recv_subscriber);
                assert_eq!(*topic, recv_topic);
                assert_eq!(*status, recv_status);
            }
        }

        usubscription
            .unregister_for_notifications(notifications_register_request)
            .await
            .expect("Update notification unregistration failed");

        // Test that we don't get any more notifications after unregistering our listener - maybe performance can be improved (all the timeouts...)?
        subscriber_tuples.reverse(); // just to add a little variety
        for (sub, topic, status) in &subscriber_tuples {
            usubscription.update_status(sub, topic, status);

            let update_result = timeout(
                Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                notification_receiver.recv(),
            )
            .await;

            assert!(
                update_result.is_err(),
                "We shouldn't have received anything here"
            )
        }
    }

    // Test for complex notification/channel registrations
    // - different subscriber-notification channel combinations
    // - more than one identical subscriber-notification combo
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                     (test_objects::subscriber_info1(), test_objects::local_topic2_uri()),
                     (test_objects::subscriber_info2(), test_objects::local_topic2_uri()),
                     (test_objects::subscriber_info3(), test_objects::local_topic3_uri())]; "Multiple different subscribers and notification topics")]
    #[tokio::test]
    async fn test_complex_notification_setup(subscriber_tuples: Vec<(SubscriberInfo, UUri)>) {
        test_lib::before_test();
        let (usubscription, mut notification_receiver, _) = get_usubscription_mock(false);

        // helper to state-cycle through below, to avoid setting identical status to the same combo
        let status_helpers: Vec<SubscriptionStatus> = vec![
            helpers::subscription_status_subscribed(),
            helpers::subscription_status_subscribepending(),
            helpers::subscription_status_unsubscribed(),
            helpers::subscription_status_unsubscribepending(),
        ];

        // List of notification topics
        #[allow(clippy::mutable_key_type)]
        let mut notification_matrix: HashSet<&UUri> = HashSet::new();

        // Register subscriber-channel combos for notification, fill in notification matrix
        for (subscriber, topic) in &subscriber_tuples {
            let notifications_register_request = NotificationsRequest {
                topic: Some(topic.clone()).into(),
                subscriber: Some(subscriber.clone()).into(),
                ..Default::default()
            };
            usubscription
                .register_for_notifications(notifications_register_request.clone())
                .await
                .expect("Update notification registration failed");

            notification_matrix.insert(topic);
        }

        for status in status_helpers {
            if let Some(false) = usubscription.update_status(
                &test_objects::subscriber_info1(),
                &test_objects::local_topic3_uri(),
                &status,
            ) {
                // No state change happened, so not update notifications coming
            } else {
                // Each registered notification topic/channel should receive an update notification
                for _ in &notification_matrix {
                    let update_tuple_result = timeout(
                        Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                        notification_receiver.recv(),
                    )
                    .await;

                    assert!(
                        update_tuple_result.is_ok(),
                        "Timeout waiting for notification data"
                    );

                    // probably overkill - but just compare notification ouput with what we put in above
                    let (recv_subscriber, recv_topic, recv_status) =
                        update_tuple_result.unwrap().unwrap();
                    assert_eq!(test_objects::subscriber_info1(), recv_subscriber);
                    assert_eq!(test_objects::local_topic3_uri(), recv_topic);
                    assert_eq!(status, recv_status);
                }
            }
        }
    }

    // Test for usubscription.subscribe() to bad subscribe topics
    // - expected response of usubscription.subscribe()
    // - expected (no) status change of usubscription bookeeping after operation completed
    #[test_case(test_objects::subscription_request(test_objects::bad_local_topic_uri(), test_objects::subscriber_info1()); "Empty topic UUri")]
    #[test_case(test_objects::subscription_request(test_objects::local_topic1_uri(), test_objects::bad_subscriber_info()); "Empty SubscriberInfo")]
    #[tokio::test]
    async fn test_subscribe_fail(subscription_request: SubscriptionRequest) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        assert_eq!(
            usubscription.count_in_state(
                &subscription_request.topic.clone().unwrap_or_default(),
                &State::SUBSCRIBED,
            ),
            0
        );

        let result = usubscription.subscribe(subscription_request.clone()).await;
        assert!(result.is_err());

        assert_eq!(
            usubscription.count_in_state(
                &subscription_request.topic.clone().unwrap_or_default(),
                &State::SUBSCRIBED,
            ),
            0
        );

        // Assert that we haven't done anything with this invalid subscription request (it's not showing up in bookkeeping)
        assert!(usubscription
            .represents_current_state(
                &subscription_request.subscriber.clone().unwrap_or_default(),
                &subscription_request.topic.clone().unwrap_or_default(),
                vec![
                    State::SUBSCRIBED,
                    State::UNSUBSCRIBED,
                    State::SUBSCRIBE_PENDING,
                    State::UNSUBSCRIBE_PENDING
                ]
            )
            .is_none())
    }

    // Test for
    // - expected response of usubscription.unsubscribe()
    // - expected status change of usubscription bookeeping after operation completed
    #[test_case(test_objects::unsubscribe_request(test_objects::local_topic1_uri(), test_objects::subscriber_info1()); "Good UnsubscribeRequest")]
    #[tokio::test]
    async fn test_unsubscribe_success(unsubscribe_request: UnsubscribeRequest) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        let result = usubscription.unsubscribe(unsubscribe_request.clone()).await;
        assert!(result.is_ok());

        assert!(usubscription.is_subscribe_status(
            &unsubscribe_request.subscriber.clone().unwrap_or_default(),
            &unsubscribe_request.topic.unwrap_or_default(),
            &State::UNSUBSCRIBED,
        ));
    }

    // Test for usubscription.subscribe() to bad subscribe topics
    // - expected response of usubscription.subscribe()
    // - expected (no) status change of usubscription bookeeping after operation completed
    #[test_case(test_objects::unsubscribe_request(test_objects::bad_local_topic_uri(), test_objects::subscriber_info1()); "Empty topic UUri")]
    #[test_case(test_objects::unsubscribe_request(test_objects::local_topic1_uri(), test_objects::bad_subscriber_info()); "Empty SubscriberInfo")]
    #[tokio::test]
    async fn test_unsubscribe_fail(unsubscribe_request: UnsubscribeRequest) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        assert_eq!(
            usubscription.count_in_state(
                &unsubscribe_request.topic.clone().unwrap_or_default(),
                &State::SUBSCRIBED,
            ),
            0
        );

        let result = usubscription.unsubscribe(unsubscribe_request.clone()).await;
        assert!(result.is_err());
    }

    // Test usubscription.subscribe() and usubscription.unsubscribe() roundtrip
    // - expected response of usubscription.subscribe()
    // - expected status change of usubscription bookkeeping after operation completed
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri())]; "One subscriber, one topic")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                     (test_objects::subscriber_info1(), test_objects::local_topic2_uri())]; "One subscriber, two different topics")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                     (test_objects::subscriber_info1(), test_objects::local_topic1_uri())]; "One subscriber, two identical topics")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                     (test_objects::subscriber_info2(), test_objects::local_topic2_uri())]; "Two subscribers, two different topics")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::local_topic1_uri()),
                     (test_objects::subscriber_info2(), test_objects::local_topic1_uri())]; "Two subscribers, two identical topics")]
    #[tokio::test]
    async fn test_local_subscribe_unsubscribe_roundtrip(
        subscriber_tuples: Vec<(SubscriberInfo, UUri)>,
    ) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        // List of notification topics
        #[allow(clippy::mutable_key_type)]
        let mut subscriber_info: HashMap<&UUri, HashSet<&SubscriberInfo>> = HashMap::new();

        // #### SECTION 1 - perform setup and prep

        // Create the SubscriptionRequest objects we need to do our thing
        let mut subscription_requests: Vec<SubscriptionRequest> = Vec::new();
        for (subscriber, topic) in &subscriber_tuples {
            subscription_requests.push(test_objects::subscription_request(
                topic.clone(),
                subscriber.clone(),
            ));

            subscriber_info.entry(topic).or_default().insert(subscriber);
        }

        // at this point there should be no subscriptions yet
        for subscription_request in &subscription_requests {
            assert_eq!(
                usubscription.count_in_state(
                    subscription_request.topic.as_ref().unwrap_or_default(),
                    &State::SUBSCRIBED,
                ),
                0
            );
        }

        // #### SECTION 2 - subscribe everyone

        // perform subscriptions and check resulting status
        for subscription_request in &subscription_requests {
            let result = usubscription.subscribe(subscription_request.clone()).await;
            assert!(result.is_ok());

            assert!(usubscription.is_subscribe_status(
                subscription_request.subscriber.as_ref().unwrap_or_default(),
                subscription_request.topic.as_ref().unwrap_or_default(),
                &State::SUBSCRIBED,
            ));
        }

        // #### SECTION 3 - validate correct subscription counts

        // Check for correct number of subscriptions for each topic
        for (topic, subs) in subscriber_info {
            assert_eq!(
                usubscription.count_in_state(topic, &State::SUBSCRIBED,),
                subs.len()
            );
        }

        // #### SECTION 4 - unsubscribe everyone

        // Create the SubscriptionRequest objects we need to do our thing
        let mut unsubscribe_requests: Vec<UnsubscribeRequest> = Vec::new();
        for (subscriber, topic) in &subscriber_tuples {
            unsubscribe_requests.push(test_objects::unsubscribe_request(
                topic.clone(),
                subscriber.clone(),
            ));
        }
        for unsubscribe_request in &unsubscribe_requests {
            let result = usubscription.unsubscribe(unsubscribe_request.clone()).await;
            assert!(result.is_ok());

            assert!(usubscription.is_subscribe_status(
                unsubscribe_request.subscriber.as_ref().unwrap_or_default(),
                unsubscribe_request.topic.as_ref().unwrap_or_default(),
                &State::UNSUBSCRIBED,
            ));
        }
        // at this point there should again be no subscriptions
        for subscription_request in &subscription_requests {
            assert_eq!(
                usubscription.count_in_state(
                    subscription_request.topic.as_ref().unwrap_or_default(),
                    &State::SUBSCRIBED,
                ),
                0
            );
        }
    }

    // Test remote subscribe/unsubscribe business logic
    // - expected response of usubscription.remote_subscribe()
    // - expected response of usubscription.remote_unsubscribe()
    // - expected content of remote-usubscription.invoke_method() call
    // - expected status change of usubscription bookeeping after operation completed
    #[tokio::test]
    async fn test_remote_subscribe_unsubscribe() {
        test_lib::before_test();
        let (local_usubscription, _, _) = get_usubscription_mock(false);

        // First, subscribe...
        let subscription_request = test_objects::subscription_request(
            test_objects::remote_topic1_uri(),
            test_objects::subscriber_info1(),
        );

        let result: Result<(), up_rust::UStatus> = local_usubscription
            .remote_subscribe(&subscription_request)
            .await;
        assert!(result.is_ok());

        assert!(local_usubscription.is_subscribe_status(
            &subscription_request.subscriber.clone().unwrap_or_default(),
            &subscription_request.topic.unwrap_or_default(),
            &State::SUBSCRIBED,
        ));

        assert_eq!(
            local_usubscription.remote_topic_subscribers(&test_objects::remote_topic1_uri()),
            1
        );

        // Then, unsubscribe again...
        let unsubscribe_request = test_objects::unsubscribe_request(
            test_objects::remote_topic1_uri(),
            test_objects::subscriber_info1(),
        );

        let result = local_usubscription
            .remote_unsubscribe(&unsubscribe_request)
            .await;
        assert!(result.is_ok());

        assert!(local_usubscription.is_subscribe_status(
            &unsubscribe_request.subscriber.clone().unwrap_or_default(),
            &unsubscribe_request.topic.unwrap_or_default(),
            &State::UNSUBSCRIBED,
        ));

        assert_eq!(
            local_usubscription.remote_topic_subscribers(&test_objects::remote_topic1_uri()),
            0
        );
    }

    // Test remote subscribe/unsubscribe business logic (only use remote topic URIs in test input)
    // (this is for inner remote_susbscribe()/remote_unsubscribe(), without going through the regular subscribe()/unsubscribe() outers)
    // - expected response of usubscription.remote_unsubscribe()
    // - expected content of remote-usubscription.invoke_method() call
    // - expected status change of usubscription bookeeping after operation completed
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri())]; "One subscriber, one remote topic")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri()),
                     (test_objects::subscriber_info1(), test_objects::remote_topic2_uri())]; "One subscriber, two remote topics")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri()),
                     (test_objects::subscriber_info2(), test_objects::remote_topic1_uri())]; "Two subscribers, one remote topic")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri()),
                     (test_objects::subscriber_info2(), test_objects::remote_topic2_uri())]; "Two subscribers, two remote topics")]
    #[tokio::test]
    async fn test_remote_subscribe_unsubscribe_scenarios(
        subscriber_tuples: Vec<(SubscriberInfo, UUri)>,
    ) {
        test_lib::before_test();
        let (usubscription, _, _) = get_usubscription_mock(false);

        // List of notification topics
        #[allow(clippy::mutable_key_type)]
        let mut subscriber_info: HashMap<&UUri, HashSet<&SubscriberInfo>> = HashMap::new();

        // #### SECTION 1 - perform setup and prep

        // Create the SubscriptionRequest objects we need to do our thing
        let mut subscription_requests: Vec<SubscriptionRequest> = Vec::new();
        for (subscriber, topic) in &subscriber_tuples {
            subscription_requests.push(test_objects::subscription_request(
                topic.clone(),
                subscriber.clone(),
            ));

            subscriber_info.entry(topic).or_default().insert(subscriber);
        }

        // at this point there should be no subscriptions yet
        for subscription_request in &subscription_requests {
            assert_eq!(
                usubscription.count_in_state(
                    subscription_request.topic.as_ref().unwrap_or_default(),
                    &State::SUBSCRIBED,
                ),
                0
            );
        }

        // #### SECTION 2 - subscribe everyone

        // perform subscriptions and check resulting status
        for subscription_request in &subscription_requests {
            let result = usubscription.remote_subscribe(subscription_request).await;
            assert!(result.is_ok());

            let state = usubscription.subscribe_status(
                subscription_request.subscriber.as_ref().unwrap_or_default(),
                subscription_request.topic.as_ref().unwrap_or_default(),
            );
            println!("state: {:?}", state);

            assert!(usubscription.is_subscribe_status(
                subscription_request.subscriber.as_ref().unwrap_or_default(),
                subscription_request.topic.as_ref().unwrap_or_default(),
                &State::SUBSCRIBED,
            ));
        }

        // #### SECTION 3 - validate correct subscription counts

        // Check for correct number of subscriptions for each topic
        for (topic, subs) in &subscriber_info {
            assert_eq!(
                usubscription.count_in_state(topic, &State::SUBSCRIBED,),
                subs.len()
            );

            assert_eq!(
                usubscription.remote_topic_subscribers(topic),
                subs.len() as u32
            );
        }

        // #### SECTION 4 - unsubscribe everyone

        // Create the SubscriptionRequest objects we need to do our thing
        let mut unsubscribe_requests: Vec<UnsubscribeRequest> = Vec::new();
        for (subscriber, topic) in &subscriber_tuples {
            unsubscribe_requests.push(test_objects::unsubscribe_request(
                topic.clone(),
                subscriber.clone(),
            ));
        }
        for unsubscribe_request in &unsubscribe_requests {
            let result = usubscription.remote_unsubscribe(unsubscribe_request).await;
            assert!(result.is_ok());

            assert!(usubscription.is_subscribe_status(
                unsubscribe_request.subscriber.as_ref().unwrap_or_default(),
                unsubscribe_request.topic.as_ref().unwrap_or_default(),
                &State::UNSUBSCRIBED,
            ));
        }

        // at this point there should again be no subscriptions
        for subscription_request in &subscription_requests {
            assert_eq!(
                usubscription.count_in_state(
                    subscription_request.topic.as_ref().unwrap_or_default(),
                    &State::SUBSCRIBED,
                ),
                0
            );
        }
        assert_eq!(
            usubscription.remote_topic_subscribers(&test_objects::remote_topic1_uri()),
            0
        );
    }

    // Test remote subscribe/unsubscribe business logic by watching state change notifications (only use remote topic URIs in test input)
    // (this is going through the regular subscribe()/unsubscribe() outers)
    // - expected response of usubscription.remote_unsubscribe()
    // - expected content of remote-usubscription.invoke_method() call
    // - expected status change of usubscription bookeeping after operation completed
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri())]; "Simple remote-subscribe flow")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri()),
                     (test_objects::subscriber_info1(), test_objects::remote_topic2_uri())]; "One subscriber, two remote topics")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri()),
                     (test_objects::subscriber_info2(), test_objects::remote_topic1_uri())]; "Two subscribers, one remote topic")]
    #[test_case(vec![(test_objects::subscriber_info1(), test_objects::remote_topic1_uri()),
                     (test_objects::subscriber_info2(), test_objects::remote_topic2_uri())]; "Two subscribers, two remote topics")]
    #[tokio::test]
    async fn test_remote_subscribe_status_flow(subscriber_tuples: Vec<(SubscriberInfo, UUri)>) {
        test_lib::before_test();
        let (usubscription, mut notification_receiver, _) = get_usubscription_mock(false);

        // #### SECTION 1 - perform setup and prep

        // List of notification topics
        #[allow(clippy::mutable_key_type)]
        let mut subscriber_info: HashMap<&UUri, HashSet<&SubscriberInfo>> = HashMap::new();

        // setup notification listener, for status-change tracking
        let notifications_register_request = NotificationsRequest {
            topic: Some(test_objects::notification_topic_uri()).into(),
            subscriber: Some(test_objects::subscriber_info1()).into(),
            ..Default::default()
        };
        usubscription
            .register_for_notifications(notifications_register_request)
            .await
            .expect("Update notification registration failed");

        // #### SECTION 2 - perform subscriptions and check resulting status notifications
        // Create the SubscriptionRequest objects we need to do our thing
        let mut subscription_requests: Vec<SubscriptionRequest> = Vec::new();
        for (subscriber, topic) in &subscriber_tuples {
            subscription_requests.push(test_objects::subscription_request(
                topic.clone(),
                subscriber.clone(),
            ));

            subscriber_info.entry(topic).or_default().insert(subscriber);
        }

        for subscription_request in &subscription_requests {
            // do subscription to (remote) topic
            let result = usubscription.subscribe(subscription_request.clone()).await;
            assert!(result.is_ok());

            // this should initially give us a SUBSCRIBE_PENDING state
            let update_tuple_result = timeout(
                Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                notification_receiver.recv(),
            )
            .await
            .expect("Timeout waiting for notification data");

            let (_, _, recv_status) = update_tuple_result.unwrap();
            assert_eq!(
                recv_status.state.enum_value_or_default(),
                State::SUBSCRIBE_PENDING
            );

            // this should initially give us a SUBSCRIBED state
            let update_tuple_result = timeout(
                Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                notification_receiver.recv(),
            )
            .await
            .expect("Timeout waiting for notification data");

            let (_, _, recv_status) = update_tuple_result.unwrap();
            assert_eq!(recv_status.state.enum_value_or_default(), State::SUBSCRIBED);
        }

        // #### SECTION 2 - validate correct subscription counts

        // Check for correct number of subscriptions for each topic
        for (topic, subs) in &subscriber_info {
            assert_eq!(
                usubscription.count_in_state(topic, &State::SUBSCRIBED,),
                subs.len()
            );

            assert_eq!(
                usubscription.remote_topic_subscribers(topic),
                subs.len() as u32
            );
        }

        // #### SECTION 3 - perform unsubscriptions and check resulting status notifications
        // Create the SubscriptionRequest objects we need to do our thing
        let mut unsubscribe_requests: Vec<UnsubscribeRequest> = Vec::new();
        for (subscriber, topic) in &subscriber_tuples {
            unsubscribe_requests.push(test_objects::unsubscribe_request(
                topic.clone(),
                subscriber.clone(),
            ));
        }

        for unsubscribe_request in &unsubscribe_requests {
            // do subscription to (remote) topic
            let result = usubscription.unsubscribe(unsubscribe_request.clone()).await;
            assert!(result.is_ok());

            // this should initially give us a UNSUBSCRIBE_PENDING state
            let update_tuple_result = timeout(
                Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                notification_receiver.recv(),
            )
            .await
            .expect("Timeout waiting for notification data");

            let (_, _, recv_status) = update_tuple_result.unwrap();
            assert_eq!(
                recv_status.state.enum_value_or_default(),
                State::UNSUBSCRIBE_PENDING
            );

            // this should initially give us a UNSUBSCRIBED state
            let update_tuple_result = timeout(
                Duration::from_secs(RECEIVE_TIMEOUT_SECONDS),
                notification_receiver.recv(),
            )
            .await
            .expect("Timeout waiting for notification data");

            let (_, _, recv_status) = update_tuple_result.unwrap();
            assert_eq!(
                recv_status.state.enum_value_or_default(),
                State::UNSUBSCRIBED
            );
        }
    }
}
