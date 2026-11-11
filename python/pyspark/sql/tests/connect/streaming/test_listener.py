#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import unittest
from unittest import mock

from pyspark.errors import PySparkValueError
from pyspark.testing.connectutils import connect_requirement_message, should_test_connect

if should_test_connect:
    from google.protobuf import any_pb2

    import pyspark.sql.connect.proto as pb2
    from pyspark.sql.connect.client.core import PlanObservedMetrics
    from pyspark.sql.connect.streaming.query import StreamingQueryListenerBus
    from pyspark.sql.metrics import PlanMetrics


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class StreamingQueryListenerBusResponseTests(unittest.TestCase):
    def test_ignores_execution_metadata_before_and_after_registration(self):
        listener_added = pb2.StreamingQueryListenerEventsResult(listener_bus_listener_added=True)
        listener_events = pb2.StreamingQueryListenerEventsResult()
        event = listener_events.events.add(event_json="{}")
        results = iter(
            [
                PlanMetrics("metric", 1, 0, []),
                {"streaming_query_listener_events_result": listener_added},
                PlanObservedMetrics("observation", [], []),
                {"streaming_query_listener_events_result": listener_events},
            ]
        )
        manager = mock.Mock()
        manager._session.client.execute_command_as_iterator.return_value = results
        listener_bus = StreamingQueryListenerBus(manager)

        remaining_results = listener_bus._register_server_side_listener()
        with (
            mock.patch.object(
                listener_bus, "deserialize", return_value=mock.sentinel.deserialized_event
            ) as deserialize,
            mock.patch.object(listener_bus, "post_to_all") as post_to_all,
        ):
            listener_bus._query_event_handler(remaining_results)

        deserialize.assert_called_once_with(event)
        post_to_all.assert_called_once_with(mock.sentinel.deserialized_event)

    def test_rejects_unexpected_response(self):
        responses = [
            any_pb2.Any(),
            {"unexpected": object()},
            {"streaming_query_listener_events_result": object()},
        ]
        for response in responses:
            with self.subTest(response=response):
                with self.assertRaises(PySparkValueError) as error:
                    list(StreamingQueryListenerBus._iter_listener_events(iter([response])))

                self.assertEqual(error.exception.getCondition(), "UNKNOWN_RESPONSE")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
