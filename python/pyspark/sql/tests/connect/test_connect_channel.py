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
import uuid

from pyspark.errors import PySparkValueError
from pyspark.testing.connectutils import (
    should_test_connect,
    connect_requirement_message,
)

if should_test_connect:
    import grpc
    from pyspark.sql.connect.client import DefaultChannelBuilder, ChannelBuilder
    from pyspark.sql.connect.client.core import SparkConnectClient
    from pyspark.errors.exceptions.connect import SparkConnectException


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ChannelBuilderTests(unittest.TestCase):
    def test_invalid_connection_strings(self):
        invalid = [
            "scc://host:12",
            "http://host",
            "sc:/host:1234/path",
            "sc://host/path",
            "sc://host/;parm1;param2",
        ]
        for i in invalid:
            self.assertRaises(PySparkValueError, DefaultChannelBuilder, i)

    def test_sensible_defaults(self):
        chan = DefaultChannelBuilder("sc://host")
        self.assertFalse(chan.secure, "Default URL is not secure")

        chan = DefaultChannelBuilder("sc://host/;token=abcs")
        self.assertTrue(chan.secure, "specifying a token must set the channel to secure")
        self.assertRegex(
            chan.userAgent, r"^_SPARK_CONNECT_PYTHON spark/[^ ]+ os/[^ ]+ python/[^ ]+$"
        )
        chan = DefaultChannelBuilder("sc://host/;use_ssl=abcs")
        self.assertFalse(chan.secure, "Garbage in, false out")

    def test_user_agent(self):
        chan = DefaultChannelBuilder("sc://host/;user_agent=Agent123%20%2F3.4")
        self.assertIn("Agent123 /3.4", chan.userAgent)

    def test_user_agent_len(self):
        user_agent = "x" * 2049
        chan = DefaultChannelBuilder(f"sc://host/;user_agent={user_agent}")
        with self.assertRaises(SparkConnectException) as err:
            chan.userAgent
        self.assertRegex(err.exception._message, "'user_agent' parameter should not exceed")

        user_agent = "%C3%A4" * 341  # "%C3%A4" -> "ä"; (341 * 6 = 2046) < 2048
        expected = "ä" * 341
        chan = DefaultChannelBuilder(f"sc://host/;user_agent={user_agent}")
        self.assertIn(expected, chan.userAgent)

    def test_valid_channel_creation(self):
        chan = DefaultChannelBuilder("sc://host").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        # Sets up a channel without tokens because ssl is not used.
        chan = DefaultChannelBuilder("sc://host/;use_ssl=true;token=abc").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        chan = DefaultChannelBuilder("sc://host/;use_ssl=true").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

    def test_channel_properties(self):
        chan = DefaultChannelBuilder(
            "sc://host/;use_ssl=true;token=abc;user_agent=foo;param1=120%2021"
        )
        self.assertEqual("host:15002", chan.endpoint)
        self.assertIn("foo", chan.userAgent.split(" "))
        self.assertEqual(True, chan.secure)
        self.assertEqual("120 21", chan.get("param1"))

    def test_metadata(self):
        chan = DefaultChannelBuilder(
            "sc://host/;use_ssl=true;token=abc;param1=120%2021;x-my-header=abcd"
        )
        md = chan.metadata()
        self.assertEqual([("param1", "120 21"), ("x-my-header", "abcd")], md)

    def test_metadata_with_session_id(self):
        id = str(uuid.uuid4())
        chan = DefaultChannelBuilder(f"sc://host/;session_id={id}")
        self.assertEqual(id, chan.session_id)

        chan = DefaultChannelBuilder(
            f"sc://host/;session_id={id};user_agent=acbd;token=abcd;use_ssl=true"
        )
        md = chan.metadata()
        for kv in md:
            self.assertNotIn(
                kv[0],
                [
                    ChannelBuilder.PARAM_SESSION_ID,
                    ChannelBuilder.PARAM_TOKEN,
                    ChannelBuilder.PARAM_USER_ID,
                    ChannelBuilder.PARAM_USER_AGENT,
                    ChannelBuilder.PARAM_USE_SSL,
                ],
                "Metadata must not contain fixed params",
            )

        with self.assertRaises(ValueError) as ve:
            chan = DefaultChannelBuilder("sc://host/;session_id=abcd")
            SparkConnectClient(chan)
        self.assertIn("Parameter value session_id must be a valid UUID format", str(ve.exception))

        chan = DefaultChannelBuilder("sc://host/")
        self.assertIsNone(chan.session_id)

    def test_keepalive_defaults(self):
        # SPARK-58094
        chan = DefaultChannelBuilder("sc://host")
        self.assertTrue(chan.keepalive_enabled)
        self.assertEqual(60 * 1000, chan.keepalive_time_ms)
        self.assertEqual(20 * 1000, chan.keepalive_timeout_ms)
        self.assertTrue(chan.keepalive_without_calls)

        options = dict(chan._effective_channel_options())
        self.assertEqual(60 * 1000, options["grpc.keepalive_time_ms"])
        self.assertEqual(20 * 1000, options["grpc.keepalive_timeout_ms"])
        self.assertEqual(1, options["grpc.keepalive_permit_without_calls"])

    def test_keepalive_connection_string_overrides(self):
        # SPARK-58094
        chan = DefaultChannelBuilder(
            "sc://host/;grpc_keepalive_enabled=false;grpc_keepalive_time_ms=12345;"
            "grpc_keepalive_timeout_ms=6789;grpc_keepalive_without_calls=false"
        )
        self.assertFalse(chan.keepalive_enabled)
        self.assertEqual(12345, chan.keepalive_time_ms)
        self.assertEqual(6789, chan.keepalive_timeout_ms)
        self.assertFalse(chan.keepalive_without_calls)

        # Disabled entirely: none of the keepalive options are applied to the channel.
        options = dict(chan._effective_channel_options())
        self.assertNotIn("grpc.keepalive_time_ms", options)
        self.assertNotIn("grpc.keepalive_timeout_ms", options)
        self.assertNotIn("grpc.keepalive_permit_without_calls", options)

    def test_keepalive_enabled_with_custom_values(self):
        # SPARK-58094
        chan = DefaultChannelBuilder(
            "sc://host/;grpc_keepalive_enabled=true;grpc_keepalive_time_ms=1000;"
            "grpc_keepalive_timeout_ms=500;grpc_keepalive_without_calls=false"
        )
        self.assertTrue(chan.keepalive_enabled)
        options = dict(chan._effective_channel_options())
        self.assertEqual(1000, options["grpc.keepalive_time_ms"])
        self.assertEqual(500, options["grpc.keepalive_timeout_ms"])
        self.assertEqual(0, options["grpc.keepalive_permit_without_calls"])

    def test_keepalive_explicit_channel_option_wins(self):
        # SPARK-58094: an explicitly-provided channel option for one of the keepalive keys is
        # not overwritten/duplicated by the default-derived value.
        chan = DefaultChannelBuilder("sc://host", [("grpc.keepalive_time_ms", 999)])
        options = chan._effective_channel_options()
        self.assertEqual(
            [k for k, _ in options].count("grpc.keepalive_time_ms"),
            1,
            "only one occurrence, no duplicate",
        )
        self.assertEqual(
            next(v for k, v in options if k == "grpc.keepalive_time_ms"),
            999,
            "explicit value is not overwritten by the default",
        )
        # The other keepalive keys are still applied normally.
        self.assertIn("grpc.keepalive_timeout_ms", dict(options))

    def test_keepalive_params_excluded_from_metadata(self):
        # SPARK-58094
        chan = DefaultChannelBuilder(
            "sc://host/;grpc_keepalive_enabled=false;grpc_keepalive_time_ms=1000;"
            "grpc_keepalive_timeout_ms=500;grpc_keepalive_without_calls=false;param1=abc"
        )
        md = dict(chan.metadata())
        self.assertNotIn(ChannelBuilder.PARAM_GRPC_KEEPALIVE_ENABLED, md)
        self.assertNotIn(ChannelBuilder.PARAM_GRPC_KEEPALIVE_TIME_MS, md)
        self.assertNotIn(ChannelBuilder.PARAM_GRPC_KEEPALIVE_TIMEOUT_MS, md)
        self.assertNotIn(ChannelBuilder.PARAM_GRPC_KEEPALIVE_WITHOUT_CALLS, md)
        self.assertIn("param1", md)

    def test_channel_options(self):
        # SPARK-47694
        chan = DefaultChannelBuilder(
            "sc://host", [("grpc.max_send_message_length", 1860), ("test", "robert")]
        )
        options = chan._channel_options
        self.assertEqual(
            [k for k, _ in options].count("grpc.max_send_message_length"),
            1,
            "only one occurrence for defaults",
        )
        self.assertEqual(
            next(v for k, v in options if k == "grpc.max_send_message_length"),
            1860,
            "overwrites defaults",
        )
        self.assertEqual(
            next(v for k, v in options if k == "test"), "robert", "new values are picked up"
        )


if __name__ == "__main__":
    from pyspark.testing import main

    main()
