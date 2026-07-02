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
    from pyspark.sql.connect.client import (
        DefaultChannelBuilder,
        ChannelBuilder,
        PathAwareChannelBuilder,
    )
    from pyspark.sql.connect.client.core import (
        SparkConnectClient,
        _ClientCallDetails,
        _PathPrefixInterceptor,
    )
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


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class PathAwareChannelBuilderTests(unittest.TestCase):
    def test_invalid_scheme(self):
        for i in ["http://host", "sc:/host", "host:1234", ""]:
            self.assertRaises(PySparkValueError, PathAwareChannelBuilder, i)

    def test_invalid_params(self):
        # Params must be provided as key=value pairs.
        self.assertRaises(PySparkValueError, PathAwareChannelBuilder, "sc://host/;foobar")
        self.assertRaises(PySparkValueError, PathAwareChannelBuilder, "sc://host/;a=1=2")

    def test_standard_form_defaults(self):
        # Without a path the builder behaves like the default one: no prefix, default port.
        chan = PathAwareChannelBuilder("sc://host")
        self.assertEqual("host:15002", chan.endpoint)
        self.assertEqual("host", chan.host)
        self.assertEqual("", chan.path_prefix)
        self.assertFalse(chan.secure)
        self.assertFalse(chan.use_ssl)
        self.assertEqual([], chan._interceptors)

    def test_standard_form_explicit_port(self):
        chan = PathAwareChannelBuilder("sc://host:1234")
        self.assertEqual("host:1234", chan.endpoint)
        self.assertEqual("", chan.path_prefix)
        self.assertFalse(chan.secure)

    def test_standard_form_params(self):
        chan = PathAwareChannelBuilder("sc://host/;use_ssl=true;token=abc")
        self.assertEqual("host:15002", chan.endpoint)
        self.assertEqual("", chan.path_prefix)
        self.assertTrue(chan.use_ssl)
        self.assertTrue(chan.secure)
        self.assertEqual("abc", chan.token)
        # A bare path (only a route prefix, no params) still adds no interceptor.
        self.assertEqual([], chan._interceptors)

    def test_path_routed_with_trailing_port(self):
        # The trailing ":<port>" on the final path segment is the connection port,
        # the remaining path is the ingress route prefix, and port 443 implies TLS.
        chan = PathAwareChannelBuilder("sc://gateway/sparkConnect/app-1/driver:443")
        self.assertEqual("gateway:443", chan.endpoint)
        self.assertEqual("gateway", chan.host)
        self.assertEqual("/sparkConnect/app-1/driver", chan.path_prefix)
        self.assertTrue(chan.use_ssl)
        self.assertTrue(chan.secure)

    def test_path_routed_port_in_netloc(self):
        # The port may instead live in the netloc; the whole path is then the prefix.
        chan = PathAwareChannelBuilder("sc://gateway:443/sparkConnect/app-1")
        self.assertEqual("gateway:443", chan.endpoint)
        self.assertEqual("/sparkConnect/app-1", chan.path_prefix)
        self.assertTrue(chan.use_ssl)
        self.assertTrue(chan.secure)

    def test_netloc_port_wins_over_path_port(self):
        # When the netloc carries a port, a trailing ":<port>" in the path does not
        # override it, but is still stripped from the route prefix.
        chan = PathAwareChannelBuilder("sc://host:1234/foo:443")
        self.assertEqual("host:1234", chan.endpoint)
        self.assertEqual("/foo", chan.path_prefix)
        self.assertFalse(chan.use_ssl)
        self.assertFalse(chan.secure)

    def test_non_443_port_not_secure_by_default(self):
        chan = PathAwareChannelBuilder("sc://gateway/sparkConnect/driver:8080")
        self.assertEqual("gateway:8080", chan.endpoint)
        self.assertEqual("/sparkConnect/driver", chan.path_prefix)
        self.assertFalse(chan.use_ssl)
        self.assertFalse(chan.secure)

    def test_explicit_use_ssl_overrides_443_default(self):
        # An explicit ;use_ssl=false must win over the implicit TLS for port 443.
        chan = PathAwareChannelBuilder("sc://gateway:443/sparkConnect/app-1/;use_ssl=false")
        self.assertEqual("gateway:443", chan.endpoint)
        self.assertEqual("/sparkConnect/app-1", chan.path_prefix)
        self.assertFalse(chan.use_ssl)
        self.assertFalse(chan.secure)

    def test_token_implies_secure(self):
        chan = PathAwareChannelBuilder("sc://gateway:8080/sparkConnect/driver/;token=abc")
        self.assertEqual("gateway:8080", chan.endpoint)
        self.assertEqual("/sparkConnect/driver", chan.path_prefix)
        self.assertEqual("abc", chan.token)
        self.assertFalse(chan.use_ssl)
        self.assertTrue(chan.secure, "a token must set the channel to secure")

    def test_ipv6_endpoint(self):
        # IPv6 literals must be handled and bracket-wrapped, with or without a port.
        chan = PathAwareChannelBuilder("sc://[::1]:15002/path1")
        self.assertEqual("[::1]:15002", chan.endpoint)
        self.assertEqual("[::1]", chan.host)
        self.assertEqual("/path1", chan.path_prefix)

        chan = PathAwareChannelBuilder("sc://[::1]/path1")
        self.assertEqual("[::1]:15002", chan.endpoint)
        self.assertEqual("[::1]", chan.host)
        self.assertEqual("/path1", chan.path_prefix)

    def test_missing_hostname_raises(self):
        # A netloc with only a port (empty host) must raise the wrapped error.
        self.assertRaises(PySparkValueError, PathAwareChannelBuilder, "sc://:15002/p")

    def test_path_port_with_trailing_slash_params(self):
        # The trailing ":<port>" must still be recognized when combined with the
        # standard "/;params" form, which leaves a trailing slash on the path.
        chan = PathAwareChannelBuilder("sc://gateway/app/driver:443/;token=abc")
        self.assertEqual("gateway:443", chan.endpoint)
        self.assertEqual("/app/driver", chan.path_prefix)
        self.assertEqual("abc", chan.token)
        self.assertTrue(chan.use_ssl)
        self.assertTrue(chan.secure)

    def test_subclasses_default_builder(self):
        # It inherits DefaultChannelBuilder so parsing/credential handling (and the
        # SPARK_TESTING-aware default_port) stay in sync between the two builders.
        self.assertTrue(issubclass(PathAwareChannelBuilder, DefaultChannelBuilder))
        self.assertEqual(
            DefaultChannelBuilder.default_port(), PathAwareChannelBuilder.default_port()
        )

    def test_no_path_no_implicit_tls(self):
        # Without a path prefix the 443-implies-TLS rule does not apply; this matches
        # DefaultChannelBuilder and the (now narrowed) docstring.
        chan = PathAwareChannelBuilder("sc://gateway:443")
        self.assertEqual("gateway:443", chan.endpoint)
        self.assertEqual("", chan.path_prefix)
        self.assertFalse(chan.use_ssl)
        self.assertFalse(chan.secure)
        self.assertEqual([], chan._interceptors)

    def test_bare_path_port_without_prefix(self):
        # "sc://host/:443" has a trailing port but no route prefix. The port must not
        # be adopted on its own (which would speak plaintext against an HTTPS port);
        # it collapses to a plain "sc://host" with the default port and no TLS.
        chan = PathAwareChannelBuilder("sc://host/:443")
        self.assertEqual(f"host:{PathAwareChannelBuilder.default_port()}", chan.endpoint)
        self.assertEqual("", chan.path_prefix)
        self.assertFalse(chan.use_ssl)
        self.assertFalse(chan.secure)
        self.assertEqual([], chan._interceptors)

    def test_param_in_non_final_segment_rejected(self):
        # urlparse only strips ";params" from the final segment; a ";key=value" on an
        # earlier segment would otherwise be swallowed into the route prefix (leaking
        # the credential into every RPC path). It must be rejected instead.
        self.assertRaises(
            PySparkValueError, PathAwareChannelBuilder, "sc://gateway/app;token=abc/driver"
        )

    def test_invalid_path_port_rejected(self):
        # The path-derived port must pass the same 0-65535 integer validation as the
        # netloc port; out-of-range, negative and PEP 515 underscore forms are invalid.
        for url in [
            "sc://gateway/driver:99999",
            "sc://gateway/driver:-1",
            "sc://gateway/driver:4_43",
        ]:
            with self.subTest(url=url):
                self.assertRaises(PySparkValueError, PathAwareChannelBuilder, url)

    def test_interceptor_added_for_path_prefix(self):
        chan = PathAwareChannelBuilder("sc://gateway/sparkConnect/app-1/driver:443")
        self.assertEqual(1, len(chan._interceptors))
        self.assertIsInstance(chan._interceptors[0], _PathPrefixInterceptor)

    def test_valid_channel_creation(self):
        # Secure (implicit TLS via port 443) path-routed channel.
        chan = PathAwareChannelBuilder("sc://gateway/sparkConnect/app-1/driver:443").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        # Insecure channel.
        chan = PathAwareChannelBuilder("sc://host").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

        # localhost + token uses local channel credentials.
        chan = PathAwareChannelBuilder("sc://localhost:8080/route/;token=abc").toChannel()
        self.assertIsInstance(chan, grpc.Channel)

    def test_channel_options(self):
        chan = PathAwareChannelBuilder(
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


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class PathPrefixInterceptorTests(unittest.TestCase):
    def _details(self, method):
        return _ClientCallDetails(
            method=method,
            timeout=None,
            metadata=None,
            credentials=None,
            wait_for_ready=None,
            compression=None,
        )

    def test_prepends_prefix_to_method(self):
        captured = {}

        def continuation(details, request):
            captured["details"] = details
            return "response"

        interceptor = _PathPrefixInterceptor("/sparkConnect/app-1/driver")
        details = self._details("/spark.connect.SparkConnectService/ExecutePlan")

        # Every call type rewrites the method the same way.
        for call in [
            interceptor.intercept_unary_unary,
            interceptor.intercept_unary_stream,
            interceptor.intercept_stream_unary,
            interceptor.intercept_stream_stream,
        ]:
            captured.clear()
            resp = call(continuation, details, "request")
            self.assertEqual("response", resp)
            self.assertEqual(
                "/sparkConnect/app-1/driver/spark.connect.SparkConnectService/ExecutePlan",
                captured["details"].method,
            )

    def test_preserves_other_call_details(self):
        captured = {}

        def continuation(details, request):
            captured["details"] = details
            return "response"

        interceptor = _PathPrefixInterceptor("/prefix")
        details = _ClientCallDetails(
            method="/svc/Method",
            timeout=30,
            metadata=(("k", "v"),),
            credentials=None,
            wait_for_ready=True,
            compression="gzip",
        )
        interceptor.intercept_unary_unary(continuation, details, "request")
        rewritten = captured["details"]
        self.assertEqual("/prefix/svc/Method", rewritten.method)
        self.assertEqual(30, rewritten.timeout)
        self.assertEqual((("k", "v"),), rewritten.metadata)
        self.assertTrue(rewritten.wait_for_ready)
        self.assertEqual("gzip", rewritten.compression)


if __name__ == "__main__":
    from pyspark.testing import main

    main()
