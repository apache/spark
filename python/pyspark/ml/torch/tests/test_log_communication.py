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

from __future__ import absolute_import, division, print_function

import contextlib
import sys
import time
import unittest
from io import StringIO
from struct import pack
from typing import Any, Callable

import pyspark.ml.torch.log_communication
from pyspark.ml.torch.log_communication import (
    _SERVER_POLL_INTERVAL,
    LogStreamingClient,
    LogStreamingClientBase,
    LogStreamingServer,
)


@contextlib.contextmanager
def patch_stderr() -> StringIO:
    """patch stdout and give an output"""
    sys_stderr = sys.stderr
    io_out = StringIO()
    sys.stderr = io_out
    try:
        yield io_out
    finally:
        sys.stderr = sys_stderr


class LogStreamingServiceTestCase(unittest.TestCase):
    def setUp(self) -> None:
        self.default_truncate_msg_len = pyspark.ml.torch.log_communication._TRUNCATE_MSG_LEN
        pyspark.ml.torch.log_communication._TRUNCATE_MSG_LEN = 10

    def tearDown(self) -> None:
        pyspark.ml.torch.log_communication._TRUNCATE_MSG_LEN = self.default_truncate_msg_len

    def basic_test(self) -> None:
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        with patch_stderr() as output:
            client.send("msg 001")
            client.send("msg 002")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            output = output.getvalue()
            self.assertIn("msg 001\nmsg 002\n", output)
        client.close()
        server.shutdown()

    def test_truncate_message(self) -> None:
        msg1 = "abc"
        assert LogStreamingClientBase._maybe_truncate_msg(msg1) == msg1
        msg2 = "abcdefghijkl"
        assert LogStreamingClientBase._maybe_truncate_msg(msg2) == "abcdefghij...(truncated)"

    def test_multiple_clients(self) -> None:
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        client1 = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        client2 = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        with patch_stderr() as output:
            client1.send("c1 msg1")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            client2.send("c2 msg1")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            client1.send("c1 msg2")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            client2.send("c2 msg2")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            output = output.getvalue()
            self.assertIn("c1 msg1\nc2 msg1\nc1 msg2\nc2 msg2\n", output)
        client1.close()
        client2.close()
        server.shutdown()

    def test_client_should_fail_gracefully(self) -> None:
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        client.send("msg 001")
        server.shutdown()
        for i in range(5):
            client.send("msg 002")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
        self.assertTrue(client.failed)
        client.close()

    def test_client_send_intermittently(self) -> None:
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        with patch_stderr() as output:
            client._connect()
            # test client send half message first
            client.send("msg part1")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            # test client send another half message
            client.send(" msg part2")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            output = output.getvalue()
            self.assertIn("msg part1\n msg part2\n", output)
        client.close()
        server.shutdown()

    @staticmethod
    def test_server_shutdown() -> None:
        def run_test(client_ops: Callable) -> None:
            server = LogStreamingServer()
            server.start()
            time.sleep(1)
            client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
            client_ops(client)
            server.shutdown()
            client.close()

        def client_ops_close(client: Any) -> None:
            client.close()

        def client_ops_send_half_msg(client: Any) -> None:
            # Test server only recv incomplete message from client can exit.
            client._connect()
            client.sock.sendall(b"msg part1 ")
            time.sleep(_SERVER_POLL_INTERVAL + 1)

        def client_ops_send_a_msg(client: Any) -> None:
            client.send("msg1")
            time.sleep(_SERVER_POLL_INTERVAL + 1)

        def client_ops_send_a_msg_and_close(client: Any) -> None:
            client.send("msg1")
            client.close()
            time.sleep(_SERVER_POLL_INTERVAL + 1)

        run_test(client_ops_close)
        run_test(client_ops_send_half_msg)
        run_test(client_ops_send_a_msg)
        run_test(client_ops_send_a_msg_and_close)

    def test_unauthenticated_client_is_ignored(self) -> None:
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        good_client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        bad_client = LogStreamingClient("localhost", server.port, auth_secret="wrong-secret")
        no_secret_client = LogStreamingClient("localhost", server.port)
        with patch_stderr() as output:
            bad_client.send("forged msg")
            no_secret_client.send("noauth msg")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            good_client.send("real msg")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            output = output.getvalue()
            self.assertIn("real msg\n", output)
            self.assertNotIn("forged msg", output)
            self.assertNotIn("noauth msg", output)
        bad_client.close()
        no_secret_client.close()
        good_client.close()
        server.shutdown()

    def test_oversized_frame_drops_connection(self) -> None:
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        with patch_stderr() as output:
            client._connect()
            # Declare a frame far larger than the server-side cap: the server
            # must drop the connection instead of buffering the payload.
            client.sock.sendall(pack(">i", 0x7FFFFFFF))
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            client.send("dropped")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            self.assertNotIn("dropped", output.getvalue())
        client.close()
        server.shutdown()

    def test_empty_message_frame(self) -> None:
        # Blank log lines reach the client as empty strings; the zero-length
        # frame must not break the connection.
        server = LogStreamingServer()
        server.start()
        time.sleep(1)
        client = LogStreamingClient("localhost", server.port, auth_secret=server.auth_secret)
        with patch_stderr() as output:
            client.send("")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            client.send("alive")
            time.sleep(_SERVER_POLL_INTERVAL + 1)
            self.assertIn("\nalive\n", output.getvalue())
        client.close()
        server.shutdown()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
