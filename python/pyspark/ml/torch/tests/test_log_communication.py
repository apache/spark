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
from io import StringIO
import sys
import time
from typing import Any, Callable
import unittest

import pyspark.ml.torch.log_communication
from pyspark.ml.torch.log_communication import (
    LogStreamingServer,
    LogStreamingClient,
    LogStreamingClientBase,
    _SERVER_POLL_INTERVAL,
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
        client = LogStreamingClient("localhost", server.port)
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
        client1 = LogStreamingClient("localhost", server.port)
        client2 = LogStreamingClient("localhost", server.port)
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
        client = LogStreamingClient("localhost", server.port)
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
        client = LogStreamingClient("localhost", server.port)
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
            client = LogStreamingClient("localhost", server.port)
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


if __name__ == "__main__":
    from pyspark.ml.torch.tests.test_log_communication import *  # noqa: F401,F403

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
