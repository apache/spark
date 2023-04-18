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
from typing import Optional

from pyspark.sql.connect.client import SparkConnectClient
import pyspark.sql.connect.proto as proto


class SparkConnectClientTestCase(unittest.TestCase):
    def test_user_agent_passthrough(self):
        client = SparkConnectClient("sc://foo/;user_agent=bar")
        mock = MockService(client._session_id)
        client._stub = mock

        command = proto.Command()
        client.execute_command(command)

        self.assertIsNotNone(mock.req, "ExecutePlan API was not called when expected")
        self.assertEqual(mock.req.client_type, "bar")

    def test_user_agent_default(self):
        client = SparkConnectClient("sc://foo/")
        mock = MockService(client._session_id)
        client._stub = mock

        command = proto.Command()
        client.execute_command(command)

        self.assertIsNotNone(mock.req, "ExecutePlan API was not called when expected")
        self.assertEqual(mock.req.client_type, "_SPARK_CONNECT_PYTHON")

    def test_properties(self):
        client = SparkConnectClient("sc://foo/;token=bar")
        self.assertEqual(client.token, "bar")
        self.assertEqual(client.host, "foo")

        client = SparkConnectClient("sc://foo/")
        self.assertIsNone(client.token)


class MockService:
    # Simplest mock of the SparkConnectService.
    # If this needs more complex logic, it needs to be replaced with Python mocking.

    req: Optional[proto.ExecutePlanRequest]

    def __init__(self, session_id: str):
        self._session_id = session_id
        self.req = None

    def ExecutePlan(self, req: proto.ExecutePlanRequest, metadata):
        self.req = req
        resp = proto.ExecutePlanResponse()
        resp.client_id = self._session_id
        return [resp]


if __name__ == "__main__":
    unittest.main()
