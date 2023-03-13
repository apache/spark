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
from pyspark.testing.connectutils import should_test_connect

if should_test_connect:
    import pandas as pd
    import pyarrow as pa


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
        resp.session_id = self._session_id

        pdf = pd.DataFrame(data={"col1": [1, 2]})
        schema = pa.Schema.from_pandas(pdf)
        table = pa.Table.from_pandas(pdf)
        sink = pa.BufferOutputStream()

        writer = pa.ipc.new_stream(sink, schema=schema)
        writer.write(table)
        writer.close()

        buf = sink.getvalue()
        resp.arrow_batch.data = buf.to_pybytes()
        return [resp]


if __name__ == "__main__":
    unittest.main()
