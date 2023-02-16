import unittest

from pyspark.sql.connect.client import SparkConnectClient
import pyspark.sql.connect.proto as proto


class SparkConnectClientTestCase(unittest.TestCase):
    def test_user_agent_passthrough(self):
        client = SparkConnectClient('sc://foo/;user_agent=bar')
        mock = MockService(client._session_id)
        client._stub = mock

        command = proto.Command()
        client.execute_command(command)

        self.assertIsNotNone(mock.req, "ExecutePlan API was not called when expected")
        self.assertEqual(mock.req.client_type, "bar")

    def test_user_agent_default(self):
        client = SparkConnectClient('sc://foo/')
        mock = MockService(client._session_id)
        client._stub = mock

        command = proto.Command()
        client.execute_command(command)

        self.assertIsNotNone(mock.req, "ExecutePlan API was not called when expected")
        self.assertEqual(mock.req.client_type, "_SPARK_CONNECT_PYTHON")


class MockService:
    """
    Simplest mock of the SparkConnectService.
    If this needs more complex logic, it needs to be replaced with Python mocking.
    """
    req: proto.ExecutePlanRequest | None

    def __init__(self, session_id: str):
        self._session_id = session_id
        self.req = None

    def ExecutePlan(self, req: proto.ExecutePlanRequest, metadata):
        self.req = req
        resp = proto.ExecutePlanResponse()
        resp.client_id = self._session_id
        return [resp]


if __name__ == '__main__':
    unittest.main()
