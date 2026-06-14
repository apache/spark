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
"""
Tests for SPARK-57425: Reattach iterator recovers when short-TTL
credentials expire mid-stream.
"""

import unittest
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    import grpc
    import pyspark.sql.connect.proto as proto
    from pyspark.sql.connect.client.reattach import ExecutePlanResponseReattachableIterator
    from pyspark.sql.connect.client.retries import Retrying, DefaultPolicy


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class TestSPARK57425(unittest.TestCase):
    """Tests for SPARK-57425 fix."""

    def test_permission_denied_is_retryable(self):
        """PERMISSION_DENIED should be retryable after the fix."""
        policy = DefaultPolicy()

        class PermDeniedError(grpc.RpcError, grpc.Call):
            def code(self):
                return grpc.StatusCode.PERMISSION_DENIED

            def details(self):
                return "Token expired"

            def trailing_metadata(self):
                return None

        self.assertTrue(policy.can_retry(PermDeniedError()))

    def test_unauthenticated_is_retryable(self):
        """UNAUTHENTICATED should be retryable after the fix."""
        policy = DefaultPolicy()

        class UnauthError(grpc.RpcError, grpc.Call):
            def code(self):
                return grpc.StatusCode.UNAUTHENTICATED

            def details(self):
                return "Token expired"

            def trailing_metadata(self):
                return None

        self.assertTrue(policy.can_retry(UnauthError()))

    def test_metadata_refreshed_on_reattach(self):
        """Metadata callable is invoked on each RPC so credentials are fresh."""
        call_count = {"n": 0}

        def refreshing_metadata():
            call_count["n"] += 1
            return [("authorization", f"Bearer token-v{call_count['n']}")]

        metadata_log = []

        class TrackingStub:
            def ExecutePlan(self, request, metadata=None, timeout=None):
                metadata_log.append(("execute", list(metadata) if metadata else []))
                resp = proto.ExecutePlanResponse()
                resp.session_id = "s"
                resp.operation_id = request.operation_id
                resp.response_id = "r1"
                return iter([resp])  # no result_complete -> triggers reattach

            def ReattachExecute(self, request, metadata=None, timeout=None):
                metadata_log.append(("reattach", list(metadata) if metadata else []))
                resp = proto.ExecutePlanResponse()
                resp.session_id = "s"
                resp.operation_id = request.operation_id
                resp.response_id = "r2"
                resp.result_complete.CopyFrom(proto.ExecutePlanResponse.ResultComplete())
                return iter([resp])

            def ReleaseExecute(self, request, metadata=None, timeout=None):
                pass

        req = proto.ExecutePlanRequest()
        req.session_id = "s"
        req.user_context.user_id = "u"

        it = ExecutePlanResponseReattachableIterator(
            request=req,
            stub=TrackingStub(),
            retrying=lambda: Retrying(DefaultPolicy()),
            metadata=refreshing_metadata,  # callable!
        )
        list(it)

        exec_md = [m for rpc, m in metadata_log if rpc == "execute"]
        reattach_md = [m for rpc, m in metadata_log if rpc == "reattach"]

        # Metadata should be DIFFERENT between execute and reattach (refreshed)
        self.assertNotEqual(exec_md[0], reattach_md[0],
                            "Reattach should use fresh metadata from callable")
        self.assertIn("token-v1", exec_md[0][0][1])
        # Reattach token version > 1 (exact number depends on interleaved ReleaseExecute calls)
        self.assertNotIn("token-v1", reattach_md[0][0][1])

    def test_static_metadata_still_works(self):
        """Backwards compat: plain iterable metadata still works."""

        class SimpleStub:
            def ExecutePlan(self, request, metadata=None, timeout=None):
                resp = proto.ExecutePlanResponse()
                resp.session_id = "s"
                resp.operation_id = request.operation_id
                resp.response_id = "r1"
                resp.result_complete.CopyFrom(proto.ExecutePlanResponse.ResultComplete())
                return iter([resp])

            def ReleaseExecute(self, request, metadata=None, timeout=None):
                pass

        req = proto.ExecutePlanRequest()
        req.session_id = "s"
        req.user_context.user_id = "u"

        # Pass static metadata (old style) -- should still work
        it = ExecutePlanResponseReattachableIterator(
            request=req,
            stub=SimpleStub(),
            retrying=lambda: Retrying(DefaultPolicy()),
            metadata=[("authorization", "Bearer static-token")],
        )
        responses = list(it)
        self.assertEqual(len(responses), 1)

    def test_permission_denied_triggers_reattach(self):
        """After fix: PERMISSION_DENIED mid-stream triggers retry with fresh creds."""
        call_count = {"n": 0}

        def refreshing_metadata():
            call_count["n"] += 1
            return [("authorization", f"Bearer token-v{call_count['n']}")]

        class RecoveringStub:
            def __init__(self):
                self.execute_count = 0

            def ExecutePlan(self, request, metadata=None, timeout=None):
                self.execute_count += 1

                class Iter:
                    def __init__(self, op_id, fail):
                        self._op_id = op_id
                        self._done = False
                        self._fail = fail

                    def __iter__(self):
                        return self

                    def __next__(self):
                        if not self._done:
                            self._done = True
                            if self._fail:
                                e = grpc.RpcError()
                                e.code = lambda: grpc.StatusCode.PERMISSION_DENIED
                                e.details = lambda: "Token expired"
                                e.trailing_metadata = lambda: None
                                raise e
                            resp = proto.ExecutePlanResponse()
                            resp.session_id = "s"
                            resp.operation_id = self._op_id
                            resp.response_id = "r1"
                            resp.result_complete.CopyFrom(
                                proto.ExecutePlanResponse.ResultComplete())
                            return resp
                        raise StopIteration()

                # First call fails with PERMISSION_DENIED, second succeeds
                return Iter(request.operation_id, fail=(self.execute_count == 1))

            def ReattachExecute(self, request, metadata=None, timeout=None):
                resp = proto.ExecutePlanResponse()
                resp.session_id = "s"
                resp.operation_id = request.operation_id
                resp.response_id = "r2"
                resp.result_complete.CopyFrom(proto.ExecutePlanResponse.ResultComplete())
                return iter([resp])

            def ReleaseExecute(self, request, metadata=None, timeout=None):
                pass

        stub = RecoveringStub()
        req = proto.ExecutePlanRequest()
        req.session_id = "s"
        req.user_context.user_id = "u"

        it = ExecutePlanResponseReattachableIterator(
            request=req,
            stub=stub,
            retrying=lambda: Retrying(DefaultPolicy()),
            metadata=refreshing_metadata,
        )

        # Should NOT raise -- PERMISSION_DENIED is now retried with fresh creds
        responses = list(it)
        self.assertEqual(len(responses), 1)


if __name__ == "__main__":
    from pyspark.testing import main
    main()
