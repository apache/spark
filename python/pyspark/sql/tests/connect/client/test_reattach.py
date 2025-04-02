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

import os
import unittest

from pyspark.util import is_remote_only
from pyspark.sql import SparkSession as PySparkSession
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.testing.pandasutils import PandasOnSparkTestUtils
from pyspark.testing.sqlutils import SQLTestUtils
from pyspark.testing.utils import eventually


@unittest.skipIf(is_remote_only(), "Requires JVM access")
class SparkConnectReattachTestCase(ReusedConnectTestCase, SQLTestUtils, PandasOnSparkTestUtils):
    @classmethod
    def setUpClass(cls):
        super(SparkConnectReattachTestCase, cls).setUpClass()
        # Disable the shared namespace so pyspark.sql.functions, etc point the regular
        # PySpark libraries.
        os.environ["PYSPARK_NO_NAMESPACE_SHARE"] = "1"

        cls.connect = cls.spark  # Switch Spark Connect session and regular PySpark session.
        cls.spark = PySparkSession._instantiatedSession
        assert cls.spark is not None

    @classmethod
    def tearDownClass(cls):
        try:
            # Stopping Spark Connect closes the session in JVM at the server.
            cls.spark = cls.connect
            del os.environ["PYSPARK_NO_NAMESPACE_SHARE"]
        finally:
            super(SparkConnectReattachTestCase, cls).tearDownClass()

    def test_release_sessions(self):
        big_enough_query = "select * from range(1000000)"
        query1 = self.connect.sql(big_enough_query).toLocalIterator()
        query2 = self.connect.sql(big_enough_query).toLocalIterator()
        query3 = self.connect.sql("select 1").toLocalIterator()

        next(query1)
        next(query2)

        jvm = PySparkSession._instantiatedSession._jvm  # type: ignore[union-attr]
        service = getattr(
            getattr(
                jvm.org.apache.spark.sql.connect.service,  # type: ignore[union-attr]
                "SparkConnectService$",
            ),
            "MODULE$",
        )

        @eventually(catch_assertions=True)
        def wait_for_requests():
            self.assertEqual(service.executionManager().listExecuteHolders().length(), 2)

        wait_for_requests()

        # Close session
        self.connect.client.release_session()
        # Calling release session again should be a no-op.
        self.connect.client.release_session()

        @eventually(catch_assertions=True)
        def wait_for_responses():
            self.assertEqual(service.executionManager().listExecuteHolders().length(), 0)

        wait_for_responses()

        # query1 and query2 could get either an:
        # OPERATION_CANCELED if it happens fast - when closing the session interrupted the queries,
        # and that error got pushed to the client buffers before the client got disconnected.
        # INVALID_HANDLE.SESSION_CLOSED if it happens slow - when closing the session interrupted
        # the client RPCs before it pushed out the error above. The client would then get an
        # INVALID_CURSOR.DISCONNECTED, which it will retry with a ReattachExecute, and then get an
        # INVALID_HANDLE.SESSION_CLOSED.

        def check_error(q):
            try:
                list(q)  # Iterate all.
            except Exception as e:  # noqa: F841
                return e

        e = check_error(query1)
        self.assertIsNotNone(e, "An exception has to be thrown")
        self.assertTrue("OPERATION_CANCELED" in str(e) or "INVALID_HANDLE.SESSION_CLOSED" in str(e))
        e = check_error(query2)
        self.assertIsNotNone(e, "An exception has to be thrown")
        self.assertTrue("OPERATION_CANCELED" in str(e) or "INVALID_HANDLE.SESSION_CLOSED" in str(e))

        # query3 has not been submitted before, so it should now fail with SESSION_CLOSED
        e = check_error(query3)
        self.assertIsNotNone(3, "An exception has to be thrown")
        self.assertIn("INVALID_HANDLE.SESSION_CLOSED", str(e))


if __name__ == "__main__":
    from pyspark.sql.tests.connect.client.test_reattach import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
