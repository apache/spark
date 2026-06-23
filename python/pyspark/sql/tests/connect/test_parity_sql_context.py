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
import warnings

from pyspark.sql.connect.context import SQLContext
from pyspark.sql.tests.test_sql_context import SQLContextTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class SQLContextParityTests(SQLContextTestsMixin, ReusedConnectTestCase):
    def setUp(self) -> None:
        super().setUp()
        SQLContext._instantiatedContext = None

    def tearDown(self) -> None:
        super().tearDown()
        SQLContext._instantiatedContext = None

    def _make_ctx(self) -> SQLContext:
        with warnings.catch_warnings():
            warnings.simplefilter("ignore", FutureWarning)
            return SQLContext(self.spark)

    def test_newSession_returns_distinct_instance(self) -> None:
        ctx = self._make_ctx()
        ctx2 = ctx.newSession()
        try:
            self.assertIsNot(ctx, ctx2)
        finally:
            # Release only the new server-side session and close its own client channel,
            # not SparkSession.stop(). See SQLContextConnectTests.test_newSession_returns
            # _fresh_state in test_connect_context.py for why: stop() would terminate the
            # shared local Connect server, while leaving the client open hangs the suite
            # in the client's atexit hook once the server goes away.
            client = ctx2.sparkSession.client
            try:
                client.release_session()
            except Exception:
                pass
            client.close()


if __name__ == "__main__":
    from pyspark.testing import main

    main()
