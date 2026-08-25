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
"""End-to-end tests for the session's Python worker environment.

These run a real Python worker, so they cover what the planner tests cannot: that a value set
through `spark.conf` actually reaches `os.environ` inside a UDF.
"""

from pyspark.sql.functions import lit, udf
from pyspark.testing.connectutils import ReusedConnectTestCase

PREFIX = "spark.pythonWorkerEnv."

# A process environment cannot carry NUL. Built from its code point rather than written as an
# escape, so nothing along the way can turn it into a literal control character.
NUL = chr(0)


class SparkConnectPythonWorkerEnvTests(ReusedConnectTestCase):
    def _read_in_worker(self, name):
        """The value of `name` as seen by os.environ inside a Python worker.

        The UDF is built here rather than at module scope: constructing one needs a live session,
        so a module-level definition would fail at import.
        """

        def read_env(variable):
            import os

            return os.environ.get(variable, "<unset>")

        read_env_udf = udf(read_env, "string")
        df = self.spark.range(1).select(read_env_udf(lit(name)).alias("value"))
        return df.collect()[0]["value"]

    def test_env_var_reaches_udf(self):
        self.spark.conf.set(PREFIX + "MY_SETTING", "abc")
        try:
            self.assertEqual(self._read_in_worker("MY_SETTING"), "abc")
        finally:
            self.spark.conf.unset(PREFIX + "MY_SETTING")

    def test_env_var_unset_is_not_visible(self):
        # Nothing set: the UDF sees no such variable.
        self.assertEqual(self._read_in_worker("NEVER_SET_BY_THIS_TEST"), "<unset>")

    def test_env_var_update_is_picked_up(self):
        self.spark.conf.set(PREFIX + "ROTATING", "first")
        try:
            self.assertEqual(self._read_in_worker("ROTATING"), "first")
            self.spark.conf.set(PREFIX + "ROTATING", "second")
            self.assertEqual(self._read_in_worker("ROTATING"), "second")
        finally:
            self.spark.conf.unset(PREFIX + "ROTATING")

    def test_env_var_unset_removes_it(self):
        # try/finally like the others: the session is shared, so a failed assertion here must not
        # leave the configuration behind to contaminate a later test.
        self.spark.conf.set(PREFIX + "TEMPORARY", "value")
        try:
            self.assertEqual(self._read_in_worker("TEMPORARY"), "value")
            self.spark.conf.unset(PREFIX + "TEMPORARY")
            self.assertEqual(self._read_in_worker("TEMPORARY"), "<unset>")
        finally:
            self.spark.conf.unset(PREFIX + "TEMPORARY")

    def test_empty_value_is_visible_as_empty(self):
        # `FOO=` in a shell: the variable exists and its value is the empty string.
        self.spark.conf.set(PREFIX + "EMPTY", "")
        try:
            self.assertEqual(self._read_in_worker("EMPTY"), "")
        finally:
            self.spark.conf.unset(PREFIX + "EMPTY")

    def test_platform_owned_variable_is_not_overridden(self):
        # The worker factory sets PYTHONUNBUFFERED after applying the user's map, so the platform
        # value has to win.
        self.spark.conf.set(PREFIX + "PYTHONUNBUFFERED", "NO")
        try:
            self.assertEqual(self._read_in_worker("PYTHONUNBUFFERED"), "YES")
        finally:
            self.spark.conf.unset(PREFIX + "PYTHONUNBUFFERED")

    def test_invalid_name_fails_the_set(self):
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "1INVALID", "x")
        message = str(context.exception)
        # Assert on the message text rather than the condition name, which the client is not
        # required to surface in the string form of the exception.
        self.assertIn("is not valid", message)
        self.assertIn("1INVALID", message)
        # The write was refused, so nothing was stored to break later queries.
        self.assertIsNone(self.spark.conf.get(PREFIX + "1INVALID", None))

    def test_value_containing_nul_fails_the_set(self):
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "WITH_NUL", "abc" + NUL + "def")
        message = str(context.exception)
        self.assertIn("NUL character", message)
        self.assertIn("WITH_NUL", message)
        # The value must not appear in the failure.
        self.assertNotIn("abc", message)
        self.assertIsNone(self.spark.conf.get(PREFIX + "WITH_NUL", None))

    def test_invalid_name_set_through_sql_fails_the_query(self):
        # SQL `SET` writes the session configuration without going through the config RPC, so the
        # write-time check cannot see it. The check performed when a Python function is built is
        # what stops an environment installed this way from reaching a worker.
        self.spark.sql("SET {}1INVALID=x".format(PREFIX)).collect()
        try:
            self.assertEqual(self.spark.conf.get(PREFIX + "1INVALID"), "x")
            with self.assertRaises(Exception) as context:
                self._read_in_worker("ANY")
            message = str(context.exception)
            self.assertIn("is not valid", message)
            self.assertIn("1INVALID", message)
        finally:
            self.spark.conf.unset(PREFIX + "1INVALID")

    def test_env_var_reaches_map_in_pandas(self):
        import pandas as pd

        self.spark.conf.set(PREFIX + "MAP_SETTING", "mapped")
        try:

            def func(iterator):
                import os

                for _ in iterator:
                    yield pd.DataFrame({"value": [os.environ.get("MAP_SETTING", "<unset>")]})

            result = self.spark.range(1).mapInPandas(func, "value string").collect()
            self.assertEqual(result[0]["value"], "mapped")
        finally:
            self.spark.conf.unset(PREFIX + "MAP_SETTING")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
