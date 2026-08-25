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
from pyspark.sql.tests.connect.test_connect_basic import SparkConnectSQLTestCase

PREFIX = "spark.pythonWorkerEnv."


@udf("string")
def read_env(name):
    import os

    return os.environ.get(name, "<unset>")


class SparkConnectPythonWorkerEnvTests(SparkConnectSQLTestCase):
    def _read_in_worker(self, name):
        """The value of `name` as seen by os.environ inside a Python worker."""
        df = self.connect.range(1).select(read_env(lit(name)).alias("value"))
        return df.collect()[0]["value"]

    def test_env_var_reaches_udf(self):
        self.connect.conf.set(PREFIX + "MY_SETTING", "abc")
        try:
            self.assertEqual(self._read_in_worker("MY_SETTING"), "abc")
        finally:
            self.connect.conf.unset(PREFIX + "MY_SETTING")

    def test_env_var_unset_is_not_visible(self):
        # Nothing set: the UDF sees no such variable.
        self.assertEqual(self._read_in_worker("NEVER_SET_BY_THIS_TEST"), "<unset>")

    def test_env_var_update_is_picked_up(self):
        self.connect.conf.set(PREFIX + "ROTATING", "first")
        try:
            self.assertEqual(self._read_in_worker("ROTATING"), "first")
            self.connect.conf.set(PREFIX + "ROTATING", "second")
            self.assertEqual(self._read_in_worker("ROTATING"), "second")
        finally:
            self.connect.conf.unset(PREFIX + "ROTATING")

    def test_env_var_unset_removes_it(self):
        self.connect.conf.set(PREFIX + "TEMPORARY", "value")
        self.assertEqual(self._read_in_worker("TEMPORARY"), "value")
        self.connect.conf.unset(PREFIX + "TEMPORARY")
        self.assertEqual(self._read_in_worker("TEMPORARY"), "<unset>")

    def test_empty_value_is_visible_as_empty(self):
        # `FOO=` in a shell: the variable exists and its value is the empty string.
        self.connect.conf.set(PREFIX + "EMPTY", "")
        try:
            self.assertEqual(self._read_in_worker("EMPTY"), "")
        finally:
            self.connect.conf.unset(PREFIX + "EMPTY")

    def test_platform_owned_variable_is_not_overridden(self):
        # The worker factory sets PYTHONUNBUFFERED after applying the user's map, so the platform
        # value has to win.
        self.connect.conf.set(PREFIX + "PYTHONUNBUFFERED", "NO")
        try:
            self.assertEqual(self._read_in_worker("PYTHONUNBUFFERED"), "YES")
        finally:
            self.connect.conf.unset(PREFIX + "PYTHONUNBUFFERED")

    def test_invalid_name_fails_the_query(self):
        self.connect.conf.set(PREFIX + "1INVALID", "x")
        try:
            with self.assertRaises(Exception) as context:
                self._read_in_worker("ANY")
            self.assertIn("INVALID_PYTHON_WORKER_ENV_VAR_NAME", str(context.exception))
        finally:
            self.connect.conf.unset(PREFIX + "1INVALID")

    def test_value_containing_nul_fails_the_query(self):
        self.connect.conf.set(PREFIX + "WITH_NUL", "abc\x00def")
        try:
            with self.assertRaises(Exception) as context:
                self._read_in_worker("WITH_NUL")
            message = str(context.exception)
            self.assertIn("INVALID_PYTHON_WORKER_ENV_VAR_VALUE", message)
            # The value must not appear in the failure.
            self.assertNotIn("abc", message)
        finally:
            self.connect.conf.unset(PREFIX + "WITH_NUL")

    def test_env_var_reaches_map_in_pandas(self):
        import pandas as pd

        self.connect.conf.set(PREFIX + "MAP_SETTING", "mapped")
        try:

            def func(iterator):
                import os

                for pdf in iterator:
                    yield pd.DataFrame({"value": [os.environ.get("MAP_SETTING", "<unset>")]})

            result = self.connect.range(1).mapInPandas(func, "value string").collect()
            self.assertEqual(result[0]["value"], "mapped")
        finally:
            self.connect.conf.unset(PREFIX + "MAP_SETTING")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
