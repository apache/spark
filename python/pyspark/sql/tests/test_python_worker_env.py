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

These run a real Python worker, so they cover what the unit tests cannot: that a value set through
`spark.conf` actually reaches `os.environ` inside a UDF. The mixin is shared by the classic suite
here and the Spark Connect parity suite, so both front ends are held to the same behaviour.
"""

import os

from pyspark.sql.functions import lit, udf
from pyspark.testing.sqlutils import ReusedSQLTestCase

PREFIX = "spark.pythonWorkerEnv."

# A process environment cannot carry NUL. Built from its code point rather than written as an
# escape, so nothing along the way can turn it into a literal control character.
NUL = chr(0)


class PythonWorkerEnvMixin:
    """Behaviour that must hold identically on classic Spark and on Spark Connect."""

    def _read_in_worker(self, name, use_arrow=None):
        """The value of `name` as seen by os.environ inside a Python worker.

        The UDF is defined here rather than at module scope because cloudpickle serializes a
        module-level function by reference: the worker would try to import this test module and
        fail. A local function is serialized by value.

        `use_arrow` selects the execution path. Left as None it follows
        `spark.sql.execution.pythonUDF.arrow.enabled`, which is enabled by default, so the default
        case exercises the Arrow path.
        """

        def read_env(variable):
            import os

            return os.environ.get(variable, "<unset>")

        read_env_udf = udf(read_env, "string", useArrow=use_arrow)
        df = self.spark.range(1).select(read_env_udf(lit(name)).alias("value"))
        return df.collect()[0]["value"]

    def _set_env(self, name, value):
        self.spark.conf.set(PREFIX + name, value)

    def _unset_env(self, name):
        self.spark.conf.unset(PREFIX + name)

    # -- the two execution paths of a regular scalar Python UDF ----------------

    def test_env_var_reaches_arrow_udf(self):
        # Arrow is the default, so this is the ordinary case.
        self._set_env("MY_SETTING", "abc")
        try:
            self.assertEqual(self._read_in_worker("MY_SETTING", use_arrow=True), "abc")
        finally:
            self._unset_env("MY_SETTING")

    def test_env_var_reaches_non_arrow_udf(self):
        self._set_env("MY_SETTING", "abc")
        try:
            self.assertEqual(self._read_in_worker("MY_SETTING", use_arrow=False), "abc")
        finally:
            self._unset_env("MY_SETTING")

    def test_env_var_reaches_udf_with_the_default_path(self):
        self._set_env("MY_SETTING", "abc")
        try:
            self.assertEqual(self._read_in_worker("MY_SETTING"), "abc")
        finally:
            self._unset_env("MY_SETTING")

    # -- lifecycle ------------------------------------------------------------

    def test_env_var_unset_is_not_visible(self):
        self.assertEqual(self._read_in_worker("NEVER_SET_BY_THIS_TEST"), "<unset>")

    def test_env_var_change_takes_effect_on_the_next_action(self):
        # The environment is installed when a worker is launched, so a change is picked up without
        # rebuilding the UDF, and a cached or reused plan cannot pin an old value.
        self._set_env("ROTATING", "first")
        try:
            self.assertEqual(self._read_in_worker("ROTATING"), "first")
            self._set_env("ROTATING", "second")
            self.assertEqual(self._read_in_worker("ROTATING"), "second")
        finally:
            self._unset_env("ROTATING")

    def test_env_var_change_takes_effect_on_a_reused_dataframe(self):
        # The same DataFrame, and so the same plan and the same built function, observed twice
        # across a change. This is what a plan cache would otherwise get wrong.
        def read_env(variable):
            import os

            return os.environ.get(variable, "<unset>")

        read_env_udf = udf(read_env, "string")
        df = self.spark.range(1).select(read_env_udf(lit("REUSED")).alias("value"))
        self._set_env("REUSED", "first")
        try:
            self.assertEqual(df.collect()[0]["value"], "first")
            self._set_env("REUSED", "second")
            self.assertEqual(df.collect()[0]["value"], "second")
        finally:
            self._unset_env("REUSED")

    def test_empty_value_is_visible_as_empty(self):
        # `FOO=` in a shell: the variable exists and its value is the empty string.
        self._set_env("EMPTY", "")
        try:
            self.assertEqual(self._read_in_worker("EMPTY"), "")
        finally:
            self._unset_env("EMPTY")

    def test_platform_owned_variable_is_not_overridden(self):
        # The worker factory sets PYTHONUNBUFFERED after applying the session's map, so the platform
        # value wins by write order and the name does not need reserving.
        self._set_env("PYTHONUNBUFFERED", "NO")
        try:
            self.assertEqual(self._read_in_worker("PYTHONUNBUFFERED"), "YES")
        finally:
            self._unset_env("PYTHONUNBUFFERED")

    def test_pythonpath_reaches_the_worker_import_path(self):
        # PYTHONPATH is neither reserved nor overridden: PythonWorkerFactory folds the session's
        # value into the path it computes, so the entry reaches the worker rather than being
        # dropped. That is the whole guarantee. The relative order is deliberately not asserted:
        # `sparkPythonPath` is empty when SPARK_HOME is unset and Spark's classes did not come from
        # a jar, and `mergePythonPaths` drops empty entries, so the session's path can legitimately
        # come first. Leaving the name settable rests on the session already choosing the code its
        # own worker runs, not on Spark's entries winning.
        marker = "/tmp/spark-58752-extra-modules"
        self._set_env("PYTHONPATH", marker)
        try:
            entries = self._read_in_worker("PYTHONPATH").split(os.pathsep)
            self.assertIn(marker, entries)
        finally:
            self._unset_env("PYTHONPATH")

    def test_reserved_name_is_refused(self):
        # Spark sets SPARK_PIPELINED_UDF only when pipelined execution is on, and the worker reads
        # it to choose its wire protocol, so a session must not be able to set it at all. One name
        # is enough end to end: `PythonWorkerEnvironmentSuite` iterates every reserved prefix and
        # name, including the conditional PYTHON_* ones and PYTHON_WORKER_FACTORY_SOCK_DIR.
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "SPARK_PIPELINED_UDF", "1")
        self.assertIn("RESERVED_PYTHON_WORKER_ENV_VAR_NAME", str(context.exception))
        self.assertIsNone(self.spark.conf.get(PREFIX + "SPARK_PIPELINED_UDF", None))

    def test_sql_set_is_refused_too(self):
        # SQL `SET` reaches `RuntimeConfig.set` through `SetCommand`, so it goes through the same
        # check as `spark.conf.set` rather than storing the value for a later query to trip over.
        with self.assertRaises(Exception) as context:
            self.spark.sql("SET {}1INVALID=x".format(PREFIX)).collect()
        message = str(context.exception)
        self.assertIn("1INVALID", message)
        # A rejection names the variable but must never quote its value.
        self.assertNotIn("is not valid: x", message)
        self.assertIsNone(self.spark.conf.get(PREFIX + "1INVALID", None))

    def test_oversized_environment_is_refused(self):
        # Exceeds the default 128 KiB spark.sql.pythonWorkerEnv.maxTotalSizeBytes. The limits are
        # static configs, so a shared session cannot lower one for a test; going over the default is
        # what lets this run end to end. The Scala suite covers the other limits by overriding them
        # on the SparkConf directly.
        big = "x" * (200 * 1024)
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "BIG", big)
        message = str(context.exception)
        self.assertIn("PYTHON_WORKER_ENV_TOO_LARGE", message)
        # The rejection reports sizes, never the offending value.
        self.assertNotIn("xxxxxxxxxx", message)
        self.assertIsNone(self.spark.conf.get(PREFIX + "BIG", None))

    # -- families that are not covered yet ------------------------------------

    def test_map_in_pandas_does_not_receive_the_environment(self):
        # mapInPandas has its own runner and is not in scope for this change. Asserted rather than
        # left untested so that widening the scope has to update this test deliberately.
        import pandas as pd

        self._set_env("MAP_SETTING", "mapped")
        try:

            def func(iterator):
                import os

                for _ in iterator:
                    yield pd.DataFrame({"value": [os.environ.get("MAP_SETTING", "<unset>")]})

            result = self.spark.range(1).mapInPandas(func, "value string").collect()
            self.assertEqual(result[0]["value"], "<unset>")
        finally:
            self._unset_env("MAP_SETTING")

    # -- rejection ------------------------------------------------------------

    def test_invalid_name_fails_the_set(self):
        # `spark.conf.set` goes through `RuntimeConfig.set` on both front ends -- the Spark Connect
        # server writes through a classic `RuntimeConfig` too -- so an invalid variable is refused
        # at the call rather than at a later query.
        with self.assertRaises(Exception) as context:
            self.spark.conf.set(PREFIX + "1INVALID", "x")
        message = str(context.exception)
        # Assert on the message text rather than the condition name, which a client is not required
        # to surface in the string form of the exception.
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
        # A value can be a secret, so it must not appear in the failure.
        self.assertNotIn("abc", message)
        self.assertIsNone(self.spark.conf.get(PREFIX + "WITH_NUL", None))

    def test_env_var_reaches_a_udf_inside_a_higher_order_function(self):
        # A UDF written inside a lambda is lifted to the element-wise eval type, which is enabled by
        # default. It is the same UDF to the user, so it has to receive the environment too.
        from pyspark.sql.functions import array, transform

        self._set_env("HOF_SETTING", "lifted")
        try:

            def read_env(_):
                import os

                return os.environ.get("HOF_SETTING", "<unset>")

            read_env_udf = udf(read_env, "string")
            df = self.spark.range(1).select(
                transform(array(lit(1), lit(2)), lambda x: read_env_udf(x)).alias("value")
            )
            self.assertEqual(df.collect()[0]["value"], ["lifted", "lifted"])
        finally:
            self._unset_env("HOF_SETTING")


class PythonWorkerEnvTests(PythonWorkerEnvMixin, ReusedSQLTestCase):
    @classmethod
    def conf(cls):
        # An application-scoped variable, so the tests can pin the precedence between it and a
        # session variable of the same name.
        return super().conf().set("spark.executorEnv.SHARED", "from_application")

    # `spark.executorEnv.*` reaches a Python worker only on classic: `SparkContext` copies those
    # entries into `sc.environment` (core/context.py), which becomes the function's own envVars.
    # A Spark Connect client has no SparkContext and the server builds the function with an empty
    # map, so there is no inherited value there to fall back to or to override.

    def test_unset_reveals_the_inherited_value(self):
        self._set_env("SHARED", "from_session")
        try:
            self.assertEqual(self._read_in_worker("SHARED"), "from_session")
        finally:
            self._unset_env("SHARED")
        self.assertEqual(self._read_in_worker("SHARED"), "from_application")

    def test_an_environment_stored_by_a_bypass_path_fails_at_worker_launch(self):
        # `RuntimeConfig.set` now refuses an invalid variable, and SQL `SET` reaches it too, so the
        # only way left into a bad session state is a write straight to `SQLConf` -- which is what
        # the configurations merged into a new session by `SparkSession.builder` do. Reached here
        # through the JVM, so this is classic-only, and it is the end-to-end proof that the check
        # performed when a worker is launched is still what makes an invalid environment unusable.
        jconf = self.spark._jsparkSession.sessionState().conf()
        jconf.setConfString(PREFIX + "1INVALID", "x")
        try:
            self.assertEqual(self.spark.conf.get(PREFIX + "1INVALID"), "x")
            with self.assertRaises(Exception) as context:
                self._read_in_worker("ANY")
            self.assertIn("1INVALID", str(context.exception))
        finally:
            self._unset_env("1INVALID")


if __name__ == "__main__":
    from pyspark.testing import main

    main()
