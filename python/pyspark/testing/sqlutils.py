#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import glob
import math
import os
import shutil
import tempfile
from contextlib import contextmanager

from pyspark.sql import SparkSession
from pyspark.sql.types import Row
from pyspark.testing.utils import (
    ReusedPySparkTestCase,
    PySparkErrorTestUtils,
    have_pandas,
    pandas_requirement_message,
    have_pyarrow,
    pyarrow_requirement_message,
)
from pyspark.find_spark_home import _find_spark_home


SPARK_HOME = _find_spark_home()


def search_jar(project_relative_path, sbt_jar_name_prefix, mvn_jar_name_prefix):
    # Note that 'sbt_jar_name_prefix' and 'mvn_jar_name_prefix' are used since the prefix can
    # vary for SBT or Maven specifically. See also SPARK-26856
    project_full_path = os.path.join(SPARK_HOME, project_relative_path)

    # We should ignore the following jars
    ignored_jar_suffixes = ("javadoc.jar", "sources.jar", "test-sources.jar", "tests.jar")

    # Search jar in the project dir using the jar name_prefix for both sbt build and maven
    # build because the artifact jars are in different directories.
    sbt_build = glob.glob(
        os.path.join(project_full_path, "target/scala-*/%s*.jar" % sbt_jar_name_prefix)
    )
    maven_build = glob.glob(os.path.join(project_full_path, "target/%s*.jar" % mvn_jar_name_prefix))
    jar_paths = sbt_build + maven_build
    jars = [jar for jar in jar_paths if not jar.endswith(ignored_jar_suffixes)]

    if not jars:
        return None
    elif len(jars) > 1:
        raise RuntimeError("Found multiple JARs: %s; please remove all but one" % (", ".join(jars)))
    else:
        return jars[0]


test_not_compiled_message = None
try:
    from pyspark.sql.utils import require_test_compiled

    require_test_compiled()
except Exception as e:
    test_not_compiled_message = str(e)

test_compiled = test_not_compiled_message is None


class SQLTestUtils:
    """
    This util assumes the instance of this to have 'spark' attribute, having a spark session.
    It is usually used with 'ReusedSQLTestCase' class but can be used if you feel sure the
    the implementation of this class has 'spark' attribute.
    """

    @contextmanager
    def sql_conf(self, pairs):
        """
        A convenient context manager to test some configuration specific logic. This sets
        `value` to the configuration `key` and then restores it back when it exits.
        """
        assert isinstance(pairs, dict), "pairs should be a dictionary."
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        keys = pairs.keys()
        new_values = pairs.values()
        old_values = [self.spark.conf.get(key, None) for key in keys]
        for key, new_value in zip(keys, new_values):
            self.spark.conf.set(key, new_value)
        try:
            yield
        finally:
            for key, old_value in zip(keys, old_values):
                if old_value is None:
                    self.spark.conf.unset(key)
                else:
                    self.spark.conf.set(key, old_value)

    @contextmanager
    def database(self, *databases):
        """
        A convenient context manager to test with some specific databases. This drops the given
        databases if it exists and sets current database to "default" when it exits.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for db in databases:
                self.spark.sql("DROP DATABASE IF EXISTS %s CASCADE" % db)
            self.spark.catalog.setCurrentDatabase("default")

    @contextmanager
    def table(self, *tables):
        """
        A convenient context manager to test with some specific tables. This drops the given tables
        if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for t in tables:
                self.spark.sql("DROP TABLE IF EXISTS %s" % t)

    @contextmanager
    def tempView(self, *views):
        """
        A convenient context manager to test with some specific views. This drops the given views
        if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for v in views:
                self.spark.catalog.dropTempView(v)

    @contextmanager
    def temp_func(self, *functions):
        """
        A convenient context manager to test with some specific temporary functions.
        This drops the temporary functions if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for f in functions:
                self.spark.sql("DROP TEMPORARY FUNCTION IF EXISTS %s" % f)

    @contextmanager
    def function(self, *functions):
        """
        A convenient context manager to test with some specific functions. This drops the given
        functions if it exists.
        """
        assert hasattr(self, "spark"), "it should have 'spark' attribute, having a spark session."

        try:
            yield
        finally:
            for f in functions:
                self.spark.sql("DROP FUNCTION IF EXISTS %s" % f)

    @contextmanager
    def temp_env(self, pairs):
        assert isinstance(pairs, dict), "pairs should be a dictionary."

        keys = pairs.keys()
        new_values = pairs.values()
        old_values = [os.environ.get(key, None) for key in keys]
        for key, new_value in zip(keys, new_values):
            if new_value is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = new_value
        try:
            yield
        finally:
            for key, old_value in zip(keys, old_values):
                if old_value is None:
                    if key in os.environ:
                        del os.environ[key]
                else:
                    os.environ[key] = old_value

    @staticmethod
    def assert_close(a, b):
        c = [j[0] for j in b]
        diff = [abs(v - c[k]) < 1e-6 if math.isfinite(v) else v == c[k] for k, v in enumerate(a)]
        assert sum(diff) == len(a), f"sum: {sum(diff)}, len: {len(a)}"


class ReusedSQLTestCase(ReusedPySparkTestCase, SQLTestUtils, PySparkErrorTestUtils):
    @classmethod
    def setUpClass(cls):
        super(ReusedSQLTestCase, cls).setUpClass()
        cls._legacy_sc = cls.sc
        cls.spark = SparkSession(cls.sc)
        cls.tempdir = tempfile.NamedTemporaryFile(delete=False)
        os.unlink(cls.tempdir.name)
        cls.testData = [Row(key=i, value=str(i)) for i in range(100)]
        cls.df = cls.spark.createDataFrame(cls.testData)

    @classmethod
    def tearDownClass(cls):
        super(ReusedSQLTestCase, cls).tearDownClass()
        cls.spark.stop()
        shutil.rmtree(cls.tempdir.name, ignore_errors=True)

    def tearDown(self):
        try:
            self.spark._jsparkSession.cleanupPythonWorkerLogs()
        finally:
            super().tearDown()
