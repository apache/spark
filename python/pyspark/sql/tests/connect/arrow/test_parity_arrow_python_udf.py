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

import contextlib
import os
import tempfile
import unittest

from pyspark.sql.functions import udf
from pyspark.sql.tests.arrow.test_arrow_python_udf import ArrowPythonUDFTestsMixin
from pyspark.sql.tests.connect.test_parity_udf import UDFParityTests
from pyspark.testing.connectutils import ReusedConnectTestCase
from pyspark.util import PythonEvalType


class ArrowPythonUDFParityTests(UDFParityTests, ArrowPythonUDFTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()


class ArrowPythonUDFParityLegacyTestsMixin(ArrowPythonUDFTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled")
        finally:
            super().tearDownClass()

    @unittest.skip("Duplicate test as it is already tested in ArrowPythonUDFLegacyTests.")
    def test_udf_binary_type(self):
        super().test_udf_binary_type(self)

    @unittest.skip("Duplicate test as it is already tested in ArrowPythonUDFLegacyTests.")
    def test_udf_binary_type_in_nested_structures(self):
        super().test_udf_binary_type_in_nested_structures(self)


class ArrowPythonUDFParityNonLegacyTestsMixin(ArrowPythonUDFTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set(
            "spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled", "false"
        )

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled")
        finally:
            super().tearDownClass()

    @unittest.skip("Duplicate test as it is already tested in ArrowPythonUDFNonLegacyTests.")
    def test_udf_binary_type(self):
        super().test_udf_binary_type(self)

    @unittest.skip("Duplicate test as it is already tested in ArrowPythonUDFLegacyTests.")
    def test_udf_binary_type_in_nested_structures(self):
        super().test_udf_binary_type_in_nested_structures(self)


class ArrowPythonUDFParityLegacyTests(UDFParityTests, ArrowPythonUDFParityLegacyTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()


class ArrowPythonUDFParityNonLegacyTests(UDFParityTests, ArrowPythonUDFParityNonLegacyTestsMixin):
    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.spark.conf.set("spark.sql.execution.pythonUDF.arrow.enabled", "true")

    @classmethod
    def tearDownClass(cls):
        try:
            cls.spark.conf.unset("spark.sql.execution.pythonUDF.arrow.enabled")
        finally:
            super().tearDownClass()


class ArrowPythonUDFServerFallbackTests(ReusedConnectTestCase):
    """
    The client checks its own Python before it creates an Arrow-optimized Python UDF, so these hide
    a package from the Python that runs the UDF on the server only.
    """

    @contextlib.contextmanager
    def _not_installed_on_server(self, module):
        with tempfile.TemporaryDirectory() as d:
            with open(os.path.join(d, f"{module}.py"), "w") as f:
                f.write(f'raise ModuleNotFoundError("No module named {module!r}", name={module!r})')
            with self.sql_conf({"spark.pythonWorkerEnv.PYTHONPATH": d}):
                yield

    def test_falls_back_without_pyarrow(self):
        with self._not_installed_on_server("pyarrow"):

            @udf("int")
            def plus_one(x):
                return x + 1

            self.assertEqual(plus_one.evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF)
            df = self.spark.range(3).select(plus_one("id").alias("v"))
            self.assertEqual([row.v for row in df.collect()], [1, 2, 3])
            self.assertIn("BatchEvalPython", df._explain_string())

    def test_stays_arrow_optimized_without_pandas(self):
        with self._not_installed_on_server("pandas"):
            df = self.spark.sql(
                "SELECT 1 AS i, 'a' AS s, 1.5D AS d, CAST(1.25 AS DECIMAL(3, 2)) AS dec, "
                "X'01' AS b, DATE'2026-10-24' AS dt, TIMESTAMP'2026-10-24 01:02:03' AS ts, "
                "array(1, 2) AS a, map('k', 1) AS m, named_struct('f', 1) AS st"
            )
            identity = [udf(lambda x: x, f.dataType)(f.name).alias(f.name) for f in df.schema]
            result = df.select(*identity)
            self.assertEqual(result.collect(), df.collect())
            self.assertIn("ArrowEvalPython", result._explain_string())

    def test_falls_back_without_pandas_in_legacy_mode(self):
        with (
            self._not_installed_on_server("pandas"),
            self.sql_conf(
                {"spark.sql.legacy.execution.pythonUDF.pandas.conversion.enabled": "true"}
            ),
        ):

            @udf("int")
            def plus_one(x):
                return x + 1

            self.assertEqual(plus_one.evalType, PythonEvalType.SQL_ARROW_BATCHED_UDF)
            df = self.spark.range(3).select(plus_one("id").alias("v"))
            self.assertEqual([row.v for row in df.collect()], [1, 2, 3])
            self.assertIn("BatchEvalPython", df._explain_string())


if __name__ == "__main__":
    from pyspark.testing import main

    main()
