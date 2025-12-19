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

from pyspark.sql.tests.connect.test_parity_udf import UDFParityTests
from pyspark.sql.tests.arrow.test_arrow_python_udf import ArrowPythonUDFTestsMixin


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


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.arrow.test_parity_arrow_python_udf import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
