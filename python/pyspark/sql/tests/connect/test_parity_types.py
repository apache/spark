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

from pyspark.sql.tests.test_types import TypesTestsMixin
from pyspark.testing.connectutils import ReusedConnectTestCase


class TypesParityTests(TypesTestsMixin, ReusedConnectTestCase):
    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_apply_schema(self):
        super().test_apply_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_apply_schema_to_dict_and_rows(self):
        super().test_apply_schema_to_dict_and_rows()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_apply_schema_to_row(self):
        super().test_apply_schema_to_dict_and_rows()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_apply_schema_with_udt(self):
        super().test_apply_schema_with_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cast_to_string_with_udt(self):
        super().test_cast_to_string_with_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_cast_to_udt_with_udt(self):
        super().test_cast_to_udt_with_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_complex_nested_udt_in_df(self):
        super().test_complex_nested_udt_in_df()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_create_dataframe_schema_mismatch(self):
        super().test_create_dataframe_schema_mismatch()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_element_type_empty(self):
        super().test_infer_array_element_type_empty()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_element_type_with_struct(self):
        super().test_infer_array_element_type_with_struct()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_merge_element_types(self):
        super().test_infer_array_merge_element_types()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_binary_type(self):
        super().test_infer_binary_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_long_type(self):
        super().test_infer_long_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_nested_dict_as_struct(self):
        super().test_infer_nested_dict_as_struct()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_nested_schema(self):
        super().test_infer_nested_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema(self):
        super().test_infer_schema()

    # TODO(SPARK-42022): createDataFrame should autogenerate missing column names
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_infer_schema_not_enough_names(self):
        super().test_infer_schema_not_enough_names()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_infer_schema_specification(self):
        super().test_infer_schema_specification()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema_to_local(self):
        super().test_infer_schema_to_local()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema_upcast_int_to_string(self):
        super().test_infer_schema_upcast_int_to_string()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_infer_schema_with_udt(self):
        super().test_infer_schema_with_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_nested_udt_in_df(self):
        super().test_nested_udt_in_df()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_parquet_with_udt(self):
        super().test_parquet_with_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_simple_udt_in_df(self):
        super().test_simple_udt_in_df()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udf_with_udt(self):
        super().test_udf_with_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udt(self):
        super().test_udt()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_udt_with_none(self):
        super().test_udt_with_none()

    # TODO(SPARK-42020): createDataFrame with UDT
    @unittest.skip("Fails in Spark Connect, should enable.")
    def test_union_with_udt(self):
        super().test_union_with_udt()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_types import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
