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

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_create_dataframe_schema_mismatch(self):
        super().test_create_dataframe_schema_mismatch()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_element_type_empty_rdd(self):
        super().test_infer_array_element_type_empty_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_array_merge_element_types_with_rdd(self):
        super().test_infer_array_merge_element_types_with_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_map_pair_type_empty_rdd(self):
        super().test_infer_map_pair_type_empty_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_map_merge_pair_types_with_rdd(self):
        super().test_infer_map_merge_pair_types_with_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_binary_type(self):
        super().test_infer_binary_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_long_type(self):
        super().test_infer_long_type()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_nested_dict_as_struct_with_rdd(self):
        super().test_infer_nested_dict_as_struct_with_rdd()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_nested_schema(self):
        super().test_infer_nested_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema(self):
        super().test_infer_schema()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema_to_local(self):
        super().test_infer_schema_to_local()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_infer_schema_upcast_int_to_string(self):
        super().test_infer_schema_upcast_int_to_string()

    @unittest.skip("Spark Connect does not support RDD but the tests depend on them.")
    def test_rdd_with_udt(self):
        super().test_rdd_with_udt()

    @unittest.skip("Requires JVM access.")
    def test_udt(self):
        super().test_udt()

    @unittest.skip("Does not test anything related to Spark Connect")
    def test_parse_datatype_string(self):
        super().test_parse_datatype_string()


if __name__ == "__main__":
    import unittest
    from pyspark.sql.tests.connect.test_parity_types import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore[import]

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
