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

from pyspark.errors import PySparkTypeError
from pyspark import pipelines as sdp


class DecoratorsTest(unittest.TestCase):
    def test_dataset_name_not_string(self):
        for decorator in [sdp.table, sdp.temporary_view, sdp.materialized_view]:
            with self.assertRaises(PySparkTypeError) as context:

                @decorator(name=5)
                def dataset_with_non_string_name():
                    raise NotImplementedError()

            assert context.exception.getCondition() == "NOT_STR"
            assert context.exception.getMessageParameters() == {
                "arg_name": "name",
                "arg_type": "int",
            }, context.exception.getMessageParameters()

    def test_invalid_partition_cols(self):
        for decorator in [sdp.table, sdp.materialized_view]:
            with self.assertRaises(PySparkTypeError) as context:

                @decorator(partition_cols=["a", 1, 2])  # type: ignore
                def dataset_with_invalid_partition_cols():
                    raise NotImplementedError()

            assert context.exception.getCondition() == "NOT_LIST_OF_STR"
            assert context.exception.getMessageParameters() == {
                "arg_name": "partition_cols",
                "arg_type": "list",
            }, context.exception.getMessageParameters()

    def test_decorator_with_positional_arg(self):
        for decorator in [sdp.table, sdp.temporary_view, sdp.materialized_view]:
            with self.assertRaises(PySparkTypeError) as context:
                decorator("table1")

            self.assertEqual(context.exception.getCondition(), "DECORATOR_ARGUMENT_NOT_CALLABLE")
            message_parameters = context.exception.getMessageParameters()
            assert message_parameters is not None
            self.assertEqual(message_parameters["decorator_name"], decorator.__name__)
            assert message_parameters["example_usage"].startswith(f"@{decorator.__name__}(")


if __name__ == "__main__":
    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
