# -*- encoding: utf-8 -*-
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

import json
import unittest

from pyspark.errors import PySparkValueError
from pyspark.errors.error_classes import ERROR_CLASSES_JSON
from pyspark.errors.utils import ErrorClassesReader


class ErrorsTest(unittest.TestCase):
    def test_error_classes_sorted(self):
        # Test error classes is sorted alphabetically
        error_reader = ErrorClassesReader()
        error_class_names = list(error_reader.error_info_map.keys())
        for i in range(len(error_class_names) - 1):
            self.assertTrue(
                error_class_names[i] < error_class_names[i + 1],
                f"Error class [{error_class_names[i]}] should place "
                f"after [{error_class_names[i + 1]}]."
                "\n\nRun 'cd $SPARK_HOME; bin/pyspark' and "
                "'from pyspark.errors.exceptions import _write_self; _write_self()' "
                "to automatically sort them.",
            )

    def test_error_classes_duplicated(self):
        # Test error classes is not duplicated
        def detect_duplication(pairs):
            error_classes_json = {}
            for name, message in pairs:
                self.assertTrue(name not in error_classes_json, f"Duplicate error class: {name}")
                error_classes_json[name] = message
            return error_classes_json

        json.loads(ERROR_CLASSES_JSON, object_pairs_hook=detect_duplication)

    def test_invalid_error_class(self):
        with self.assertRaisesRegex(ValueError, "Cannot find main error class"):
            PySparkValueError(errorClass="invalid", messageParameters={})


if __name__ == "__main__":
    import unittest
    from pyspark.errors.tests.test_errors import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
