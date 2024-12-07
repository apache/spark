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

import unittest
from pyspark.errors.exceptions.connect import (
    convert_exception,
    EXCEPTION_CLASS_MAPPING,
    SparkConnectGrpcException,
    PythonException,
    AnalysisException,
)
from pyspark.sql.connect.proto import FetchErrorDetailsResponse as pb2
from google.rpc.error_details_pb2 import ErrorInfo


class ConnectErrorsTest(unittest.TestCase):
    def test_convert_exception_known_class(self):
        # Mock ErrorInfo with a known error class
        info = {
            "reason": "org.apache.spark.sql.AnalysisException",
            "metadata": {
                "classes": '["org.apache.spark.sql.AnalysisException"]',
                "sqlState": "42000",
                "errorClass": "ANALYSIS.ERROR",
                "messageParameters": '{"param1": "value1"}',
            },
        }
        truncated_message = "Analysis error occurred"
        exception = convert_exception(
            info=ErrorInfo(**info), truncated_message=truncated_message, resp=None
        )

        self.assertIsInstance(exception, AnalysisException)
        self.assertEqual(exception.getSqlState(), "42000")
        self.assertEqual(exception._errorClass, "ANALYSIS.ERROR")
        self.assertEqual(exception._messageParameters, {"param1": "value1"})

    def test_convert_exception_python_exception(self):
        # Mock ErrorInfo for PythonException
        info = {
            "reason": "org.apache.spark.api.python.PythonException",
            "metadata": {
                "classes": '["org.apache.spark.api.python.PythonException"]',
            },
        }
        truncated_message = "Python worker error occurred"
        exception = convert_exception(
            info=ErrorInfo(**info), truncated_message=truncated_message, resp=None
        )

        self.assertIsInstance(exception, PythonException)
        self.assertIn("An exception was thrown from the Python worker", exception.getMessage())

    def test_convert_exception_unknown_class(self):
        # Mock ErrorInfo with an unknown error class
        info = {
            "reason": "org.apache.spark.UnknownException",
            "metadata": {"classes": '["org.apache.spark.UnknownException"]'},
        }
        truncated_message = "Unknown error occurred"
        exception = convert_exception(
            info=ErrorInfo(**info), truncated_message=truncated_message, resp=None
        )

        self.assertIsInstance(exception, SparkConnectGrpcException)
        self.assertEqual(
            exception.getMessage(), "(org.apache.spark.UnknownException) Unknown error occurred"
        )

    def test_exception_class_mapping(self):
        # Ensure that all keys in EXCEPTION_CLASS_MAPPING are valid
        for error_class_name, exception_class in EXCEPTION_CLASS_MAPPING.items():
            self.assertTrue(
                hasattr(exception_class, "__name__"),
                f"{exception_class} in EXCEPTION_CLASS_MAPPING is not a valid class",
            )

    def test_convert_exception_with_stacktrace(self):
        # Mock FetchErrorDetailsResponse with stacktrace
        resp = pb2(
            root_error_idx=0,
            errors=[
                pb2.Error(
                    message="Root error message",
                    error_type_hierarchy=["org.apache.spark.SparkException"],
                    stack_trace=[
                        pb2.StackTraceElement(
                            declaring_class="org.apache.spark.Main",
                            method_name="main",
                            file_name="Main.scala",
                            line_number=42,
                        ),
                    ],
                    cause_idx=1,
                ),
                pb2.Error(
                    message="Cause error message",
                    error_type_hierarchy=["java.lang.RuntimeException"],
                    stack_trace=[
                        pb2.StackTraceElement(
                            declaring_class="org.apache.utils.Helper",
                            method_name="help",
                            file_name="Helper.java",
                            line_number=10,
                        ),
                    ],
                ),
            ],
        )

        info = {
            "reason": "org.apache.spark.SparkException",
            "metadata": {
                "classes": '["org.apache.spark.SparkException"]',
                "sqlState": "42000",
            },
        }
        truncated_message = "Root error message"
        exception = convert_exception(
            info=ErrorInfo(**info), truncated_message=truncated_message, resp=resp
        )

        self.assertIsInstance(exception, SparkConnectGrpcException)
        self.assertIn("Root error message", exception.getMessage())
        self.assertIn("Caused by", exception.getMessage())

    def test_convert_exception_fallback(self):
        # Mock ErrorInfo with missing class information
        info = {
            "reason": "org.apache.spark.UnknownReason",
            "metadata": {},
        }
        truncated_message = "Fallback error occurred"
        exception = convert_exception(
            info=ErrorInfo(**info), truncated_message=truncated_message, resp=None
        )

        self.assertIsInstance(exception, SparkConnectGrpcException)
        self.assertEqual(
            exception.getMessage(), "(org.apache.spark.UnknownReason) Fallback error occurred"
        )


if __name__ == "__main__":
    import unittest
    from pyspark.errors.tests.test_errors import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
