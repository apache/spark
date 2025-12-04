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

from pyspark.testing.utils import should_test_connect, connect_requirement_message

if should_test_connect:
    from pyspark.errors.exceptions.connect import (
        convert_exception,
        EXCEPTION_CLASS_MAPPING,
        SparkConnectGrpcException,
        PythonException,
        AnalysisException,
    )


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class ConnectErrorsTest(unittest.TestCase):
    def test_convert_exception_known_class(self):
        # Mock ErrorInfo with a known error class
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

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
            info=ErrorInfo(**info),
            truncated_message=truncated_message,
            resp=None,
            grpc_status_code=StatusCode.INTERNAL,
        )

        self.assertIsInstance(exception, AnalysisException)
        self.assertEqual(exception.getSqlState(), "42000")
        self.assertEqual(exception._errorClass, "ANALYSIS.ERROR")
        self.assertEqual(exception._messageParameters, {"param1": "value1"})
        self.assertEqual(exception.getGrpcStatusCode(), StatusCode.INTERNAL)

    def test_convert_exception_python_exception(self):
        # Mock ErrorInfo for PythonException
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

        info = {
            "reason": "org.apache.spark.api.python.PythonException",
            "metadata": {
                "classes": '["org.apache.spark.api.python.PythonException"]',
            },
        }
        truncated_message = "Python worker error occurred"
        exception = convert_exception(
            info=ErrorInfo(**info),
            truncated_message=truncated_message,
            resp=None,
            grpc_status_code=StatusCode.INTERNAL,
        )

        self.assertIsInstance(exception, PythonException)
        self.assertIn("An exception was thrown from the Python worker", exception.getMessage())
        self.assertEqual(exception.getGrpcStatusCode(), StatusCode.INTERNAL)

    def test_convert_exception_unknown_class(self):
        # Mock ErrorInfo with an unknown error class
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

        info = {
            "reason": "org.apache.spark.UnknownException",
            "metadata": {"classes": '["org.apache.spark.UnknownException"]'},
        }
        truncated_message = "Unknown error occurred"
        exception = convert_exception(
            info=ErrorInfo(**info),
            truncated_message=truncated_message,
            resp=None,
            grpc_status_code=StatusCode.INTERNAL,
        )

        self.assertIsInstance(exception, SparkConnectGrpcException)
        self.assertEqual(
            exception.getMessage(), "(org.apache.spark.UnknownException) Unknown error occurred"
        )
        self.assertEqual(exception.getGrpcStatusCode(), StatusCode.INTERNAL)

    def test_exception_class_mapping(self):
        # Ensure that all keys in EXCEPTION_CLASS_MAPPING are valid
        for error_class_name, exception_class in EXCEPTION_CLASS_MAPPING.items():
            self.assertTrue(
                hasattr(exception_class, "__name__"),
                f"{exception_class} in EXCEPTION_CLASS_MAPPING is not a valid class",
            )

    def test_convert_exception_with_stacktrace(self):
        # Mock FetchErrorDetailsResponse with stacktrace
        from google.rpc.error_details_pb2 import ErrorInfo
        from pyspark.sql.connect.proto import FetchErrorDetailsResponse as pb2

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
            info=ErrorInfo(**info),
            truncated_message=truncated_message,
            resp=resp,
            display_server_stacktrace=True,
        )

        self.assertIsInstance(exception, SparkConnectGrpcException)
        self.assertIn("Root error message", exception.getMessage())
        self.assertIn("Caused by", exception.getMessage())

    def test_convert_exception_fallback(self):
        # Mock ErrorInfo with missing class information
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

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
        self.assertEqual(exception.getGrpcStatusCode(), StatusCode.UNKNOWN)

    def test_convert_exception_with_breaking_change_info(self):
        """Test that breaking change info is correctly extracted from protobuf response."""
        import pyspark.sql.connect.proto as pb2
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

        # Create mock FetchErrorDetailsResponse with breaking change info
        resp = pb2.FetchErrorDetailsResponse()
        resp.root_error_idx = 0

        error = resp.errors.add()
        error.message = "Test error with breaking change"
        error.error_type_hierarchy.append("org.apache.spark.SparkException")

        # Add SparkThrowable with breaking change info
        spark_throwable = error.spark_throwable
        spark_throwable.error_class = "TEST_BREAKING_CHANGE_ERROR"

        # Add breaking change info
        bci = spark_throwable.breaking_change_info
        bci.migration_message.append("Please update your code to use new API")
        bci.migration_message.append("See documentation for details")
        bci.needs_audit = False

        # Add mitigation config
        mitigation_config = bci.mitigation_config
        mitigation_config.key = "spark.sql.legacy.behavior.enabled"
        mitigation_config.value = "true"

        info = ErrorInfo()
        info.reason = "org.apache.spark.SparkException"
        info.metadata["classes"] = '["org.apache.spark.SparkException"]'

        exception = convert_exception(
            info=info,
            truncated_message="Test error",
            resp=resp,
            grpc_status_code=StatusCode.INTERNAL,
        )

        # Verify breaking change info is correctly extracted
        breaking_change_info = exception.getBreakingChangeInfo()
        self.assertIsNotNone(breaking_change_info)
        self.assertEqual(
            breaking_change_info["migration_message"],
            ["Please update your code to use new API", "See documentation for details"],
        )
        self.assertEqual(breaking_change_info["needs_audit"], False)
        self.assertIn("mitigation_config", breaking_change_info)
        self.assertEqual(
            breaking_change_info["mitigation_config"]["key"],
            "spark.sql.legacy.behavior.enabled",
        )
        self.assertEqual(breaking_change_info["mitigation_config"]["value"], "true")

    def test_convert_exception_without_breaking_change_info(self):
        """Test that getBreakingChangeInfo returns None when no breaking change info."""
        import pyspark.sql.connect.proto as pb2
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

        # Create mock FetchErrorDetailsResponse without breaking change info
        resp = pb2.FetchErrorDetailsResponse()
        resp.root_error_idx = 0

        error = resp.errors.add()
        error.message = "Test error without breaking change"
        error.error_type_hierarchy.append("org.apache.spark.SparkException")

        # Add SparkThrowable without breaking change info
        spark_throwable = error.spark_throwable
        spark_throwable.error_class = "REGULAR_ERROR"

        info = ErrorInfo()
        info.reason = "org.apache.spark.SparkException"
        info.metadata["classes"] = '["org.apache.spark.SparkException"]'

        exception = convert_exception(
            info=info,
            truncated_message="Test error",
            resp=resp,
            grpc_status_code=StatusCode.INTERNAL,
        )

        # Verify breaking change info is None
        breaking_change_info = exception.getBreakingChangeInfo()
        self.assertIsNone(breaking_change_info)

    def test_breaking_change_info_storage_in_exception(self):
        """Test SparkConnectGrpcException correctly stores and retrieves breaking change info."""
        from pyspark.errors.exceptions.connect import SparkConnectGrpcException

        breaking_change_info = {
            "migration_message": ["Test migration message"],
            "mitigation_config": {"key": "test.config.key", "value": "test.config.value"},
            "needs_audit": True,
        }

        exception = SparkConnectGrpcException(
            message="Test error", errorClass="TEST_ERROR", breaking_change_info=breaking_change_info
        )

        stored_info = exception.getBreakingChangeInfo()
        self.assertEqual(stored_info, breaking_change_info)

    def test_breaking_change_info_inheritance(self):
        """Test that subclasses of SparkConnectGrpcException
        correctly inherit breaking change info."""
        from pyspark.errors.exceptions.connect import AnalysisException, UnknownException

        breaking_change_info = {
            "migration_message": ["Inheritance test message"],
            "needs_audit": False,
        }

        # Test AnalysisException
        analysis_exception = AnalysisException(
            message="Analysis error with breaking change",
            errorClass="TEST_ANALYSIS_ERROR",
            breaking_change_info=breaking_change_info,
        )

        stored_info = analysis_exception.getBreakingChangeInfo()
        self.assertEqual(stored_info, breaking_change_info)

        # Test UnknownException
        unknown_exception = UnknownException(
            message="Unknown error with breaking change",
            errorClass="TEST_UNKNOWN_ERROR",
            breaking_change_info=breaking_change_info,
        )

        stored_info = unknown_exception.getBreakingChangeInfo()
        self.assertEqual(stored_info, breaking_change_info)

    def test_breaking_change_info_without_mitigation_config(self):
        """Test breaking change info that only has migration messages."""
        import pyspark.sql.connect.proto as pb2
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

        # Create mock FetchErrorDetailsResponse with breaking change info (no mitigation config)
        resp = pb2.FetchErrorDetailsResponse()
        resp.root_error_idx = 0

        error = resp.errors.add()
        error.message = "Test error with breaking change"
        error.error_type_hierarchy.append("org.apache.spark.SparkException")

        # Add SparkThrowable with breaking change info
        spark_throwable = error.spark_throwable
        spark_throwable.error_class = "TEST_BREAKING_CHANGE_ERROR"

        # Add breaking change info without mitigation config
        bci = spark_throwable.breaking_change_info
        bci.migration_message.append("Migration message only")
        bci.needs_audit = True

        info = ErrorInfo()
        info.reason = "org.apache.spark.SparkException"
        info.metadata["classes"] = '["org.apache.spark.SparkException"]'

        exception = convert_exception(
            info=info,
            truncated_message="Test error",
            resp=resp,
            grpc_status_code=StatusCode.INTERNAL,
        )

        # Verify breaking change info is correctly extracted
        breaking_change_info = exception.getBreakingChangeInfo()
        self.assertIsNotNone(breaking_change_info)
        self.assertEqual(breaking_change_info["migration_message"], ["Migration message only"])
        self.assertEqual(breaking_change_info["needs_audit"], True)
        self.assertNotIn("mitigation_config", breaking_change_info)

    def test_convert_exception_error_class_from_fetch_error_details(self):
        """Test that errorClass is extracted from FetchErrorDetailsResponse
        when not present in ErrorInfo metadata (e.g., when messageParameters exceed limit)."""
        import pyspark.sql.connect.proto as pb2
        from google.rpc.error_details_pb2 import ErrorInfo
        from grpc import StatusCode

        # Create mock FetchErrorDetailsResponse with errorClass
        resp = pb2.FetchErrorDetailsResponse()
        resp.root_error_idx = 0

        error = resp.errors.add()
        error.message = "Test error"
        error.error_type_hierarchy.append("org.apache.spark.sql.AnalysisException")

        # Add SparkThrowable with errorClass and messageParameters
        spark_throwable = error.spark_throwable
        spark_throwable.error_class = "TEST_ERROR_CLASS_FROM_RESPONSE"
        spark_throwable.message_parameters["param1"] = "value1"
        spark_throwable.message_parameters["param2"] = "value2"

        # Create ErrorInfo WITHOUT errorClass in metadata
        # (simulating the case where messageParameters exceeded maxMetadataSize)
        info = ErrorInfo()
        info.reason = "org.apache.spark.sql.AnalysisException"
        info.metadata["classes"] = '["org.apache.spark.sql.AnalysisException"]'

        exception = convert_exception(
            info=info,
            truncated_message="Test error",
            resp=resp,
            grpc_status_code=StatusCode.INTERNAL,
        )

        # Verify errorClass was extracted from FetchErrorDetailsResponse
        self.assertIsInstance(exception, AnalysisException)
        self.assertEqual(exception._errorClass, "TEST_ERROR_CLASS_FROM_RESPONSE")
        # Verify messageParameters were also extracted from FetchErrorDetailsResponse
        self.assertEqual(exception._messageParameters, {"param1": "value1", "param2": "value2"})


if __name__ == "__main__":
    import unittest
    from pyspark.errors.tests.test_connect_errors_conversion import *  # noqa: F401

    try:
        import xmlrunner

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
