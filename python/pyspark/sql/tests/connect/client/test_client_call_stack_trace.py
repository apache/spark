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

import os
import sys
import unittest
from unittest.mock import patch

import pyspark
from pyspark.sql import functions
from pyspark.testing.connectutils import should_test_connect, connect_requirement_message

if should_test_connect:
    import pyspark.sql.connect.proto as pb2
    from pyspark.sql.connect.client import SparkConnectClient, core
    from pyspark.sql.connect.client.core import (
        _is_pyspark_source,
        _retrieve_stack_frames,
        _build_call_stack_trace,
    )
    from pyspark.traceback_utils import CallSite
    from google.protobuf import any_pb2

    # The _cleanup_ml_cache invocation will hang in this test (no valid spark cluster)
    # and it blocks the test process exiting because it is registered as the atexit handler
    # in `SparkConnectClient` constructor. To bypass the issue, patch the method in the test.
    SparkConnectClient._cleanup_ml_cache = lambda _: None

# SPARK-54314: Improve Server-Side debuggability in Spark Connect by capturing client application's
# file name and line numbers in PySpark
# https://issues.apache.org/jira/browse/SPARK-54314


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class CallStackTraceTestCase(unittest.TestCase):
    """Test cases for call stack trace functionality in Spark Connect client."""

    def setUp(self):
        # Since this test itself is under pyspark module path, stack frames for test functions inside
        # this file - for example, user_function() - will normally be filtered out. So here we
        # set the PYSPARK_ROOT to more specific pyspaark.sql.connect that doesn't include this
        # test file to ensure that the stack frames for user functions inside this test file are
        # not filtered out.
        self.original_pyspark_root = core.PYSPARK_ROOT
        core.PYSPARK_ROOT = os.path.dirname(pyspark.sql.connect.__file__)

    def tearDown(self):
        # Restore the original PYSPARK_ROOT
        core.PYSPARK_ROOT = self.original_pyspark_root

    def test_is_pyspark_source_with_pyspark_file(self):
        """Test that _is_pyspark_source correctly identifies PySpark files."""
        # Get a known pyspark file path
        from pyspark import sql

        pyspark_file = sql.connect.client.__file__
        self.assertTrue(_is_pyspark_source(pyspark_file))

    def test_is_pyspark_source_with_non_pyspark_file(self):
        """Test that _is_pyspark_source correctly identifies non-PySpark files."""
        # Use the current test file which is in pyspark but we'll simulate a non-pyspark path
        non_pyspark_file = "/tmp/user_script.py"
        self.assertFalse(_is_pyspark_source(non_pyspark_file))

        # Test with stdlib file
        stdlib_file = os.__file__
        self.assertFalse(_is_pyspark_source(stdlib_file))

    def test_is_pyspark_source_with_relative_path(self):
        """Test _is_pyspark_source with various path formats."""
        from pyspark import sql

        # Test with absolute path to pyspark file
        pyspark_sql_file = sql.connect.client.__file__
        self.assertTrue(_is_pyspark_source(pyspark_sql_file))

        # Test with non-pyspark absolute path
        self.assertFalse(_is_pyspark_source("/home/user/my_script.py"))

    def test_retrieve_stack_frames_filters_pyspark_frames(self):
        """Test that _retrieve_stack_frames filters out PySpark internal frames."""

        def user_function():
            return _retrieve_stack_frames()

        stack_frames = user_function()

        # Verify we have at least some frames
        self.assertGreater(len(stack_frames), 0, "Expected at least some stack frames")

        # Verify that none of the returned frames are from PySpark internal code
        for frame in stack_frames:
            # Check that this frame is not from pyspark internal code
            self.assertFalse(
                _is_pyspark_source(frame.file),
                f"Expected frame from {frame.file} (function: {frame.function}) to be filtered out as PySpark internal frame",
            )

        # Verify that user function names are present (confirming user frames are included)
        function_names = [frame.function for frame in stack_frames]
        expected_functions = ["user_function", "test_retrieve_stack_frames_filters_pyspark_frames"]
        self.assertTrue(
            "user_function" in function_names,
            f"Expected user function names not found in: {function_names}",
        )
        self.assertTrue(
            "test_retrieve_stack_frames_filters_pyspark_frames" in function_names,
            f"Expected user function names not found in: {function_names}",
        )

    def test_retrieve_stack_frames_includes_user_frames(self):
        """Test that _retrieve_stack_frames includes user code frames."""

        def user_function():
            """Simulate a user function."""
            return _retrieve_stack_frames()

        def another_user_function():
            """Another level of user code."""
            return user_function()

        stack_frames = another_user_function()

        # We should have at least some frames from the test
        self.assertGreater(len(stack_frames), 0)

        # Check that we have frames with function names we expect
        function_names = [frame.function for frame in stack_frames]
        # At least one of our test functions should be in the stack
        self.assertTrue(
            "user_function" in function_names,
            f"Expected user function names not found in: {function_names}",
        )
        self.assertTrue(
            "another_user_function" in function_names,
            f"Expected user function names not found in: {function_names}",
        )
        self.assertTrue(
            "test_retrieve_stack_frames_includes_user_frames" in function_names,
            f"Expected user function names not found in: {function_names}",
        )

    def test_retrieve_stack_frames_captures_correct_info(self):
        """Test that _retrieve_stack_frames captures correct frame information."""

        def user_function():
            return _retrieve_stack_frames()

        stack_frames = user_function()

        # Verify each frame has the expected attributes
        functions = set()
        files = set()
        for frame in stack_frames:
            functions.add(frame.function)
            files.add(frame.file)
            self.assertIsNotNone(frame.function)
            self.assertIsNotNone(frame.file)
            self.assertIsNotNone(frame.linenum)
            self.assertIsInstance(frame.function, str)
            self.assertIsInstance(frame.file, str)
            self.assertIsInstance(frame.linenum, int)
            self.assertGreater(frame.linenum, 0)

        self.assertTrue(
            "user_function" in functions, f"Expected user function names not found in: {functions}"
        )
        self.assertTrue(
            "test_retrieve_stack_frames_captures_correct_info" in functions,
            f"Expected user function names not found in: {functions}",
        )
        self.assertTrue(__file__ in files, f"Expected user function names not found in: {files}")

    def test_build_call_stack_trace_without_env_var(self):
        """Test that _build_call_stack_trace returns empty list when env var is not set."""
        # Make sure the env var is not set
        with patch.dict(os.environ, {}, clear=False):
            if "SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK" in os.environ:
                del os.environ["SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK"]

            call_stack = _build_call_stack_trace()

            self.assertIsInstance(call_stack, list)
            self.assertEqual(len(call_stack), 0, "Expected empty list when env var is not set")

    def test_build_call_stack_trace_with_env_var_set(self):
        """Test that _build_call_stack_trace builds trace when env var is set."""
        # Set the env var to enable call stack tracing
        with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": "1"}):
            call_stack = _build_call_stack_trace()

            self.assertIsInstance(call_stack, list)
            # Should have at least one frame (this test function)
            self.assertGreater(len(call_stack), 0, "Expected non-empty list when env var is set")

            # Verify each element is an Any protobuf message
            functions = set()
            files = set()
            for stack_frame in call_stack:
                self.assertIsInstance(stack_frame, any_pb2.Any)

                # Unpack and verify it's a StackTraceElement
                stack_trace_element = pb2.FetchErrorDetailsResponse.StackTraceElement()
                stack_frame.Unpack(stack_trace_element)
                functions.add(stack_trace_element.method_name)
                files.add(stack_trace_element.file_name)

                # Verify the fields are populated
                self.assertIsInstance(stack_trace_element.method_name, str)
                self.assertIsInstance(stack_trace_element.file_name, str)
                self.assertIsInstance(stack_trace_element.line_number, int)
                self.assertEqual(
                    stack_trace_element.declaring_class, "", "declaring_class should be empty"
                )

            self.assertTrue(
                "test_build_call_stack_trace_with_env_var_set" in functions,
                f"Expected user function names not found in: {functions}",
            )
            self.assertTrue(
                __file__ in files, f"Expected user function names not found in: {files}"
            )

    def test_build_call_stack_trace_with_env_var_empty_string(self):
        """Test that _build_call_stack_trace returns empty list when env var is empty string."""
        with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": ""}):
            call_stack = _build_call_stack_trace()

            self.assertIsInstance(call_stack, list)
            self.assertEqual(len(call_stack), 0, "Expected empty list when env var is empty string")

    def test_build_call_stack_trace_with_various_env_var_values(self):
        """Test _build_call_stack_trace behavior with various env var values."""
        test_cases = [
            ("0", 0, "zero string should be treated as falsy"),
            ("false", 0, "non-empty string 'false' should be falsy"),
            ("TRUE", 1, "string 'TRUE' should be truthy"),
            ("true", 1, "string 'true' should be truthy"),
            ("1", 1, "string '1' should be truthy"),
            ("any_value", 0, "any non-empty string should be falsy"),
        ]

        for env_value, expected_behavior, message in test_cases:
            with self.subTest(env_value=env_value):
                with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": env_value}):
                    call_stack = _build_call_stack_trace()
                    if expected_behavior == 0:
                        self.assertEqual(len(call_stack), 0, message)
                    else:
                        self.assertGreater(len(call_stack), 0, message)


@unittest.skipIf(not should_test_connect, connect_requirement_message)
class CallStackTraceIntegrationTestCase(unittest.TestCase):
    """Integration tests for call stack trace in client request methods."""

    def setUp(self):
        """Set up test fixtures."""
        self.client = SparkConnectClient("sc://localhost:15002", use_reattachable_execute=False)
        # Since this test itself is under pyspark module path, stack frames for test functions inside
        # this file - for example, user_function() - will normally be filtered out. So here we
        # set the PYSPARK_ROOT to more specific pyspaark.sql.connect that doesn't include this
        # test file to ensure that the stack frames for user functions inside this test file are
        # not filtered out.
        self.original_pyspark_root = core.PYSPARK_ROOT
        core.PYSPARK_ROOT = os.path.dirname(pyspark.sql.connect.__file__)

    def tearDown(self):
        # Restore the original PYSPARK_ROOT
        core.PYSPARK_ROOT = self.original_pyspark_root

    def test_execute_plan_request_includes_call_stack_without_env_var(self):
        """Test that _execute_plan_request_with_metadata doesn't include call stack without env var."""
        with patch.dict(os.environ, {}, clear=False):
            if "SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK" in os.environ:
                del os.environ["SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK"]

            req = self.client._execute_plan_request_with_metadata()

            # Should have no extensions when env var is not set
            self.assertEqual(
                len(req.user_context.extensions),
                0,
                "Expected no extensions without env var",
            )

    def test_execute_plan_request_includes_call_stack_with_env_var(self):
        """Test that _execute_plan_request_with_metadata includes call stack with env var."""
        with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": "1"}):
            req = self.client._execute_plan_request_with_metadata()

            # Should have extensions when env var is set
            self.assertGreater(
                len(req.user_context.extensions),
                0,
                "Expected extensions with env var set",
            )

            # Verify each extension can be unpacked as StackTraceElement
            files = set()
            functions = set()
            for extension in req.user_context.extensions:
                stack_trace_element = pb2.FetchErrorDetailsResponse.StackTraceElement()
                extension.Unpack(stack_trace_element)
                functions.add(stack_trace_element.method_name)
                files.add(stack_trace_element.file_name)
                self.assertIsInstance(stack_trace_element.method_name, str)
                self.assertIsInstance(stack_trace_element.file_name, str)

            self.assertTrue(
                "test_execute_plan_request_includes_call_stack_with_env_var" in functions,
                f"Expected user function names not found in: {functions}",
            )
            self.assertTrue(
                __file__ in files, f"Expected user function names not found in: {files}"
            )

    def test_analyze_plan_request_includes_call_stack_without_env_var(self):
        """Test that _analyze_plan_request_with_metadata doesn't include call stack without env var."""
        with patch.dict(os.environ, {}, clear=False):
            if "SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK" in os.environ:
                del os.environ["SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK"]

            req = self.client._analyze_plan_request_with_metadata()

            # Should have no extensions when env var is not set
            self.assertEqual(
                len(req.user_context.extensions),
                0,
                "Expected no extensions without env var",
            )

    def test_analyze_plan_request_includes_call_stack_with_env_var(self):
        """Test that _analyze_plan_request_with_metadata includes call stack with env var."""
        with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": "1"}):
            req = self.client._analyze_plan_request_with_metadata()

            # Should have extensions when env var is set
            self.assertGreater(
                len(req.user_context.extensions),
                0,
                "Expected extensions with env var set",
            )

            # Verify each extension can be unpacked as StackTraceElement
            files = set()
            functions = set()
            for extension in req.user_context.extensions:
                stack_trace_element = pb2.FetchErrorDetailsResponse.StackTraceElement()
                extension.Unpack(stack_trace_element)
                functions.add(stack_trace_element.method_name)
                files.add(stack_trace_element.file_name)
                self.assertIsInstance(stack_trace_element.method_name, str)
                self.assertIsInstance(stack_trace_element.file_name, str)

            self.assertTrue(
                "test_analyze_plan_request_includes_call_stack_with_env_var" in functions,
                f"Expected user function names not found in: {functions}",
            )
            self.assertTrue(
                __file__ in files, f"Expected user function names not found in: {files}"
            )

    def test_config_request_includes_call_stack_without_env_var(self):
        """Test that _config_request_with_metadata doesn't include call stack without env var."""
        with patch.dict(os.environ, {}, clear=False):
            if "SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK" in os.environ:
                del os.environ["SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK"]

            req = self.client._config_request_with_metadata()

            # Should have no extensions when env var is not set
            self.assertEqual(
                len(req.user_context.extensions),
                0,
                "Expected no extensions without env var",
            )

    def test_config_request_includes_call_stack_with_env_var(self):
        """Test that _config_request_with_metadata includes call stack with env var."""
        with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": "1"}):
            req = self.client._config_request_with_metadata()

            # Should have extensions when env var is set
            self.assertGreater(
                len(req.user_context.extensions),
                0,
                "Expected extensions with env var set",
            )

            # Verify each extension can be unpacked as StackTraceElement
            files = set()
            functions = set()
            for extension in req.user_context.extensions:
                stack_trace_element = pb2.FetchErrorDetailsResponse.StackTraceElement()
                extension.Unpack(stack_trace_element)
                functions.add(stack_trace_element.method_name)
                files.add(stack_trace_element.file_name)
                self.assertIsInstance(stack_trace_element.method_name, str)
                self.assertIsInstance(stack_trace_element.file_name, str)

            self.assertTrue(
                "test_config_request_includes_call_stack_with_env_var" in functions,
                f"Expected user function names not found in: {functions}",
            )
            self.assertTrue(
                __file__ in files, f"Expected user function names not found in: {files}"
            )

    def test_call_stack_trace_captures_correct_calling_context(self):
        """Test that call stack trace captures the correct calling context."""

        def level3():
            """Third level function."""
            with patch.dict(os.environ, {"SPARK_CONNECT_DEBUG_CLIENT_CALL_STACK": "1"}):
                req = self.client._execute_plan_request_with_metadata()
                return req

        def level2():
            """Second level function."""
            return level3()

        def level1():
            """First level function."""
            return level2()

        req = level1()

        # Verify we captured frames from our nested functions
        self.assertGreater(len(req.user_context.extensions), 0)

        # Unpack and check that we have function names from our call chain
        functions = set()
        files = set()
        for extension in req.user_context.extensions:
            stack_trace_element = pb2.FetchErrorDetailsResponse.StackTraceElement()
            extension.Unpack(stack_trace_element)
            functions.add(stack_trace_element.method_name)
            files.add(stack_trace_element.file_name)
            self.assertGreater(
                stack_trace_element.line_number,
                0,
                f"Expected line number to be greater than 0, got: {stack_trace_element.line_number}",
            )

        self.assertTrue(
            "level1" in functions, f"Expected user function names not found in: {functions}"
        )
        self.assertTrue(
            "level2" in functions, f"Expected user function names not found in: {functions}"
        )
        self.assertTrue(
            "level3" in functions, f"Expected user function names not found in: {functions}"
        )
        self.assertTrue(__file__ in files, f"Expected user function names not found in: {files}")


if __name__ == "__main__":
    from pyspark.sql.tests.connect.client.test_client_call_stack_trace import *  # noqa: F401

    try:
        import xmlrunner  # type: ignore

        testRunner = xmlrunner.XMLTestRunner(output="target/test-reports", verbosity=2)
    except ImportError:
        testRunner = None
    unittest.main(testRunner=testRunner, verbosity=2)
