#!/usr/bin/env python3

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

import ast
import dataclasses
import functools
import json
import sys
import textwrap

sys.path.insert(0, "python")
import os


@dataclasses.dataclass
class CustomErrorFailure:
    file_path: str
    line_number: int
    source_code: str
    error_message: str

    def __str__(self):
        return f"{self.file_path}:({self.line_number}):\n{textwrap.dedent(self.source_code)}{self.error_message}"


@dataclasses.dataclass
class CustomErrorAnalyzer:
    def __init__(self, file_path):
        self.file_path = file_path
        self.file_content = None
        self.pyspark_error_list = self._load_error_list()
        self.error_conditions = self._load_error_conditions()

    @staticmethod
    @functools.cache
    def _load_error_conditions():
        with open("python/pyspark/errors/error-conditions.json", "r") as file:
            return json.load(file)

    @staticmethod
    @functools.cache
    def _load_error_list():
        # PySpark-specific errors
        pyspark_error_list = [
            "AnalysisException",
            "ArithmeticException",
            "ArrayIndexOutOfBoundsException",
            "DateTimeException",
            "IllegalArgumentException",
            "NumberFormatException",
            "ParseException",
            "PySparkAssertionError",
            "PySparkAttributeError",
            "PySparkException",
            "PySparkImportError",
            "PySparkIndexError",
            "PySparkKeyError",
            "PySparkNotImplementedError",
            "PySparkPicklingError",
            "PySparkRuntimeError",
            "PySparkTypeError",
            "PySparkValueError",
            "PythonException",
            "QueryExecutionException",
            "SessionNotSameException",
            "SparkNoSuchElementException",
            "SparkRuntimeException",
            "SparkUpgradeException",
            "StreamingQueryException",
            "TempTableAlreadyExistsException",
            "UnknownException",
            "UnsupportedOperationException",
        ]
        connect_error_list = [
            "AnalysisException",
            "ArithmeticException",
            "ArrayIndexOutOfBoundsException",
            "BaseAnalysisException",
            "BaseArithmeticException",
            "BaseArrayIndexOutOfBoundsException",
            "BaseDateTimeException",
            "BaseIllegalArgumentException",
            "BaseNoSuchElementException",
            "BaseNumberFormatException",
            "BaseParseException",
            "BasePythonException",
            "BaseQueryExecutionException",
            "BaseSparkRuntimeException",
            "BaseSparkUpgradeException",
            "BaseStreamingQueryException",
            "BaseUnsupportedOperationException",
            "DateTimeException",
            "IllegalArgumentException",
            "NumberFormatException",
            "ParseException",
            "PySparkException",
            "PythonException",
            "QueryExecutionException",
            "SparkConnectException",
            "SparkConnectGrpcException",
            "SparkException",
            "SparkNoSuchElementException",
            "SparkRuntimeException",
            "SparkUpgradeException",
            "StreamingQueryException",
            "UnsupportedOperationException",
        ]
        internal_error_list = ["RetryException", "StopIteration"]
        pyspark_error_list += connect_error_list
        pyspark_error_list += internal_error_list
        return pyspark_error_list

    def analyze(self):
        with open(self.file_path, "r") as file:
            self.file_lines = file.readlines()
            tree = ast.parse("".join(self.file_lines))

        failures = []

        for node in ast.walk(tree):
            if isinstance(node, ast.Raise):
                if isinstance(node.exc, ast.Call):
                    if failure := self.analyze_call(node.exc):
                        failures.append(failure)
        return failures

    def analyze_call(self, call_node):
        if not isinstance(call_node.func, ast.Name):
            return None

        exc_type = call_node.func.id
        if exc_type[0].isupper() and exc_type not in self.pyspark_error_list:
            return CustomErrorFailure(
                file_path=self.file_path,
                line_number=call_node.lineno,
                source_code=self.get_source(call_node),
                error_message=f"custom error '{exc_type}' is not defined in pyspark.errors",
            )

        keywords = list(call_node.keywords)
        # We need to parse errorClass first
        keywords.sort(key=lambda x: x.arg)
        for keyword in keywords:
            if keyword.arg == "errorClass":
                if isinstance(keyword.value, ast.Constant):
                    error_class = keyword.value.value
                    if error_class not in self.error_conditions:
                        return CustomErrorFailure(
                            file_path=self.file_path,
                            line_number=call_node.lineno,
                            source_code=self.get_source(call_node),
                            error_message=f"errorClass '{error_class}' is not defined in error-conditions.json",
                        )
                else:
                    error_class = None
                    continue
            elif keyword.arg == "messageParameters" and error_class is not None:
                if isinstance(keyword.value, ast.Dict):
                    for key_node in keyword.value.keys:
                        if isinstance(key_node, ast.Constant):
                            key = key_node.value
                            if f"<{key}>" not in self.error_conditions[error_class]["message"][0]:
                                return CustomErrorFailure(
                                    file_path=self.file_path,
                                    line_number=call_node.lineno,
                                    source_code=self.get_source(call_node),
                                    error_message=f"messageParameter '{key}' is not defined in {error_class}",
                                )
        return None

    def get_source(self, node):
        # lineno is 1-indexed
        return "".join(self.file_lines[node.lineno - 1 : node.end_lineno])


def find_py_files(path, exclude_paths):
    """
    Find all .py files in a directory, excluding files in specified subdirectories.

    Parameters
    ----------
    path : str
        The base directory to search for .py files.
    exclude_paths : list of str
        A list of subdirectories to exclude from the search.

    Returns
    -------
    list of str
        A list of paths to .py files.
    """
    py_files = []
    for root, dirs, files in os.walk(path):
        if any(exclude_path in root for exclude_path in exclude_paths):
            continue
        for file in files:
            if file.endswith(".py"):
                py_files.append(os.path.join(root, file))
    return py_files


def check_errors_in_file(file_path):
    """
    Check if a file uses PySpark-specific errors correctly.

    Parameters
    ----------
    file_path : str
        Path to the file to check.
    pyspark_error_list : list of str
        List of PySpark-specific error names.

    Returns
    -------
    list of str
        A list of strings describing the errors found in the file, with line numbers.
    """
    analyzer = CustomErrorAnalyzer(file_path)
    return analyzer.analyze()


def check_pyspark_custom_errors(target_paths, exclude_paths):
    """
    Check PySpark-specific errors in multiple paths.

    Parameters
    ----------
    target_paths : list of str
        List of paths to check for PySpark-specific errors.
    exclude_paths : list of str
        List of paths to exclude from the check.

    Returns
    -------
    list of str
        A list of strings describing the errors found, with file paths and line numbers.
    """
    all_errors = []
    for path in target_paths:
        for py_file in find_py_files(path, exclude_paths):
            file_errors = check_errors_in_file(py_file)
            all_errors.extend(file_errors)
    return all_errors


if __name__ == "__main__":
    # Target paths and exclude paths
    TARGET_PATHS = ["python/pyspark/sql"]
    EXCLUDE_PATHS = [
        "python/pyspark/sql/tests",
        "python/pyspark/sql/connect/resource",
        "python/pyspark/sql/connect/proto",
    ]

    # Check errors
    errors_found = check_pyspark_custom_errors(TARGET_PATHS, EXCLUDE_PATHS)
    if errors_found:
        print("\nPySpark custom errors check found issues:", file=sys.stderr)
        for error in errors_found:
            print(error, file=sys.stderr, end="\n\n")
        sys.exit(1)
