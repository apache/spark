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

import sys

sys.path.insert(0, "python")
import os


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


def check_errors_in_file(file_path, pyspark_error_list):
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
    errors_found = []
    with open(file_path, "r") as file:
        for line_num, line in enumerate(file, start=1):
            if line.strip().startswith("raise"):
                parts = line.split()
                # Check for 'raise' statement and ensure the error raised is a capitalized word.
                if len(parts) > 1 and parts[1][0].isupper():
                    if not any(pyspark_error in line for pyspark_error in pyspark_error_list):
                        errors_found.append(f"{file_path}:{line_num}: {line.strip()}")
    return errors_found


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
            file_errors = check_errors_in_file(py_file, pyspark_error_list)
            all_errors.extend(file_errors)
    return all_errors


if __name__ == "__main__":
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
        "RetriesExceeded",
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
        print("\nPySpark custom errors check found issues in the following files:", file=sys.stderr)
        for error in errors_found:
            print(error, file=sys.stderr)
        print("\nUse existing or create a new custom error from pyspark.errors.", file=sys.stderr)
        sys.exit(1)
