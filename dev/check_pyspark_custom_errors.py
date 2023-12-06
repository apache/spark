import sys

sys.path.insert(0, "python")
import os
from pyspark import errors as pyspark_errors
from pyspark.errors.exceptions import connect as pyspark_connect_errors


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
    """
    errors_found = []
    with open(file_path, "r") as file:
        for line_num, line in enumerate(file, start=1):
            if line.strip().startswith("raise"):
                parts = line.split()
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
    """
    all_errors = []
    for path in target_paths:
        for py_file in find_py_files(path, exclude_paths):
            file_errors = check_errors_in_file(py_file, pyspark_error_list)
            all_errors.extend(file_errors)
    return all_errors


if __name__ == "__main__":
    # PySpark-specific errors
    pyspark_error_list = [error for error in dir(pyspark_errors) if not error.startswith("__")]
    connect_error_list = [
        error for error in dir(pyspark_connect_errors) if not error.startswith("__")
    ]
    internal_error_list = ["RetryException", "StopIteration"]
    pyspark_error_list += connect_error_list
    pyspark_error_list += internal_error_list

    # Target paths and exclude paths
    TARGET_PATHS = ["python/pyspark/sql"]
    EXCLUDE_PATHS = [
        "python/pyspark/sql/tests",
        "python/pyspark/sql/connect/proto",
    ]

    # Check errors
    errors_found = check_pyspark_custom_errors(TARGET_PATHS, EXCLUDE_PATHS)
    if errors_found:
        print("\nPySpark custom errors check found issues in the following files:")
        for error in errors_found:
            print(error)
        print("\nPlease use PySpark custom errors defined in pyspark.errors.")
        sys.exit(1)
