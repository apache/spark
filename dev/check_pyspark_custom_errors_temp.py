import sys

sys.path.insert(0, "python")


def check_pyspark_custom_errors():
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

    # PySpark-specific errors
    pyspark_error_list = [error for error in dir(pyspark_errors) if not error.startswith("__")]
    # Including Spark Connect errors
    pyspark_error_list += [
        error for error in dir(pyspark_connect_errors) if not error.startswith("__")
    ]

    # Base path for search and paths to exclude
    base_path = "python/pyspark/sql"
    exclude_paths = ["python/pyspark/sql/tests", "python/pyspark/sql/connect/proto"]

    # Check all files and collect a list of errors
    all_errors = []
    for py_file in find_py_files(base_path, exclude_paths):
        errors_in_file = check_errors_in_file(py_file, pyspark_error_list)
        all_errors.extend(errors_in_file)

    # If errors are found, print the full list and exit the script with a failure status
    if all_errors:
        print("PySpark custom errors check found issues in the following files:")
        for error in all_errors:
            print(error)
        print("Please use PySpark custom errors defined in pyspark.errors.")
        sys.exit(1)


if __name__ == "__main__":
    check_pyspark_custom_errors()
