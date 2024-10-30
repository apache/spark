import re

from pyspark.errors.error_classes import ERROR_CLASSES_MAP


def generate_errors_doc(output_rst_file_path: str) -> None:
    """
    Generates a reStructuredText (RST) documentation file for PySpark error classes.

    This function fetches error classes defined in `pyspark.errors.error_classes`
    and writes them into an RST file. The generated RST file provides an overview
    of common, named error classes returned by PySpark.

    Parameters
    ----------
    output_rst_file_path : str
        The file path where the RST documentation will be written.

    Notes
    -----
    The generated RST file can be rendered using Sphinx to visualize the documentation.
    """
    header = """..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

========================
Error classes in PySpark
========================

This is a list of common, named error classes returned by PySpark which are defined at `error-conditions.json <https://github.com/apache/spark/blob/master/python/pyspark/errors/error-conditions.json>`_.

When writing PySpark errors, developers must use an error class from the list. If an appropriate error class is not available, add a new one into the list. For more information, please refer to `Contributing Error and Exception <contributing.rst#contributing-error-and-exception>`_.
"""  # noqa
    with open(output_rst_file_path, "w") as f:
        f.write(header + "\n\n")
        for error_key, error_details in ERROR_CLASSES_MAP.items():
            f.write(error_key + "\n")
            # The length of the error class name and underline must be the same
            # to satisfy the RST format.
            f.write("-" * len(error_key) + "\n\n")
            messages = error_details["message"]
            for message in messages:
                # Escape parentheses with a backslash when they follow a backtick.
                message = re.sub(r"`(\()", r"`\\\1", message)
                f.write(message + "\n")
            # Add 2 new lines between the descriptions of each error class
            # to improve the readability of the generated RST file.
            f.write("\n\n")
