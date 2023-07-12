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

"""
Utility for refining error messages based on LLM.

Usage:
    python error_message_refiner.py <error_class> [--model_name=<version>]

Arguments:
    <error_class>           Required.
                            The name of the error class to refine the messages for.
                            The list of error classes is located in
                            `common/utils/src/main/resources/error/error-classes.json`.

Options:
    --model_name=<version> Optional.
                            The version of Chat GPT to use for refining the error messages.
                            If not provided, the default version("gpt-3.5-turbo") will be used.

Example usage:
    python error_message_refiner.py CANNOT_DECODE_URL --model_name=gpt-4

Description:
    This script refines error messages using the LLM based approach.
    It takes the name of the error class as a required argument and, optionally,
    allows specifying the version of Chat GPT to use for refining the messages.

    Options:
        --model_name: Specifies the version of Chat GPT.
                       If not provided, the default version("gpt-3.5-turbo") will be used.

    Note:
    - Ensure that the necessary dependencies are installed before running the script.
    - Ensure that the valid API key is entered in the `api-key.txt`.
    - The refined error messages will be displayed in the console output.
    - To use the gpt-4 model, you need to join the waitlist. Please refer to
      https://help.openai.com/en/articles/7102672-how-can-i-access-gpt-4 for more details.
"""

import argparse
import json
import openai
import re
import subprocess
import random
import os
from typing import Tuple, Optional

from sparktestsupport import SPARK_HOME

PATH_TO_ERROR_CLASS = f"{SPARK_HOME}/common/utils/src/main/resources/error/error-classes.json"
# Register your own open API key for the environment variable.
# The API key can be obtained from https://platform.openai.com/account/api-keys.
OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

openai.api_key = OPENAI_API_KEY


def _git_grep_files(search_string: str, exclude: str = None) -> str:
    """
    Executes 'git grep' command to search for files containing the given search string.
    Returns the file path where the search string is found.
    """
    result = subprocess.run(
        ["git", "grep", "-l", search_string, "--", f"{SPARK_HOME}/*.scala"],
        capture_output=True,
        text=True,
    )
    output = result.stdout.strip()

    files = output.split("\n")
    files = [file for file in files if "Suite" not in file]
    if exclude is not None:
        files = [file for file in files if exclude not in file]
    file = random.choice(files)
    return file


def _find_function(file_name: str, search_string: str) -> Optional[str]:
    """
    Searches for a function in the given file containing the specified search string.
    Returns the name of the function if found, otherwise None.
    """
    with open(file_name, "r") as file:
        content = file.read()
        functions = re.findall(r"def\s+(\w+)\s*\(", content)

        for function in functions:
            function_content = re.search(
                rf"def\s+{re.escape(function)}(?:(?!def).)*?{re.escape(search_string)}",
                content,
                re.DOTALL,
            )
            if function_content and search_string in function_content.group(0):
                return function

    return None


def _find_func_body(file_name: str, search_string: str) -> Optional[str]:
    """
    Searches for a function body in the given file containing the specified search string.
    Returns the function body if found, otherwise None.
    """
    with open(file_name, "r") as file:
        content = file.read()
        functions = re.findall(r"def\s+(\w+)\s*\(", content)

        for function in functions:
            function_content = re.search(
                rf"def\s+{re.escape(function)}(?:(?!def\s).)*?{re.escape(search_string)}",
                content,
                re.DOTALL,
            )
            if function_content and search_string in function_content.group(0):
                return function_content.group(0)

    return None


def _get_error_function(error_class: str) -> str:
    """
    Retrieves the name of the error function that triggers the given error class.
    """
    search_string = error_class
    matched_file = _git_grep_files(search_string)
    err_func = _find_function(matched_file, search_string)
    return err_func


def _get_source_code(error_class: str) -> str:
    """
    Retrieves the source code of a function where the given error class is being invoked.
    """
    search_string = error_class
    matched_file = _git_grep_files(search_string)
    err_func = _find_function(matched_file, search_string)
    source_file = _git_grep_files(err_func, exclude=matched_file)
    source_code = _find_func_body(source_file, err_func)
    return source_code


def _get_error_message(error_class: str) -> str:
    """
    Returns the error message from the provided error class
    listed in core/src/main/resources/error/error-classes.json.
    """
    with open(PATH_TO_ERROR_CLASS) as f:
        error_classes = json.load(f)

    error_message = error_classes.get(error_class)
    if error_message:
        return error_message["message"]
    else:
        return f"Error message not found for class: {error_class}"


def ask_chat_gpt(error_class: str, model_name: str) -> Tuple[str, str]:
    """
    Requests error message improvement from Chat GPT.
    Returns a tuple containing the old error message and the refined error message.
    """
    old_error_message = " ".join(_get_error_message(error_class))
    error_function = _get_error_function(error_class)
    source_code = _get_source_code(error_class)
    prompt = f"""I would like to improve the error messages in Apache Spark.
Apache Spark provides error message guidelines, so error messages generated by Apache Spark should follow the following rules:

1. Include What, Why, and How
Error messages should explicitly address What, Why, and How.
For example, the error message "Unable to generate an encoder for inner class <> without access to the scope that this class was defined in. Try moving this class out of its parent class." follows the guidelines as it includes What, Why, and How.
On the other hand, the error message "Unsupported function name <>" only includes What and does not provide Why and How, so it needs improvement to comply with the guidelines.

2. Use clear language
Error messages in should adhere to the Diction guide and Wording guide provided in the two tables below in Markdown format.

- Diction guide:

| Phrases                                                      | When to use                                                  | Example                                         |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ----------------------------------------------- |
| Unsupported                                                  | The user may reasonably assume that the operation is supported, but it is not. This error may go away in the future if developers add support for the operation. | `Data type <> is unsupported.`                  |
| Invalid / Not allowed / Unexpected                           | The user made a mistake when specifying an operation. The message should inform the user of how to resolve the error. | `Array has size <>, index <> is invalid.`       |
| `Found <> generators for the clause <>. Only one generator is allowed.` |                                                              |                                                 |
| `Found an unexpected state format version <>. Expected versions 1 or 2.` |                                                              |                                                 |
| Failed to                                                    | The system encountered an unexpected error that cannot be reasonably attributed to user error. | `Failed to compile <>.`                         |
| Cannot                                                       | Any time, preferably only if one of the above alternatives does not apply. | `Cannot generate code for unsupported type <>.` |

- Wording guide:

| Best practice                                                | Before                                                       | After                                                        |
| ------------------------------------------------------------ | ------------------------------------------------------------ | ------------------------------------------------------------ |
| Use active voice                                             | `DataType <> is not supported by <>.`                        | `<> does not support datatype <>.`                           |
| Avoid time-based statements, such as promises of future support | `Pandas UDF aggregate expressions are currently not supported in pivot.` | `Pivot does not support Pandas UDF aggregate expressions.`   |
| `Parquet type not yet supported: <>.`                        | `<> does not support Parquet type.`                          |                                                              |
| Use the present tense to describe the error and provide suggestions | `Couldn't find the reference column for <> at <>.`           | `Cannot find the reference column for <> at <>.`             |
| `Join strategy hint parameter should be an identifier or string but was <>.` | `Cannot use join strategy hint parameter <>. Use a table name or identifier to specify the parameter.` |                                                              |
| Provide concrete examples if the resolution is unclear       | `<> Hint expects a partition number as a parameter.`         | `<> Hint expects a partition number as a parameter. For example, specify 3 partitions with <>(3).` |
| Avoid sounding accusatory, judgmental, or insulting          | `You must specify an amount for <>.`                         | `<> cannot be empty. Specify an amount for <>.`              |
| Be direct                                                    | `LEGACY store assignment policy is disallowed in Spark data source V2. Please set the configuration spark.sql.storeAssignmentPolicy to other values.` | `Spark data source V2 does not allow the LEGACY store assignment policy. Set the configuration spark.sql.storeAssignment to ANSI or STRICT.` |
| Do not use programming jargon in user-facing errors          | `RENAME TABLE source and destination databases do not match: '<>' != '<>'.` | `RENAME TABLE source and destination databases do not match. The source database is <>, but the destination database is <>.` |

To adhere to the above guidelines, please improve the following error message: "{old_error_message}"
Please note that the angle brackets in the error message represent the placeholder for the error message parameter, so they should not be modified or removed.
For more detail, the error message is triggered through a error function named "{error_function}", and the source code that actually calls the error function is as follows:

{source_code}

When improving the error, please also refer to the source code provided above to provide more detailed context to users."""
    try:
        response = openai.ChatCompletion.create(
            model=model_name,
            messages=[
                {
                    "role": "system",
                    "content": "You are a engineer who want to improve the error messages "
                    "for Apache Spark users.",
                },
                {"role": "user", "content": prompt},
            ],
        )
    except openai.error.AuthenticationError:
        raise openai.error.AuthenticationError(
            "Please verify if the API key is set correctly in `OPENAI_API_KEY`."
        )
    except openai.error.InvalidRequestError as e:
        if "gpt-4" in str(e):
            raise openai.error.AuthenticationError(
                "To use the gpt-4 model, you need to join the waitlist. "
                "Please refer to "
                "https://help.openai.com/en/articles/7102672-how-can-i-access-gpt-4 "
                "for more details."
            )
        raise e
    result = ""
    for choice in response.choices:
        result += choice.message.content
    return old_error_message, result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("error_class", type=str)
    parser.add_argument("--model_name", type=str, default="gpt-3.5-turbo")

    args = parser.parse_args()

    old_error_message, new_error_message = ask_chat_gpt(args.error_class, args.model_name)
    print(f"[{args.error_class}]\nBefore: {old_error_message}\nAfter: {new_error_message}")
