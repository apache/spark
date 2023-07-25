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

import argparse
import openai
import os

OPENAI_API_KEY = os.environ.get("OPENAI_API_KEY")

openai.api_key = OPENAI_API_KEY


def ask_gpt(pyspark_api: str, model_name: str = "gpt-3.5-turbo"):
    """
    Requests test generation from GPT API.
    """
    prompt = rf"""
    You are an expert Spark programmer who is generating a test suite for PySpark public APIs, to 
    improve test coverage.

    We will test the PySpark {pyspark_api} API. Write a suite of test cases, in Python, that cover 
    all general cases as well as all possible edge cases. 
    
    Below are some edge cases to consider, in terms of arguments passed into the function.
    Make sure to have a separate test case for each edge case.
    Each separate test should cover only ONE case.
    In total, you should have over 20 tests written.

    Edge case examples:
    Data Inputs:
    1. Empty input
    2. None type
    3. Negatives
    4. Integer 0
    5. value > Int.MaxValue
    6. value < Int.MinValue
    7. Negatives
    8. Float 0.0
    9. Float(“nan”)
    10. Float("inf")
    11. Float("-inf")
    12. decimal.Decimal
    13. numpy.float16
    14. String with special characters
    15. String of only spaces
    16. Empty strings
    DataFrame inputs:
    17. Non-existent column
    18. Empty column name
    19. Column name with special characters, e.g. dots
    20. Multi columns with the same name
    21. Nested column vs. quoted column name, e.g. ‘a.b.c’ vs ‘`a.b.c`’
    22. Column of special types, e.g. nested type;
    23. Column containing special values, e.g. Null;
    24. Empty input; e.g DataFrame.drop()
    25. Special cases for each single column
    26. Mix column with column names; e.g. DataFrame.drop(“col1”, df.col2, “col3”)
    27. Duplicated columns; e.g. DataFrame.drop(“col1”, col(“col1”))
    28. DataFrame argument
    29. Empty dataframe; e.g. spark.range(5).limit(0)
    30. Dataframe with 0 columns, e.g. spark.range(5).drop('id')
    31. Dataset with repeated arguments
    32. Local dataset (pd.DataFrame) containing unsupported datatype
    """
    try:
        response = openai.ChatCompletion.create(
            model=model_name,
            messages=[
                {
                    "role": "system",
                    "content": "You are a engineer who want to improve test coverage "
                    "for Apache Spark PySpark API functions.",
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
    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("pyspark_api", type=str)
    parser.add_argument("--model_name", type=str, default="gpt-3.5-turbo")

    args = parser.parse_args()

    result = ask_gpt(args.pyspark_api)
    print(f"Result: {result}")
