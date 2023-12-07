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

from dataclasses import dataclass
from itertools import tee, filterfalse
from pathlib import Path
from typing import List

@dataclass
class TestCase:
    name: str
    input_file: str
    result_file: str

@dataclass
class ExecutionOutput:
    sql: str
    output: str

    def __str__(self):
        return f"-- !query\n{self.sql}\n-- !query output\n{self.output}"

def get_workspace_file_path():
    return Path.cwd()

def file_to_string(file_path, encoding='utf-8'):
    with open(file_path, 'rb') as file:
        content = file.read()
        return content.decode(encoding)

def split_comments_and_codes(input_string):
    # Split the input string into lines
    lines = input_string.split("\n")

    # Use tee to create two independent iterators over the lines
    lines1, lines2 = tee(lines)

    # Partition lines into comments and codes
    comments = filter(lambda line: line.strip().startswith("--") and not line.strip().startswith("--QUERY-DELIMITER"), lines1)
    codes = filterfalse(lambda line: line.strip().startswith("--") and not line.strip().startswith("--QUERY-DELIMITER"), lines2)

    return list(comments), list(codes)

def get_queries(code: List[str], comments: List[str], list_test_cases: List[TestCase]):
    def split_with_semicolon(seq):
        return "\n".join(seq).split(";")

    # If `--IMPORT` found, load code from another test case file, then insert them
    # into the head in this test.
    imported_test_case_names = [comment[9:] for comment in comments if comment.startswith("--IMPORT ")]
    imported_code = []
    for test_case_name in imported_test_case_names:
        test_case = next((tc for tc in list_test_cases if tc.name == test_case_name), None)
        if test_case:
            with open(test_case.input_file, 'r', encoding='utf-8') as file:
                input_content = file.read()
                _, imported_code_piece = split_comments_and_codes(input_content)
                imported_code.extend(imported_code_piece)

    all_code = imported_code + code
    if any(c.strip().startswith("--QUERY-DELIMITER") for c in all_code):
        queries = []
        other_codes = []
        temp_str = ""
        start = False
        for c in all_code:
            if c.strip().startswith("--QUERY-DELIMITER-START"):
                start = True
                queries.extend(split_with_semicolon(other_codes))
                other_codes.clear()
            elif c.strip().startswith("--QUERY-DELIMITER-END"):
                start = False
                queries.append(f"\n{temp_str.rstrip(';')}")
                temp_str = ""
            elif start:
                temp_str += f"\n{c}"
            else:
                other_codes.append(c)
        if other_codes:
            queries.extend(split_with_semicolon(other_codes))
        temp_queries = queries
    else:
        temp_queries = split_with_semicolon(all_code)

    # List of SQL queries to run
    striped_queries = [query.strip() for query in temp_queries]
    return [
        query
        for query in striped_queries
        if query and not query.startswith('--')
    ]

def format_postgres_output(output: List[str]) -> List[str]:
    NO_RESULT = 'no results to fetch'
    format_no_result_output = ['' if s == NO_RESULT else s for s in output]
    return "\n".join(["\t".join(row.split(",")) for row in format_no_result_output])
