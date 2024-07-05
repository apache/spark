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

import os
import sys
import re
import glob

from sparktestsupport import SPARK_HOME

def main():
    nonmigrated_pattern = r'log(?:Info|Warning|Error)\((?:".*"\.format\(.*\)|s".*(?:\$|\+\s*[^\s"]).*"\))'
    excluded_file_patterns = [
        '[Tt]est',
        './sql/catalyst/src/main/scala/org/apache/spark/sql/catalyst/expressions/codegen/CodeGenerator.scala'
        './sql/hive-thriftserver/src/main/scala/org/apache/spark/sql/hive/thriftserver/SparkSQLCLIService.scala'
    ]

    nonmigrated_files = {}
    excluded_files = set()

    scala_files = glob.glob(os.path.join(SPARK_HOME, '**', '*.scala'), recursive=True)

    for file in scala_files:
        for exclude_pattern in excluded_file_patterns:
            if re.search(exclude_pattern, file):
                excluded_files.add(file)
                break

    for file in scala_files:
        if file not in excluded_files:
            with open(file, 'r') as f:
                lines = f.readlines()
                for line_number, line in enumerate(lines, start=1):
                    if re.search(nonmigrated_pattern, line):
                        if file not in nonmigrated_files:
                            nonmigrated_files[file] = []
                        nonmigrated_files[file].append((line_number, line))

    if not nonmigrated_files:
        print("Structured logging style check passed.")
        sys.exit(0)
    else:
        for file_path, issues in nonmigrated_files.items():
            print(f"[error] Structured logging style check failed for the file '{file}' at:")
            for line_number, code in issues:
                print(f"Line: {line_number} code: {code}")

        sys.exit(-1)


if __name__ == "__main__":
    main()
