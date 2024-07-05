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
    nonmigrated_pattern = r'log(Info|Warning|Error)\((?:".*"\.format\(.*\)|s".*(?:\$|\+\s*[^\s"]).*"\))'
    whitelist_file = 'dev/structured-logging-whitelist.txt'

    nonmigrated_files = []
    whitelist_files = set()

    with open(whitelist_file, 'r') as wf:
        whitelist_patterns = [line.strip() for line in wf if line.strip()]

    scala_files = glob.glob(os.path.join(SPARK_HOME, '**', '*.scala'), recursive=True)

    for file in scala_files:
        for whitelist_pattern in whitelist_patterns:
            if re.search(whitelist_pattern, file):
                whitelist_files.add(file)
                break

    for file in scala_files:
        if file not in whitelist_files:
            with open(file, 'r') as f:
                content = f.read().replace('\t', '').strip()
                if re.search(nonmigrated_pattern, content):
                    nonmigrated_files.append(file)

    if not nonmigrated_files:
        print("Structured logging style check passed.")
        sys.exit(0)
    else:
        print("Structured logging style check failed for the following:")
        for file in nonmigrated_files:
            print(file)

        sys.exit(-1)


if __name__ == "__main__":
    main()
