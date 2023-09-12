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
# This file contains helper methods used in updating python api versions.json.

import json
import sys
from os import path


def update_docs_version(version_file_name, release_version):
    if path.isfile(version_file_name) is False:
        raise Exception(f"File {version_file_name} not found")

    versions = []
    with open(version_file_name) as version_file:
        versions = json.load(version_file)

    if ({"name": release_version, "version": release_version}) not in versions:
        versions.insert(0, {"name": release_version, "version": release_version})
        with open(version_file_name, "w") as version_file:
            json.dump(versions, version_file, indent=4, separators=(",", ": "))
            version_file.write("\n")

    print(f"Successfully update version {release_version} to the file: {version_file_name}")


if __name__ == "__main__":
    version_file_name = sys.argv[1]
    release_version = "latest"
    if len(sys.argv) == 3:
        release_version = sys.argv[2]

    print(f"Will update version {release_version} to the file: {version_file_name}")
    update_docs_version(version_file_name, release_version)
