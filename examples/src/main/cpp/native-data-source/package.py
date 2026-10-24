#!/usr/bin/env python3
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements. See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License. You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Package the compiled range connector for the C ABI in Spark PR #59424."""

import argparse
import json
from pathlib import Path
import platform
import zipfile


def current_platform():
    os_name = {"Linux": "linux", "Darwin": "osx", "Windows": "windows"}.get(platform.system())
    arch = {
        "x86_64": "x86_64",
        "amd64": "x86_64",
        "aarch64": "aarch64",
        "arm64": "aarch64",
    }.get(platform.machine().lower())
    return f"{os_name}-{arch}" if os_name and arch else None


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", default="cpp_sdk_range", help="Registered data source name")
    parser.add_argument("--library", type=Path, required=True)
    parser.add_argument("--output", type=Path, required=True)
    parser.add_argument("--platform", default=current_platform(), help="Override for cross builds")
    args = parser.parse_args()
    if not args.library.is_file():
        parser.error(f"Library does not exist: {args.library}")
    if args.output.suffix != ".sparkpkg":
        parser.error("The output file must have the .sparkpkg extension")
    if args.platform not in {
        f"{os_name}-{arch}"
        for os_name in ("linux", "osx", "windows")
        for arch in ("x86_64", "aarch64")
    }:
        parser.error("Specify a supported --platform, such as linux-x86_64")
    if args.library.resolve() == args.output.resolve():
        parser.error("The output must differ from the input library")

    library_path = f"{args.platform}/{args.library.name}"
    manifest = {
        "abiVersion": 1,
        "dataSources": [args.source],
        "libraries": {args.platform: library_path},
    }
    args.output.parent.mkdir(parents=True, exist_ok=True)
    with zipfile.ZipFile(args.output, "w", compression=zipfile.ZIP_DEFLATED) as package:
        package.writestr("spark-native-datasource.json", json.dumps(manifest, indent=2) + "\n")
        package.write(args.library, library_path)
    print(args.output.resolve())


if __name__ == "__main__":
    main()
