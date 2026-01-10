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


import argparse
import sys


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Dump threads of a process and its children")
    parser.add_argument("-p", "--pid", type=int, required=True, help="The PID to dump")
    return parser


def main() -> int:
    try:
        import psutil
        from pystack.__main__ import main as pystack_main  # type: ignore
    except ImportError:
        print("pystack and psutil are not installed")
        return 5

    parser = build_parser()
    args = parser.parse_args()

    try:
        pids = [args.pid] + [
            child.pid
            for child in psutil.Process(args.pid).children(recursive=True)
            if "python" in child.exe()
        ]
    except Exception as e:
        print(f"Error getting children of process {args.pid}: {e}")
        return 2

    for pid in pids:
        sys.argv = ["pystack", "remote", str(pid)]
        try:
            print(f"Dumping threads for process {pid}")
            pystack_main()
        except Exception:
            # We might tried to dump a process that is not a Python process
            pass

    return 0


if __name__ == "__main__":
    sys.exit(main())
