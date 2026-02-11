import os
from sparktestsupport.utils import determine_dangling_python_tests

all_python_files = []

for root, dirs, files in os.walk("../"):
    if "venv" in root:
        continue
    for file in files:
        if file.endswith(".py"):
            all_python_files.append(os.path.join(root, file))

import sys

dangling_python_tests = determine_dangling_python_tests(all_python_files)
if dangling_python_tests:
    print(f"[error] Found the following dangling Python tests {', '.join(dangling_python_tests)}")
    print("[error] Please add the tests to the appropriate module.")
    sys.exit(1)