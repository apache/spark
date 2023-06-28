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
# Original repository: https://github.com/StardustDL/aexpy
# Copyright 2022 StardustDL <stardustdl@163.com>
#

# Usage: compare current code base with a commit or branch
# python dev/python-detect-breaking-changes.py a540995345bc935db3b8ccf5cb94cb7caabc4847 (branch name or commit id)
#
# Cache dir (detailed result and logs): dev/aexpy/cache

from io import TextIOWrapper
from pathlib import Path
import os
import shutil
from contextlib import contextmanager
import subprocess
import sys
import json
import traceback

PYTHON_EXE = sys.executable
AEXPY_EXE = [PYTHON_EXE, "-m", "aexpy", "-vvv"]
PYSPARK_RELPATH = "python"
PYSPARK_TOPMODULES = ["pyspark"]
CURRENT_CODE = "__current__"

ROOT_PATH = Path(__file__).parent.parent
AEXPY_DIR = Path(__file__).parent / "aexpy"
CACHE_DIR = AEXPY_DIR / "cache"
assert AEXPY_DIR.exists()


def prepare():
    if not CACHE_DIR.exists():
        os.mkdir(CACHE_DIR)


def runWithLog(logFile: TextIOWrapper = None, *args, **kwargs):
    if logFile:
        logFile.write(f"\nsubprocess.run({args=}, {kwargs=})\n\n")
    result = subprocess.run(*args, **kwargs, capture_output=True, text=True, encoding="utf-8")
    if logFile:
        logFile.write(result.stdout + "\n")
        logFile.write(result.stderr + "\n")
        logFile.flush()
    result.check_returncode()


def runAexPy(cmd: list, logFile: TextIOWrapper = None):
    runWithLog(
        logFile,
        AEXPY_EXE + cmd,
        cwd=AEXPY_DIR,
        env={**os.environ, "PYTHONUTF8": "1", "AEXPY_PYTHON_EXE": sys.executable},
    )


@contextmanager
def checkout(branch: str = CURRENT_CODE, logFile: TextIOWrapper = None):
    if branch == CURRENT_CODE:
        yield ROOT_PATH
    else:
        srcDir = CACHE_DIR / "src" / branch
        if srcDir.exists():
            shutil.rmtree(srcDir)
        runWithLog(logFile, ["git", "worktree", "prune"], cwd=ROOT_PATH)
        os.makedirs(srcDir)
        runWithLog(
            logFile, ["git", "worktree", "add", str(srcDir.resolve()), branch], cwd=ROOT_PATH
        )

        yield srcDir

        shutil.rmtree(srcDir)
        runWithLog(logFile, ["git", "worktree", "prune"], cwd=ROOT_PATH)


def extract(branch: str, srcDir: Path, logFile: TextIOWrapper = None):
    distDir = CACHE_DIR / "dist" / branch
    if not distDir.exists():
        os.makedirs(distDir)

    preprocess = distDir / "preprocess.json"
    runAexPy(
        ["preprocess", str((srcDir / PYSPARK_RELPATH).resolve())]
        + PYSPARK_TOPMODULES
        + ["-r", f"pyspark@{branch}", "-o", str(preprocess.resolve())],
        logFile,
    )

    extract = distDir / "extract.json"
    runAexPy(["extract", str(preprocess.resolve()), "-o", str(extract.resolve())], logFile)

    return extract


def diff(old: str, new: str = CURRENT_CODE):
    distDir = CACHE_DIR / "dist" / new / f"with-{old}"
    if not distDir.exists():
        os.makedirs(distDir)
    logPath = distDir / f"log.log"
    try:
        with open(logPath, "w", encoding="utf-8") as log:
            with checkout(old, log) as src1:
                log.write(f"Extract API of {old}...\n")
                extract1 = extract(old, src1, log)
                with checkout(new) as src2:
                    log.write(f"Extract API of {new}...\n")
                    extract2 = extract(new, src2, log)

                    log.write(f"Diff APIs...\n")
                    diff = distDir / f"diff.json"
                    runAexPy(
                        [
                            "diff",
                            str(extract1.resolve()),
                            str(extract2.resolve()),
                            "-o",
                            str(diff.resolve()),
                        ],
                        log,
                    )

                    log.write(f"Generate changes...\n")
                    report = distDir / f"report.json"
                    runAexPy(["report", str(diff.resolve()), "-o", str(report.resolve())], log)

                    result = json.loads(report.read_text("utf-8"))

                    log.write(f"Result: {result}\n")

                    content = result["content"]
                    (distDir / "report.txt").write_text(content, encoding="utf-8")

                    log.write(f"\n{content}\n")

                    return content
    except Exception as ex:
        traceback.print_exception(ex)
        print(f"An error occured, view log at {logPath.resolve()} .")
        exit(1)


def main():
    prepare()

    old = ""
    outputFile = ""

    if len(sys.argv) >= 2:
        old = sys.argv[1]
    if len(sys.argv) >= 3:
        outputFile = sys.argv[2]

    assert old, "Please give the original branch name or commit id."

    result = diff(old)
    if outputFile:
        Path(outputFile).write_text(result, encoding="utf-8")
    else:
        print(result)


if __name__ == "__main__":
    main()
