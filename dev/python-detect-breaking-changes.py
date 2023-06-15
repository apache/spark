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

from pathlib import Path
import os
import shutil
from contextlib import contextmanager
import subprocess
import sys
import json

PYTHON_EXE = "python"
AEXPY_EXE = [PYTHON_EXE, "-m", "aexpy"]
PYSPARK_RELPATH = "python"
PYSPARK_TOPMODULES = ["pyspark"]

AEXPY_DIR = Path(__file__).parent / "aexpy"

assert AEXPY_DIR.exists()

CACHE_DIR = AEXPY_DIR / "cache"

def prepare():
    if not CACHE_DIR.exists():
        os.mkdir(CACHE_DIR)

@contextmanager
def checkout(branch: str):
    srcDir = CACHE_DIR / "src" / branch
    if srcDir.exists():
        shutil.rmtree(srcDir)
    subprocess.run(["git", "worktree", "prune"], check=True)
    os.makedirs(srcDir)
    subprocess.run(["git", "worktree", "add", str(srcDir.resolve()), branch], check=True)

    yield srcDir

    shutil.rmtree(srcDir)
    subprocess.run(["git", "worktree", "prune"], check=True)

def extract(branch: str, srcDir: Path):
    distDir = CACHE_DIR / "dist" / branch
    if not distDir.exists():
        os.makedirs(distDir)
    
    preprocess = distDir / "preprocess.json"
    subprocess.run(AEXPY_EXE + ["preprocess", str((srcDir / PYSPARK_RELPATH).resolve())] + PYSPARK_TOPMODULES + ["-r", f"pyspark@{branch}", "-o", str(preprocess.resolve())], cwd=AEXPY_DIR, check=True, env={**os.environ, "PYTHONUTF8": "1"})

    extract = distDir / "extract.json"
    subprocess.run(AEXPY_EXE + ["extract", str(preprocess.resolve()), "-o", str(extract.resolve())], cwd=AEXPY_DIR, check=True, env={**os.environ, "PYTHONUTF8": "1"})
    
    return extract

def diff(old: str, new: str):
    with checkout(old) as src1:
        print(f"Extract API of {old}...")
        extract1 = extract(old, src1)
        with checkout(new) as src2:
            print(f"Extract API of {new}...")
            extract2 = extract(new, src2)
            
            print(f"Diff APIs...")
            diff = CACHE_DIR / f"diff-{old}-{new}.json"
            subprocess.run(AEXPY_EXE + ["diff", str(extract1.resolve()), str(extract2.resolve()), "-o", str(diff.resolve())], cwd=AEXPY_DIR, check=True, env={**os.environ, "PYTHONUTF8": "1"})

            print(f"Generate changes...")
            report = CACHE_DIR / f"report-{old}-{new}.json"
            subprocess.run(AEXPY_EXE + ["report", str(diff.resolve()), "-o", str(report.resolve())], cwd=AEXPY_DIR, check=True, env={**os.environ, "PYTHONUTF8": "1"})

            result = json.loads(report.read_text("utf-8"))

            return result["content"]

def main():
    prepare()

    assert len(sys.argv) == 2, f"Please give the target branch name"

    old = sys.argv[1]
    new = "master"
    
    result = diff(old, new)
    print(result)


if __name__ == "__main__":
    main()