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

from datetime import datetime
import logging
import os
import pathlib

__version__ = "0.2.0"


LOGGING_FORMAT = (
    "%(levelname)s %(asctime)s %(name)s [%(pathname)s:%(lineno)d:%(funcName)s]\n%(message)s\n"
)
LOGGING_DATEFMT = "%Y-%m-%d,%H:%M:%S"


def initializeLogging(level: int = logging.WARNING) -> None:
    root = logging.getLogger()
    root.setLevel(logging.NOTSET)
    root.handlers.clear()
    handler = logging.StreamHandler()
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(LOGGING_FORMAT, LOGGING_DATEFMT))
    root.addHandler(handler)


def getAppDirectory() -> pathlib.Path:
    return pathlib.Path(__file__).parent.resolve()


def getWorkingDirectory() -> pathlib.Path:
    return pathlib.Path(os.getcwd()).resolve()


def getCommitId() -> str:
    return os.getenv("GIT_COMMIT", "unknown")


def getBuildDate() -> datetime:
    try:
        return datetime.fromisoformat(os.getenv("BUILD_DATE", "unknown"))
    except:
        return datetime.now()


def getPythonExe() -> str:
    return os.getenv("AEXPY_PYTHON_EXE", "python")
