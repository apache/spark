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

from abc import ABC, abstractmethod
from typing import Optional


class ReadLimit(ABC):
    @classmethod
    @abstractmethod
    def type_name(cls) -> str:
        pass

    @classmethod
    @abstractmethod
    def load(cls, params: dict) -> "ReadLimit":
        pass

    def dump(self) -> dict:
        params = self._dump()
        params.update({"type": self.type_name()})
        return params

    @abstractmethod
    def _dump(self) -> dict:
        pass


class ReadAllAvailable(ReadLimit):
    @classmethod
    def type_name(cls) -> str:
        return "ReadAllAvailable"

    @classmethod
    def load(cls, params: dict) -> "ReadAllAvailable":
        return ReadAllAvailable()

    def _dump(self) -> dict:
        return {}


class ReadMinRows(ReadLimit):
    def __init__(self, min_rows: int) -> None:
        self.min_rows = min_rows

    @classmethod
    def type_name(cls) -> str:
        return "ReadMinRows"

    @classmethod
    def load(cls, params: dict) -> "ReadMinRows":
        return ReadMinRows(params["min_rows"])

    def _dump(self) -> dict:
        return {"min_rows": self.min_rows}


class ReadMaxRows(ReadLimit):
    def __init__(self, max_rows: int) -> None:
        self.max_rows = max_rows

    @classmethod
    def type_name(cls) -> str:
        return "ReadMaxRows"

    @classmethod
    def load(cls, params: dict) -> "ReadMaxRows":
        return ReadMaxRows(params["max_rows"])

    def _dump(self) -> dict:
        return {"max_rows": self.max_rows}


class ReadMaxFiles(ReadLimit):
    def __init__(self, max_files: int) -> None:
        self.max_files = max_files

    @classmethod
    def type_name(cls) -> str:
        return "ReadMaxFiles"

    @classmethod
    def load(cls, params: dict) -> "ReadMaxFiles":
        return ReadMaxFiles(params["max_files"])

    def _dump(self) -> dict:
        return {"max_files": self.max_files}


class ReadMaxBytes(ReadLimit):
    def __init__(self, max_bytes: int) -> None:
        self.max_bytes = max_bytes

    @classmethod
    def type_name(cls) -> str:
        return "ReadMaxBytes"

    @classmethod
    def load(cls, params: dict) -> "ReadMaxBytes":
        return ReadMaxBytes(params["max_bytes"])

    def _dump(self) -> dict:
        return {"max_bytes": self.max_bytes}


class SupportsTriggerAvailableNow(ABC):
    @abstractmethod
    def prepareForTriggerAvailableNow(self) -> None:
        """
        FIXME: docstring needed

        /**
         * This will be called at the beginning of streaming queries with Trigger.AvailableNow, to let the
         * source record the offset for the current latest data at the time (a.k.a the target offset for
         * the query). The source will behave as if there is no new data coming in after the target
         * offset, i.e., the source will not return an offset higher than the target offset when
         * {@link #latestOffset(Offset, ReadLimit) latestOffset} is called.
         * <p>
         * Note that there is an exception on the first uncommitted batch after a restart, where the end
         * offset is not derived from the current latest offset. Sources need to take special
         * considerations if wanting to assert such relation. One possible way is to have an internal
         * flag in the source to indicate whether it is Trigger.AvailableNow, set the flag in this method,
         * and record the target offset in the first call of
         * {@link #latestOffset(Offset, ReadLimit) latestOffset}.
         */
        """
        pass
