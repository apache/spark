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

from dataclasses import dataclass, field
from enum import IntEnum
from typing import Any

from .description import ApiEntry


class BreakingRank(IntEnum):
    Unknown = -1
    Compatible = 0
    Low = 30
    Medium = 60
    High = 100


class VerifyState(IntEnum):
    Unknown = 0
    Fail = 50
    Pass = 100


@dataclass
class VerifyData:
    state: VerifyState = VerifyState.Unknown
    message: str = ""
    verifier: str = ""


@dataclass
class DiffEntry:
    id: "str" = ""
    kind: "str" = ""
    rank: "BreakingRank" = BreakingRank.Unknown
    verify: "VerifyData" = field(default_factory=VerifyData)
    message: "str" = ""
    data: "dict[str, Any]" = field(default_factory=dict)
    old: "ApiEntry | None" = field(default=None, repr=False)
    new: "ApiEntry | None" = field(default=None, repr=False)
