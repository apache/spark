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

import ast
import logging
import textwrap
from abc import ABC, abstractmethod
from ast import Call, NodeVisitor, expr, parse
from dataclasses import dataclass, field
from typing import Any

from aexpy.models import ApiDescription, FunctionEntry


@dataclass
class Argument:
    name: "str" = ""
    value: "Any | None" = None
    iskwargs: "bool" = False
    raw: "str" = ""


@dataclass
class Callsite:
    targets: "list[str]" = field(default_factory=list)
    targetValue: "Any | None" = None
    arguments: "list[Argument]" = field(default_factory=list)
    value: "Any | None" = None
    raw: "str" = ""


@dataclass
class Caller:
    id: "str" = ""
    sites: "list[Callsite]" = field(default_factory=list)


@dataclass
class Callgraph:
    items: "dict[str, Caller]" = field(default_factory=dict)

    def add(self, item: "Caller"):
        self.items[item.id] = item


class CallgraphBuilder(ABC):
    @abstractmethod
    def build(self, api: "ApiDescription") -> "Callgraph":
        pass
