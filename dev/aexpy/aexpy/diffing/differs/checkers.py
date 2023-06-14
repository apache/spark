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

import dataclasses
from dataclasses import dataclass, field
from typing import Any, Callable, TypeVar

from aexpy.models.description import ApiEntry
from aexpy.models.difference import BreakingRank

from aexpy.models import ApiDescription, DiffEntry

T_ApiEntry = TypeVar("T_ApiEntry", bound=ApiEntry)


class DiffConstraint:
    """
    A contraint (checker) generating DiffEntry.

    checker: def checker(a: ApiEntry | None, b: ApiEntry | None, old=oldApiDescription, new=newApiDescription) -> RuleCheckResult | bool: pass
    """

    def __init__(self, kind: "str" = "", checker: "Callable[[T_ApiEntry | None, T_ApiEntry | None, ApiDescription, ApiDescription], list[DiffEntry]] | None" = None) -> None:
        if checker is None:
            def tchecker(a: Any, b: Any, old: Any, new: Any):
                return []
            checker = tchecker
        self.checker: "Callable[[T_ApiEntry | None, T_ApiEntry | None, ApiDescription, ApiDescription], list[DiffEntry]]" = checker
        self.kind = kind

    def askind(self, kind: "str"):
        """Set kind."""

        self.kind = kind
        return self

    def fortype(self, type, optional: bool = False):
        """Limit to a type of ApiEntry."""

        oldchecker = self.checker

        def checker(a, b, **kwargs):
            if optional:
                if not isinstance(a, type):
                    a = None
                if not isinstance(b, type):
                    b = None
                if a or b:
                    return oldchecker(a, b, **kwargs)  # type: ignore
                return []
            else:
                if isinstance(a, type) and isinstance(b, type):
                    return oldchecker(a, b, **kwargs)  # type: ignore
                else:
                    return []

        self.checker = checker  # type: ignore
        return self

    def __call__(self, old, new, oldCollection, newCollection) -> "list[DiffEntry]":
        result = self.checker(
            old, new, old=oldCollection, new=newCollection)  # type: ignore
        if result:
            return [dataclasses.replace(entry, kind=self.kind, old=old, new=new) for entry in result]
        else:
            return []


@dataclass
class DiffConstraintCollection:
    """Collection of DiffConstraint."""

    constraints: "list[DiffConstraint]" = field(default_factory=list)

    def cons(self, constraint: "DiffConstraint"):
        self.constraints.append(constraint)
        return constraint


def diffcons(checker: "Callable[[T_ApiEntry, T_ApiEntry, ApiDescription, ApiDescription], list[DiffEntry]]") -> "DiffConstraint":
    """Create a DiffConstraint on a function."""

    return DiffConstraint(checker.__name__, checker)  # type: ignore


def fortype(type, optional: "bool" = False):
    """Limit the diff constraint to a type of ApiEntry."""

    def decorator(constraint: "DiffConstraint") -> "DiffConstraint":
        return constraint.fortype(type, optional)

    return decorator
