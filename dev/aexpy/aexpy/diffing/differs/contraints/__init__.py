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

import functools
import itertools
from itertools import zip_longest
from typing import Callable, OrderedDict
from aexpy.models import ApiDescription

from aexpy.models.description import (ApiEntry, AttributeEntry, ClassEntry,
                                      CollectionEntry, FunctionEntry, ItemEntry,
                                      ModuleEntry, Parameter, ParameterKind,
                                      SpecialEntry, SpecialKind)
from aexpy.models.difference import DiffEntry


def add(a: "ApiEntry | None", b: "ApiEntry | None", old: "ApiDescription", new: "ApiDescription"):
    if a is None and b is not None:
        return [DiffEntry(message=f"Add {b.__class__.__name__.removesuffix('Entry').lower()} ({b.parent}): {b.name}.")]
    return []


def remove(a: "ApiEntry | None", b: "ApiEntry | None", old: "ApiDescription", new: "ApiDescription"):
    if a is not None and b is None:
        if a.parent in old.entries and a.parent not in new.entries:
            # only report if parent exisits
            return []
        return [DiffEntry(message=f"Remove {a.__class__.__name__.removesuffix('Entry').lower()} ({a.parent}): {a.name}.")]
    return []
