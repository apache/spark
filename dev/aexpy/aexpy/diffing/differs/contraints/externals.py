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

from aexpy.models import ApiDescription
from aexpy.models.description import ApiEntry, SpecialEntry, SpecialKind
from aexpy.models.difference import DiffEntry

from ..checkers import DiffConstraint, DiffConstraintCollection, diffcons, fortype

ExternalConstraints = DiffConstraintCollection()


@ExternalConstraints.cons
@fortype(SpecialEntry, True)
@diffcons
def AddExternal(a: SpecialEntry | None, b: SpecialEntry | None, old: "ApiDescription", new: "ApiDescription"):
    if a is None and b is not None:
        if b.kind == SpecialKind.External:
            return [DiffEntry(message=f"Add external: {b.id}.")]
    return []


@ExternalConstraints.cons
@fortype(SpecialEntry, True)
@diffcons
def RemoveExternal(a: SpecialEntry | None, b: SpecialEntry | None, old: "ApiDescription", new: "ApiDescription"):
    if b is None and a is not None:
        if a.kind == SpecialKind.External:
            return [DiffEntry(message=f"Remove external: {a.id}.")]
    return []
