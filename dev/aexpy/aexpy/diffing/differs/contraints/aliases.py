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

from aexpy.extracting.main.base import islocal
from aexpy.models import ApiDescription
from aexpy.models.description import CollectionEntry
from aexpy.models.difference import DiffEntry

from ..checkers import DiffConstraint, DiffConstraintCollection, diffcons, fortype

AliasConstraints = DiffConstraintCollection()


@AliasConstraints.cons
@fortype(CollectionEntry)
@diffcons
def AddAlias(a: CollectionEntry, b: CollectionEntry, old: "ApiDescription", new: "ApiDescription"):
    sub = (b.members.keys() - a.members.keys()) & b.aliasMembers.keys()
    return [DiffEntry(message=f"Add alias ({a.id}): {name} => {b.members[name]}", data={"name": name, "target": b.members[name]}) for name in sub]


@AliasConstraints.cons
@fortype(CollectionEntry)
@diffcons
def RemoveAlias(a: CollectionEntry, b: CollectionEntry, old: "ApiDescription", new: "ApiDescription"):
    sub = (a.members.keys() - b.members.keys()) & a.aliasMembers.keys()
    return [DiffEntry(message=f"Remove alias ({a.id}): {name} => {a.members[name]}", data={"name": name, "target": a.members[name]}) for name in sub if new.resolveName(f"{a.id}.{name}") is None]


@AliasConstraints.cons
@fortype(CollectionEntry)
@diffcons
def ChangeAlias(a: CollectionEntry, b: CollectionEntry, old: "ApiDescription", new: "ApiDescription"):
    inter = a.members.keys() & b.members.keys()
    changed = {}
    for k in inter:
        if a.members[k] != b.members[k]:
            changed[k] = (a.members[k], b.members[k])
    return [DiffEntry(message=f"Change alias ({a.id}): {name}: {old} => {new}", data={"name": name, "old": old, "new": new}) for name, (old, new) in changed.items()]
