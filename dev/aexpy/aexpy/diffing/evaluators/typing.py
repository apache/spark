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

from typing import Iterable

from aexpy.models import ApiDescription
from aexpy.models.description import ClassEntry
from aexpy.utils import getObjectId

from aexpy.models.typing import (AnyType, CallableType, ClassType, GenericType,
                             LiteralType, NoneType, ProductType, SumType, Type, TypeFactory,
                             UnknownType)


class TypeCompatibilityChecker:
    def isSubclass(self, a: "ClassType", b: "ClassType") -> bool:
        return a.id == b.id or b.id == getObjectId(object)

    def all(self, items: "Iterable[bool | None]"):
        items = list(items)
        if any((t is None for t in items)):
            return None
        return all(items)

    def any(self, items: "Iterable[bool | None]"):
        items = list(items)
        if any((t is None for t in items)):
            return None
        return any(items)

    def isClassCompatibleTo(self, a: "ClassType", b: "Type"):
        match b:
            case ClassType() as cls:
                return self.isSubclass(a, cls)
            case SumType() as sum:
                return self.any(self.isCompatibleTo(a, t) for t in sum.types)
            case GenericType() as gen:
                # support A is a subclass of A<any, any, ...>
                return self.all([self.isCompatibleTo(TypeFactory.any(), t) for t in gen.vars] + [self.isCompatibleTo(a, gen.base)])
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isSumCompatibleTo(self, a: "SumType", b: "Type"):
        return self.all(self.isCompatibleTo(t, b) for t in a.types)

    def isProductCompatibleTo(self, a: "ProductType", b: "Type"):
        match b:
            case ProductType() as prod:
                return len(a.types) == len(prod.types) and self.all(self.isCompatibleTo(t, p) for t, p in zip(a.types, prod.types))
            case SumType() as sum:
                return self.any(self.isCompatibleTo(a, t) for t in sum.types)
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isCallableCompatibleTo(self, a: "CallableType", b: "Type"):
        match b:
            case CallableType() as callable:
                return self.all([self.isCompatibleTo(callable.args, a.args), self.isCompatibleTo(a.ret, callable.ret)])
            case SumType() as sum:
                return self.any(self.isCompatibleTo(a, t) for t in sum.types)
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isGenericCompatibleTo(self, a: "GenericType", b: "Type"):
        match b:
            case GenericType() as generic:
                return len(a.vars) == len(generic.vars) and self.all([self.isCompatibleTo(t, g) for t, g in zip(a.vars, generic.vars)] + [self.isCompatibleTo(a.base, generic.base)])
            case SumType() as sum:
                return any(self.isCompatibleTo(a, t) for t in sum.types)
            case ClassType() as cls:
                # support A<T> is a subclass of A
                return self.isCompatibleTo(a.base, cls)
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isAnyCompatibleTo(self, a: "AnyType", b: "Type"):
        match b:
            case SumType() as sum:
                return self.any(self.isCompatibleTo(a, t) for t in sum.types)
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isNoneCompatibleTo(self, a: "NoneType", b: "Type"):
        match b:
            case NoneType():
                return True
            case SumType() as sum:
                return self.any(self.isCompatibleTo(a, t) for t in sum.types)
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isLiteralCompatibleTo(self, a: "LiteralType", b: "Type"):
        match b:
            case LiteralType() as literal:
                return a.value == literal.value
            case SumType() as sum:
                return self.any(self.isCompatibleTo(a, t) for t in sum.types)
            case AnyType():
                return True
            case UnknownType():
                return None
            case _:
                return False

    def isCompatibleTo(self, a: "Type", b: "Type") -> "bool | None":
        """Return type class a is a subset of type class b, indicating that instance of a can be assign to variable of b."""
        match a:
            case ClassType():
                return self.isClassCompatibleTo(a, b)
            case SumType():
                return self.isSumCompatibleTo(a, b)
            case ProductType():
                return self.isProductCompatibleTo(a, b)
            case CallableType():
                return self.isCallableCompatibleTo(a, b)
            case GenericType():
                return self.isGenericCompatibleTo(a, b)
            case AnyType():
                return self.isAnyCompatibleTo(a, b)
            case NoneType():
                return self.isNoneCompatibleTo(a, b)
            case UnknownType():
                return None
            case LiteralType():
                return self.isLiteralCompatibleTo(a, b)
            case _:
                return False


class ApiTypeCompatibilityChecker(TypeCompatibilityChecker):
    def __init__(self, api: "ApiDescription") -> None:
        super().__init__()
        self.api = api

    def isSubclass(self, a: "ClassType", b: "ClassType") -> bool:
        if super().isSubclass(a, b):
            return True
        ea = self.api.entries.get(a.id)
        if not isinstance(ea, ClassEntry):
            return False
        return b.id in ea.bases or b.id in ea.abcs or b.id in ea.mro
