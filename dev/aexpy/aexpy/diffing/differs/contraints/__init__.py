
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
