
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
