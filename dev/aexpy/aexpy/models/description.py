from dataclasses import dataclass, field
from enum import Enum, IntEnum
from typing import Any

from .typing import Type, loadType

TRANSFER_BEGIN = "AEXPY_TRANSFER_BEGIN"
EXTERNAL_ENTRYID = "$external$"


@dataclass
class TypeInfo:
    type: "Type | None" = None
    id: "str" = ""
    raw: "str" = ""
    data: "dict | str" = ""


@dataclass
class Location:
    file: "str" = ""
    line: "int" = -1
    module: "str" = ""

    def __str__(self):
        return f"{self.file}:{self.line}:{self.module}"


@dataclass
class ApiEntry:
    name: "str" = ""
    id: "str" = ""
    alias: "list[str]" = field(default_factory=list)
    docs: "str" = field(default="", repr=False)
    comments: "str" = field(default="", repr=False)
    src: "str" = field(default="", repr=False)
    location: "Location | None" = None
    private: "bool" = False
    parent: "str" = ""
    schema: "str" = ""
    data: "dict[str, Any]" = field(default_factory=dict)


@dataclass
class CollectionEntry(ApiEntry):
    members: "dict[str, str]" = field(default_factory=dict)
    annotations: "dict[str, str]" = field(default_factory=dict)

    @property
    def aliasMembers(self):
        if not hasattr(self, "_aliasMembers"):
            self._aliasMembers = {
                k: v for k, v in self.members.items() if v != f"{self.id}.{k}"
            }
        return self._aliasMembers


class ItemScope(IntEnum):
    Static = 0
    Class = 1
    Instance = 2


@dataclass
class ItemEntry(ApiEntry):
    scope: "ItemScope" = ItemScope.Static
    type: "TypeInfo | None" = None


class SpecialKind(IntEnum):
    Unknown = 0
    Empty = 1
    External = 2


@dataclass
class SpecialEntry(ApiEntry):
    kind: "SpecialKind" = SpecialKind.Unknown
    data: "str" = ""

    def __post_init__(self):
        self.schema = "special"


@dataclass
class ModuleEntry(CollectionEntry):
    def __post_init__(self):
        self.schema = "module"


@dataclass
class ClassEntry(CollectionEntry):
    bases: "list[str]" = field(default_factory=list)
    abcs: "list[str]" = field(default_factory=list)
    mro: "list[str]" = field(default_factory=list)
    slots: "list[str]" = field(default_factory=list)

    def __post_init__(self):
        self.schema = "class"


@dataclass
class AttributeEntry(ItemEntry):
    rawType: "str" = ""
    annotation: "str" = ""
    property: "bool" = False

    def __post_init__(self):
        self.schema = "attr"


class ParameterKind(Enum):
    Positional = 0
    PositionalOrKeyword = 1
    VarPositional = 2
    Keyword = 3
    VarKeyword = 4
    VarKeywordCandidate = 5


@dataclass
class Parameter:
    kind: "ParameterKind" = ParameterKind.PositionalOrKeyword
    name: "str" = ""
    annotation: "str" = ""
    default: "str | None" = None
    """Default value. None for variable default value."""
    optional: "bool" = False
    source: "str" = ""
    type: "TypeInfo | None" = None

    @property
    def isKeyword(self):
        return self.kind in {
            ParameterKind.Keyword,
            ParameterKind.PositionalOrKeyword,
            ParameterKind.VarKeywordCandidate,
        }

    @property
    def isPositional(self):
        return self.kind in {
            ParameterKind.Positional,
            ParameterKind.PositionalOrKeyword,
        }

    @property
    def isVar(self):
        return self.kind in {ParameterKind.VarKeyword, ParameterKind.VarPositional}


@dataclass
class FunctionEntry(ItemEntry):
    returnAnnotation: "str" = ""
    parameters: "list[Parameter]" = field(default_factory=list)
    annotations: "dict[str, str]" = field(default_factory=dict)
    returnType: "TypeInfo | None" = None
    callers: "list[str]" = field(default_factory=list)
    callees: "list[str]" = field(default_factory=list)
    transmitKwargs: "bool" = False

    def __post_init__(self):
        self.schema = "func"

    def getParameter(self, name: str) -> "Parameter | None":
        for p in self.parameters:
            if p.name == name:
                return p

    def position(self, parameter: "Parameter") -> "int | None":
        try:
            return self.positionals.index(parameter)
        except:
            return None

    @property
    def positionalOnlys(self):
        return [x for x in self.parameters if x.kind == ParameterKind.Positional]

    @property
    def positionals(self):
        return [x for x in self.parameters if x.isPositional]

    @property
    def keywordOnlys(self):
        return [x for x in self.parameters if x.kind == ParameterKind.Keyword]

    @property
    def keywords(self):
        return [x for x in self.parameters if x.isKeyword]

    @property
    def candidates(self):
        return [
            x for x in self.parameters if x.kind == ParameterKind.VarKeywordCandidate
        ]

    @property
    def varPositional(self) -> "Parameter | None":
        items = [x for x in self.parameters if x.kind == ParameterKind.VarPositional]
        if len(items) > 0:
            return items[0]
        return None

    @property
    def varKeyword(self) -> "Parameter | None":
        items = [x for x in self.parameters if x.kind == ParameterKind.VarKeyword]
        if len(items) > 0:
            return items[0]
        return None


def loadTypeInfo(data: "dict | None") -> "TypeInfo | None":
    if data is None:
        return None
    if "type" in data:
        data["type"] = loadType(data["type"])
    return TypeInfo(**data)


def loadEntry(entry: "dict | None") -> "ApiEntry | None":
    if entry is None:
        return None
    schema = entry.pop("schema")
    data: dict = entry
    binded = None
    if schema == "attr":
        type = loadTypeInfo(data.pop("type"))
        binded = AttributeEntry(type=type, **data)
    elif schema == "module":
        binded = ModuleEntry(**data)
    elif schema == "class":
        binded = ClassEntry(**data)
    elif schema == "func":
        type = loadTypeInfo(data.pop("type"))
        returnType = loadTypeInfo(data.pop("returnType"))
        paras = data.pop("parameters")
        bindedParas = []
        for para in paras:
            kind = ParameterKind(para.pop("kind"))
            paratype = loadTypeInfo(para.pop("type"))
            bindedParas.append(Parameter(kind=kind, type=paratype, **para))
        binded = FunctionEntry(
            parameters=bindedParas, type=type, returnType=returnType, **data
        )
    elif schema == "special":
        kind = SpecialKind(data.pop("kind"))
        binded = SpecialEntry(kind=kind, **data)
    assert isinstance(binded, ApiEntry)
    if "location" in data and data["location"] is not None:
        location = Location(**data["location"])
        binded.location = location
    return binded
