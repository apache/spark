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
