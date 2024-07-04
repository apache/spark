# Licensed under the PYTHON SOFTWARE FOUNDATION LICENSE VERSION 2
# https://github.com/python/cpython/blob/3.11/LICENSE
# File originates from the cpython source
# https://github.com/python/cpython/blob/3.11/Lib/distutils/version.py

import re
from typing import Optional


class LooseVersion:
    component_re = re.compile(r"(\d+ | [a-z]+ | \.)", re.VERBOSE)

    def __init__(self, vstring: Optional[str]) -> None:
        if vstring:
            self.parse(vstring)

    def parse(self, vstring: str) -> None:
        self.vstring = vstring
        components = [x for x in self.component_re.split(vstring) if x and x != "."]
        for i, obj in enumerate(components):
            try:
                components[i] = int(obj)
            except ValueError:
                pass

        self.version = components

    def __str__(self) -> str:
        return self.vstring

    def __repr__(self) -> str:
        return "LooseVersion ('%s')" % str(self)

    def __eq__(self, other):  # type: ignore[no-untyped-def]
        c = self._cmp(other)
        if c is NotImplemented:
            return c
        return c == 0

    def __lt__(self, other):  # type: ignore[no-untyped-def]
        c = self._cmp(other)
        if c is NotImplemented:
            return c
        return c < 0

    def __le__(self, other):  # type: ignore[no-untyped-def]
        c = self._cmp(other)
        if c is NotImplemented:
            return c
        return c <= 0

    def __gt__(self, other):  # type: ignore[no-untyped-def]
        c = self._cmp(other)
        if c is NotImplemented:
            return c
        return c > 0

    def __ge__(self, other):  # type: ignore[no-untyped-def]
        c = self._cmp(other)
        if c is NotImplemented:
            return c
        return c >= 0

    def _cmp(self, other):  # type: ignore[no-untyped-def]
        if isinstance(other, str):
            other = LooseVersion(other)
        elif not isinstance(other, LooseVersion):
            return NotImplemented

        if self.version == other.version:
            return 0
        if self.version < other.version:
            return -1
        if self.version > other.version:
            return 1
