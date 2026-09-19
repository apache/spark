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

"""
A tiny Sphinx extension that forbids the ``:lines:`` option on ``literalinclude``.

Pinning a snippet to absolute line numbers rots silently: any edit to the included
source file (adding an import, re-sorting imports, inserting a blank line) shifts the
range, so the docs either render the wrong code or fail the build with a
"non-whitespace stripped by dedent" warning. Prefer selecting the region by name with
``:pyobject:`` or by sentinel comments with ``:start-after:`` / ``:end-before:``, both
of which are robust to line movement.

This overrides the built-in ``literalinclude`` directive to raise a build error the
moment ``:lines:`` is used. As the docs build treats warnings as errors, the error is
surfaced immediately in CI.
"""

from __future__ import annotations

from typing import Any, Dict

from docutils.nodes import Node
from sphinx.application import Sphinx
from sphinx.directives.code import LiteralInclude


class LiteralIncludeNoLines(LiteralInclude):
    """``literalinclude`` that rejects the fragile ``:lines:`` option."""

    def run(self) -> list[Node]:
        if "lines" in self.options:
            raise self.error(
                "literalinclude with ':lines:' is not allowed: it pins absolute line "
                "numbers, which silently break when the included file changes. Select "
                "the region by name with ':pyobject:', or bracket it with sentinel "
                "comments and use ':start-after:'/':end-before:' instead."
            )
        return super().run()


def setup(app: Sphinx) -> Dict[str, Any]:
    # override=True replaces the built-in ``literalinclude`` registration.
    app.add_directive("literalinclude", LiteralIncludeNoLines, override=True)
    return {
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
