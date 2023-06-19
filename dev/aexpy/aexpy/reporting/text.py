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

from aexpy.models import ApiDifference, Report
from aexpy.models.difference import BreakingRank, DiffEntry
from aexpy.reporting import Reporter

BCIcons = {
    BreakingRank.Compatible: "🟢",
    BreakingRank.Low: "🟡",
    BreakingRank.Medium: "🟠",
    BreakingRank.High: "🔴",
    BreakingRank.Unknown: "❔",
}

BCLevel = {
    BreakingRank.Compatible: "✅",
    BreakingRank.Low: "❓",
    BreakingRank.Medium: "❗",
    BreakingRank.High: "❌",
    BreakingRank.Unknown: "❔",
}

StageIcons = {
    "preprocess": "📦",
    "extract": "🔍",
    "diff": "📑",
    "evaluate": "🔬",
    "report": "📜",
}


def formatMessage(item: "DiffEntry") -> str:
    ret = []
    submessages = item.message.split(": ", 1)
    ret.append(" ".join([BCIcons[item.rank], submessages[0].strip()]))
    if len(submessages) > 1:
        for entry in submessages[1].split(";"):
            cur = entry.strip().removesuffix(".")
            cur = cur.replace("=>", " → ")
            ret.append("     " + cur)
    return "\n".join(ret)


class TextReporter(Reporter):
    """Generate a text report."""

    def report(self, diff: "ApiDifference", product: "Report"):
        result = ""

        oldRelease = diff.old.release
        newRelease = diff.new.release

        level, changesCount = diff.evaluate()

        result += f"""📜 {oldRelease} → {newRelease} {BCLevel[level]}

▶ {oldRelease}
  📦 {diff.old.rootPath}
  🔖 {diff.old.pyversion}
  📚 {', '.join(diff.old.topModules)}

▶ {newRelease}
  📦 {diff.new.rootPath}
  🔖 {diff.new.pyversion}
  📚 {', '.join(diff.new.topModules)}\n"""

        changes = diff.breaking(BreakingRank.Unknown)
        bcs = diff.breaking(BreakingRank.Low)
        nbcs = diff.rank(BreakingRank.Unknown) + diff.rank(BreakingRank.Compatible)

        if len(changes) > 0:
            result += f"\n📋 Changes {' '.join([f'{BCIcons[rank]} {changesCount[rank]}' for rank in sorted(changesCount.keys(), reverse=True)])}\n"

        if len(bcs) > 0:
            result += "\n🚧 Breakings\n\n"
            bcs.sort(key=lambda x: (x.rank, x.kind), reverse=True)
            for item in bcs:
                result += f"{formatMessage(item)}\n"

        if len(nbcs) > 0:
            result += "\n🧪 Non-breakings\n\n"
            nbcs.sort(key=lambda x: (x.rank, x.kind), reverse=True)
            for item in nbcs:
                result += f"{formatMessage(item)}\n"

        result += "\n"

        product.content = result
