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
