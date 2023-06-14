from logging import Logger
from pathlib import Path
from uuid import uuid1
from .. import Differ

from aexpy.models import DiffEntry
from aexpy.producers import ProducerOptions

from aexpy.models import ApiDescription, ApiDifference
from .checkers import EvalRule


class RuleEvaluator(Differ):
    """Evaluator based on rules."""

    def __init__(self, logger: "Logger | None" = None, rules: "list[EvalRule] | None" = None) -> None:
        super().__init__(logger)
        self.rules: "list[EvalRule]" = rules or []

    def diff(self, old: "ApiDescription", new: "ApiDescription", product: "ApiDifference"):
        for entry in product.entries.values():
            self.logger.debug(f"Evaluate entry {entry.id}: {entry.message}.")

            for rule in self.rules:
                try:
                    rule(entry, product, old, new)
                except Exception as ex:
                    self.logger.error(
                        f"Failed to evaluate entry {entry.id} ({entry.message}) by rule {rule.kind} ({rule.checker}).", exc_info=ex)
            product.entries.update({entry.id: entry})


class DefaultEvaluator(RuleEvaluator):
    def __init__(self, logger: "Logger | None" = None, rules: "list[EvalRule] | None" = None) -> None:
        rules = rules or []
        from .evals import RuleEvals
        rules.extend(RuleEvals.rules)

        super().__init__(logger, rules)

    def diff(self, old: "ApiDescription", new: "ApiDescription", product: "ApiDifference"):
        return super().diff(old, new, product)
