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

    def __init__(
        self, logger: "Logger | None" = None, rules: "list[EvalRule] | None" = None
    ) -> None:
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
                        f"Failed to evaluate entry {entry.id} ({entry.message}) by rule {rule.kind} ({rule.checker}).",
                        exc_info=ex,
                    )
            product.entries.update({entry.id: entry})


class DefaultEvaluator(RuleEvaluator):
    def __init__(
        self, logger: "Logger | None" = None, rules: "list[EvalRule] | None" = None
    ) -> None:
        rules = rules or []
        from .evals import RuleEvals

        rules.extend(RuleEvals.rules)

        super().__init__(logger, rules)

    def diff(self, old: "ApiDescription", new: "ApiDescription", product: "ApiDifference"):
        return super().diff(old, new, product)
