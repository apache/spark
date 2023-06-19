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
from aexpy.extracting.main.base import islocal

from aexpy.models.description import ApiEntry, ClassEntry, CollectionEntry, ModuleEntry
from aexpy.models.difference import DiffEntry
from aexpy.producers import ProducerOptions

from aexpy.models import ApiDescription, ApiDifference, Distribution
from . import Differ


class DefaultDiffer(Differ):
    def diff(self, old: "ApiDescription", new: "ApiDescription", product: "ApiDifference"):
        from .differs.default import DefaultDiffer

        DefaultDiffer(self.logger).diff(old, new, product)

        from .evaluators.default import DefaultEvaluator

        DefaultEvaluator(self.logger).diff(old, new, product)
