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

from dataclasses import dataclass
import logging
from abc import ABC
from logging import Logger


@dataclass
class ProducerOptions:
    def load(self, data: "dict"):
        for k, v in data.items():
            setattr(self, k, v)


class Producer(ABC):
    """Producer that produces a product."""

    @classmethod
    def cls(cls) -> "str":
        """Returns the class name of the producer, used by ProducerConfig."""
        return f"{cls.__module__}.{cls.__qualname__}"

    @property
    def name(self) -> "str":
        return self._name if hasattr(self, "_name") else self.cls()

    @name.setter
    def name(self, value: "str") -> None:
        self._name = value

    def __init__(self, logger: "Logger | None" = None) -> None:
        self.logger = (
            logger.getChild(self.name) if logger else logging.getLogger(self.name)
        )
        """The logger for the producer."""
        self.options = ProducerOptions()
        self.name = "aexpy"
