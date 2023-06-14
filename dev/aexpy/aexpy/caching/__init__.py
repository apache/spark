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

from abc import ABC, abstractmethod
from pathlib import Path

from ..utils import ensureDirectory
from ..models import Product


class ProduceCache(ABC):
    def __init__(self, id: "str") -> None:
        super().__init__()
        self.id = id

    @abstractmethod
    def save(self, product: "Product", log: str) -> None:
        pass

    @abstractmethod
    def data(self) -> "Product":
        pass

    @abstractmethod
    def log(self) -> str:
        pass


class FileProduceCache(ProduceCache):
    def __init__(self, id: "str", cacheFile: "Path") -> None:
        super().__init__(id)
        self.cacheFile = cacheFile
        # self.logFile = self.cacheFile.with_suffix(".log")

    def save(self, product: "Product", log: "str") -> None:
        ensureDirectory(self.cacheFile.parent)
        self.cacheFile.write_text(product.dumps())
        # self.logFile.write_text(log)

    def data(self) -> "str":
        return self.cacheFile.read_text()

    def log(self) -> str:
        # return self.logFile.read_text()
        pass
