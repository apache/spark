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

import subprocess

from aexpy import json
from aexpy import getPythonExe
from aexpy.extracting.enriching.callgraph import Callgraph
from aexpy.extracting.third.mypyserver import PackageMypyServer
from aexpy.models.description import TRANSFER_BEGIN, FunctionEntry

from .. import getAppDirectory
from ..models import ApiDescription, Distribution
from . import Extractor


class DefaultExtractor(Extractor):
    """Basic extractor that uses dynamic inspect."""

    def base(self, dist: "Distribution", product: "ApiDescription"):
        assert dist
        subres = subprocess.run(
            f"{getPythonExe()} -m aexpy.extracting.main.base",
            cwd=getAppDirectory().parent,
            text=True,
            capture_output=True,
            input=dist.dumps(),
        )

        self.logger.info(f"Inner extractor exit with {subres.returncode}.")

        if subres.stdout.strip():
            self.logger.debug(f"STDOUT:\n{subres.stdout}")
        if subres.stderr.strip():
            self.logger.info(f"STDERR:\n{subres.stderr}")

        subres.check_returncode()

        data = subres.stdout.split(TRANSFER_BEGIN, 1)[1]
        data = json.loads(data)
        product.load(data)

    def attributes(
        self,
        dist: "Distribution",
        product: "ApiDescription",
        server: "PackageMypyServer | None",
    ):
        from .enriching import attributes

        product.clearCache()
        if server:
            attributes.InstanceAttributeMypyEnricher(server, self.logger).enrich(
                product
            )
        else:
            attributes.InstanceAttributeAstEnricher(self.logger).enrich(product)

    def enrichCallgraph(self, product: "ApiDescription", cg: "Callgraph"):
        callees: "dict[str, set[str]]" = {}

        for caller in cg.items.values():
            cur = set()
            for site in caller.sites:
                for target in site.targets:
                    cur.add(target)
            callees[caller.id] = cur

        for key, value in callees.items():
            entry = product.entries.get(key)
            if not isinstance(entry, FunctionEntry):
                continue
            entry.callees = list(value)

        product.calcCallers()

    def kwargs(
        self,
        dist: "Distribution",
        product: "ApiDescription",
        server: "PackageMypyServer | None",
    ):
        from .enriching import kwargs

        product.clearCache()

        if server:
            from .enriching.callgraph.type import TypeCallgraphBuilder

            cg = TypeCallgraphBuilder(server, self.logger).build(product)
            self.enrichCallgraph(product, cg)
            kwargs.KwargsEnricher(cg, self.logger).enrich(product)
        else:
            from .enriching.callgraph.basic import BasicCallgraphBuilder

            cg = BasicCallgraphBuilder(self.logger).build(product)
            self.enrichCallgraph(product, cg)
            kwargs.KwargsEnricher(Callgraph(), self.logger).enrich(product)

    def types(
        self,
        dist: "Distribution",
        product: "ApiDescription",
        server: "PackageMypyServer | None",
    ):
        from .enriching import types

        product.clearCache()

        if server:
            types.TypeEnricher(server, self.logger).enrich(product)

    def extract(self, dist: "Distribution", product: "ApiDescription"):
        self.base(dist, product)

        try:
            server = PackageMypyServer(dist.rootPath, dist.src, self.logger)
            server.prepare()
        except Exception as ex:
            self.logger.error(
                f"Failed to run mypy server at {dist.rootPath}: {dist.src}.",
                exc_info=ex,
            )
            server = None

        self.attributes(dist, product, server)
        self.kwargs(dist, product, server)
        self.types(dist, product, server)
