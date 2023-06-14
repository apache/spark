import logging
import pathlib
from datetime import datetime
from typing import Tuple
from uuid import uuid1

import mypy
from mypy import find_sources
from mypy.build import State
from mypy.dmypy_server import Server
from mypy.dmypy_util import DEFAULT_STATUS_FILE
from mypy.infer import infer_function_type_arguments
from mypy.nodes import (ARG_NAMED, ARG_NAMED_OPT, ARG_POS, ARG_STAR, ARG_STAR2,
                        AssignmentStmt, CallExpr, Context, Expression,
                        FuncBase, FuncDef, MemberExpr, MypyFile, NameExpr,
                        Node, RefExpr, ReturnStmt, SymbolNode, SymbolTable,
                        SymbolTableNode, TypeInfo, Var)
from mypy.options import Options
from mypy.traverser import TraverserVisitor
from mypy.types import (AnyType, CallableType, Instance, NoneTyp, Type,
                        TypeOfAny, UnionType, is_optional)
from mypy.version import __version__

from aexpy.extracting import Extractor
from aexpy.models import ApiDescription, Distribution
from aexpy.models.description import ApiEntry, ClassEntry, ModuleEntry


class MypyServer:
    def __init__(self, sources: "list[pathlib.Path]", logger: "logging.Logger | None" = None) -> None:
        self.options = Options()
        self.logger = logger.getChild(
            "mypy") if logger else logging.getLogger("mypy")
        self.files = find_sources.create_source_list(
            [str(s) for s in sources], self.options)
        self.logger.debug(f"Mypy sources: {self.files}")
        self.server = Server(self.options, DEFAULT_STATUS_FILE)
        self.prepared = False
        self.exception = None
        self.graph = None

    def prepare(self) -> None:
        if self.prepared:
            if self.exception is not None:
                raise self.exception
            else:
                assert self.graph is not None
            return

        self.prepared = True

        try:
            self.logger.info(f"Start mypy checking {datetime.now()}.")
            result = self.server.check(self.files, True, False, 0)
            # if self.server.fine_grained_manager is None and result["status"] == 2: # Compile Error
            #     for line in result["out"].splitlines():
            #         try:
            #             file = pathlib.Path(line.split(":")[0]).absolute().as_posix()
            #             filt = [f for f in self.files if pathlib.Path(f.path).as_posix() == str(file)]
            #             if len(filt) > 0:
            #                 self.files.remove(filt[0])
            #                 self.logger.info(f"Remove compiled failed file: {filt[0].path} ({line})")
            #         except:
            #             pass
            #     result = self.server.check(self.files, False, 0)

            self.logger.info(
                f"Finish mypy checking {datetime.now()}: {result}")
            assert self.server.fine_grained_manager
            self.graph = self.server.fine_grained_manager.graph
        except Exception as ex:
            self.graph = None
            self.exception = ex
            raise ex

    def module(self, file: "pathlib.Path") -> "State | None":
        filestr = file.absolute().as_posix()
        assert self.graph
        for v in self.graph.values():
            if not v.abspath:
                continue
            if pathlib.Path(v.abspath).absolute().as_posix() == filestr:
                return v

    def locals(self, module: "State") -> "dict[str, tuple[SymbolTableNode, TypeInfo | None]]":
        assert module.tree
        return {k: (node, typeInfo)
                for k, node, typeInfo in module.tree.local_definitions()}


_cached: "dict[str, MypyServer]" = {}
_cachedRank: "dict[str, int]" = {}
_currentCacheRank: "int" = 0


def getMypyServer(sources: "list[pathlib.Path]", id: "str" = "") -> MypyServer:
    global _cachedRank, _cached, _currentCacheRank

    _currentCacheRank += 1

    if not id:
        id = str(uuid1())

    if id in _cached:
        _cachedRank[id] = _currentCacheRank
    else:
        _cachedRank[id] = _currentCacheRank
        _cached[id] = MypyServer(sources)

    while len(_cached) > 10:
        mnrank = _currentCacheRank + 1
        mnitem = None
        for item in _cached:
            if _cachedRank[item] < mnrank:
                mnrank = _cachedRank[item]
                mnitem = item
        if mnitem:
            _cached.pop(mnitem)
            _cachedRank.pop(mnitem)
        else:
            break

    return _cached[id]


class PackageMypyServer:
    def __init__(self, unpacked: "pathlib.Path", paths: "list[pathlib.Path]", logger: "logging.Logger | None" = None) -> None:
        self.unpacked = unpacked
        self.proxy = MypyServer(paths, logger)
        self.logger = self.proxy.logger

    def prepare(self) -> None:
        self.cacheFile = {}
        self.cacheMembers = {}
        self.cacheElement = {}
        self.proxy.prepare()

    def file(self, entry: "ApiEntry") -> "State | None":
        assert entry.location
        if entry.location.file not in self.cacheFile:
            self.cacheFile[entry.location.file] = self.proxy.module(
                self.unpacked.joinpath(entry.location.file))
        return self.cacheFile[entry.location.file]

    def members(self, entry: "ClassEntry") -> "dict[str, SymbolTableNode]":
        if entry.id not in self.cacheMembers:
            mod = self.file(entry)

            result = {}

            if mod:
                for node, info in self.proxy.locals(mod).values():
                    if info is None:
                        continue
                    if node.fullname is None:
                        continue
                    if node.fullname.startswith(entry.id) and info.fullname == entry.id:
                        result[node.fullname.replace(
                            entry.id, "", 1).lstrip(".")] = node

            self.cacheMembers[entry.id] = result
        return self.cacheMembers[entry.id]

    def element(self, entry: "ApiEntry") -> "State | tuple[SymbolTableNode, TypeInfo | None] | None":
        if entry.id not in self.cacheElement:
            result = None
            mod = self.file(entry)
            if isinstance(entry, ModuleEntry):
                result = mod
            elif mod:
                result = self.proxy.locals(mod).get(entry.id)
            self.cacheElement[entry.id] = result
        return self.cacheElement[entry.id]


class MypyBasedIncrementalExtractor(Extractor):
    def processWithMypy(self, server: "PackageMypyServer", product: "ApiDescription", dist: "Distribution"):
        pass

    def processWithFallback(self, product: "ApiDescription", dist: "Distribution"):
        pass

    def basicProduce(self, dist: "Distribution", product: "ApiDescription"):
        pass

    def extract(self, dist: "Distribution", product: "ApiDescription"):
        with product.increment():
            self.basicProduce(dist, product)
        self.incrementalProcess(dist, product)

    def incrementalProcess(self, dist: "Distribution", product: "ApiDescription"):
        server = None
        assert dist.wheelDir

        try:
            server = PackageMypyServer(dist.wheelDir, dist.src, self.logger)
            server.prepare()
        except Exception as ex:
            self.logger.error(
                f"Failed to run mypy server at {dist.wheelDir}: {dist.src}.", exc_info=ex)
            server = None

        if server:
            self.processWithMypy(server, product, dist)
        else:
            self.processWithFallback(product, dist)
