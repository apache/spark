import ast
import logging
from ast import NodeVisitor
from dataclasses import asdict

from mypy.nodes import NameExpr

from aexpy.models import ApiDescription
from aexpy.models.description import (ApiEntry, ClassEntry, FunctionEntry, ItemScope,
                                      Parameter, ParameterKind)

from . import Enricher, callgraph, clearSrc


def _try_addkwc_parameter(entry: "FunctionEntry", parameter: "Parameter", logger: "logging.Logger"):
    """Return if add successfully"""
    # logger.debug(f"Try add parameter {parameter.name} to {entry.id}({[p.name for p in entry.parameters]}).")
    if len([x for x in entry.parameters if x.name == parameter.name]) != 0:  # has same name parameter
        return False
    data = asdict(parameter)
    data.pop("kind")
    entry.parameters.append(
        Parameter(kind=ParameterKind.VarKeywordCandidate, **data))
    logger.debug(f"Detect candidate {entry.id}: {parameter.name}")
    return True


class KwargAliasGetter(NodeVisitor):
    def __init__(self, entry: "FunctionEntry", logger: "logging.Logger") -> None:
        super().__init__()
        self.logger = logger
        self.entry = entry
        self.kwarg = entry.varKeyword.name
        self.alias = {self.kwarg}

    def is_rvalue_kwargs(self, rvalue) -> "bool":
        match rvalue:
            case ast.Name() as name:
                return name.id in self.alias
            case ast.IfExp() as ife:
                return self.is_rvalue_kwargs(ife.body) or self.is_rvalue_kwargs(ife.orelse)
            case ast.Call() as call:
                # for case newkw = oldkw.copy()
                if isinstance(call.func, ast.Attribute):
                    if call.func.attr == "copy":
                        return self.is_rvalue_kwargs(call.func.value)
        return False
    
    # for case newkw.update(oldkw)
    def visit_Call(self, node: "ast.Call"):
        func = node.func
        args = node.args
        if isinstance(func, ast.Attribute):
            if func.attr == "update":
                test = False
                for arg in args:
                    test = test or self.is_rvalue_kwargs(arg)
                match func.value:
                    case ast.Name() as name:
                        self.alias.add(name.id)
        super().generic_visit(node)

    def visit_Assign(self, node: "ast.Assign"):
        if self.is_rvalue_kwargs(node.value) and len(node.targets) == 1:
            target = node.targets[0]
            match target:
                case ast.Name() as name:
                    self.alias.add(name.id)
        super().generic_visit(node)

    def visit_AnnAssign(self, node: "ast.AnnAssign"):
        if self.is_rvalue_kwargs(node.value):
            match node.target:
                case ast.Name() as name:
                    self.alias.add(name.id)
        super().generic_visit(node)

    def visit_AugAssign(self, node: "ast.AugAssign"):
        if self.is_rvalue_kwargs(node.value):
            match node.target:
                case ast.Name() as name:
                    self.alias.add(name.id)
        super().generic_visit(node)

    def visit_NamedExpr(self, node: "ast.NamedExpr"):
        if self.is_rvalue_kwargs(node.value):
            match node.target:
                case ast.Name() as name:
                    self.alias.add(name.id)
        super().generic_visit(node)


class KwargChangeGetter(NodeVisitor):
    def __init__(self, result: "FunctionEntry", kwargs: "set[str]", logger: "logging.Logger") -> None:
        super().__init__()
        self.logger = logger
        self.result = result
        self.kwargs = kwargs

    def add(self, name: "str"):
        _try_addkwc_parameter(self.result, Parameter(
            name=name, optional=True, source=self.result.id), self.logger)

    def visit_Call(self, node: "ast.Call"):
        try:
            method = None

            match node.func:
                # kwargs.<method> call
                case ast.Attribute(value=ast.Name() as name) as attr if name.id in self.kwargs:
                    method = attr.attr

            if method in {"get", "pop", "setdefault"}:
                arg = node.args[0]
                match arg:
                    # kwargs.get("abc")
                    case ast.Constant(value=str()):
                        self.add(arg.value)
        except Exception as ex:
            self.logger.error(
                f"Failed to detect in call {ast.unparse(node)}", exc_info=ex)

    def visit_Subscript(self, node: "ast.Subscript"):
        try:
            match node:
                # kwargs["abc"]
                case ast.Subscript(value=ast.Name() as name, slice=ast.Constant(value=str())) if name.id in self.kwargs:
                    self.add(node.slice.value)
        except Exception as ex:
            self.logger.error(
                f"Failed to detect in subscript {ast.unparse(node)}", exc_info=ex)


class KwargsEnricher(Enricher):
    def __init__(self, cg: "callgraph.Callgraph", logger: "logging.Logger | None" = None) -> None:
        super().__init__()
        self.logger = logger.getChild("kwargs-enrich") if logger is not None else logging.getLogger(
            "kwargs-enrich")
        self.cg = cg
        self.kwargAlias = {}

    def enrich(self, api: "ApiDescription"):
        self.enrichByDictChange(api)
        # self.enrichByCallgraph(api)

    def enrichByDictChange(self, api: "ApiDescription"):
        for func in api.funcs.values():
            if func.varKeyword:
                src = clearSrc(func.src)
                try:
                    astree = ast.parse(src)
                except Exception as ex:
                    self.logger.error(
                        f"Failed to parse code from {func.id}:\n{src}", exc_info=ex)
                    continue
                alias = KwargAliasGetter(func, self.logger)
                alias.visit(astree)
                self.kwargAlias[func.id] = alias.alias
                KwargChangeGetter(func, alias.alias, self.logger).visit(astree)

    def enrichByCallgraph(self, api: "ApiDescription"):
        cg = self.cg

        cycle = 0

        changed = True

        while changed and cycle < 100:
            changed = False
            cycle += 1

            self.logger.info(f"Cycle {cycle} to enrich by callgraph.")

            for caller in cg.items.values():
                callerEntry: FunctionEntry = api.entries[caller.id]

                kwarg = callerEntry.varKeyword

                if kwarg is None:
                    continue

                kwargNames = self.kwargAlias.get(callerEntry.id)
                if kwargNames is None:
                    kwargNames = [kwarg.name]

                for site in caller.sites:
                    hasKwargsRef = False
                    ignoredPosition = set()
                    ignoredKeyword = set()

                    for index, arg in enumerate(site.arguments):
                        if arg.iskwargs:
                            match arg.value:
                                # has **kwargs argument
                                case ast.Name() as name if name.id in kwargNames:
                                    hasKwargsRef = True
                                    break
                                case NameExpr() as mname if mname.name in kwargNames:
                                    hasKwargsRef = True
                                    break
                        else:
                            if arg.name:
                                ignoredKeyword.add(arg.name)
                            else:
                                ignoredPosition.add(index)

                    callerEntry.transmitKwargs = callerEntry.transmitKwargs or hasKwargsRef

                    if not hasKwargsRef:
                        continue

                    for target in site.targets:
                        targetEntry = api.entries.get(target)

                        if not isinstance(targetEntry, FunctionEntry):
                            continue

                        # ignore magic methods
                        if targetEntry.name.startswith("__") and targetEntry.name != "__init__":
                            continue

                        self.logger.debug(
                            f"Enrich by call edge: {callerEntry.id}({[p.name for p in callerEntry.parameters]}) -> {targetEntry.id}({[p.name for p in targetEntry.parameters]})")

                        for index, arg in enumerate(targetEntry.parameters[(0 if targetEntry.scope == ItemScope.Static else 1):]):
                            if index in ignoredPosition:
                                continue
                            if arg.name in ignoredKeyword:
                                continue
                            if not arg.isKeyword:
                                continue
                            changed = _try_addkwc_parameter(
                                callerEntry, arg, self.logger) or changed

        if changed:
            self.logger.warning(
                f"Too many change cycles to enrich kwargs: {cycle} cycles")
        else:
            self.logger.info(
                f"Kwargs enrichment finished in {cycle} cycles")
