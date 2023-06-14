import dataclasses
from typing import Any
from uuid import uuid1

from .typing import ApiTypeCompatibilityChecker
from aexpy.extracting.main.base import isprivateName
from aexpy.models import ApiDescription, ApiDifference
from aexpy.models.description import (EXTERNAL_ENTRYID, ApiEntry,
                                      AttributeEntry, ClassEntry,
                                      FunctionEntry, ItemScope, ModuleEntry,
                                      ParameterKind, SpecialEntry, SpecialKind)
from aexpy.models.difference import BreakingRank, DiffEntry
from aexpy.models.typing import AnyType, TypeFactory, UnknownType, NoneType, CallableType, copyType

from .checkers import EvalRule, EvalRuleCollection, forkind, rankAt, evalrule

RuleEvals = EvalRuleCollection()

AddModule = rankAt("AddModule", BreakingRank.Compatible)
RemoveModule = rankAt("RemoveModule", BreakingRank.High, BreakingRank.Low)
AddClass = rankAt("AddClass", BreakingRank.Compatible)
RemoveClass = rankAt("RemoveClass", BreakingRank.High, BreakingRank.Low)
AddBaseClass = rankAt("AddBaseClass", BreakingRank.Compatible)
RemoveBaseClass = rankAt(
    "RemoveBaseClass", BreakingRank.High, BreakingRank.Low)
ImplementAbstractBaseClass = rankAt(
    "ImplementAbstractBaseClass", BreakingRank.Compatible)
DeimplementAbstractBaseClass = rankAt(
    "DeimplementAbstractBaseClass", BreakingRank.High, BreakingRank.Low)
ChangeMethodResolutionOrder = rankAt(
    "ChangeMethodResolutionOrder", BreakingRank.Medium, BreakingRank.Low)
MoveParameter = rankAt(
    "MoveParameter", BreakingRank.High, BreakingRank.Low)

RuleEvals.rule(AddModule)
RuleEvals.rule(RemoveModule)
RuleEvals.rule(AddClass)
RuleEvals.rule(RemoveClass)
RuleEvals.rule(AddBaseClass)
RuleEvals.rule(ImplementAbstractBaseClass)
RuleEvals.rule(DeimplementAbstractBaseClass)
RuleEvals.rule(ChangeMethodResolutionOrder)
RuleEvals.rule(MoveParameter)


@RuleEvals.rule
@evalrule
def RemoveBaseClass(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    eold = entry.old
    enew = entry.new
    assert isinstance(eold, ClassEntry) and isinstance(enew, ClassEntry)
    name = entry.data["name"]
    if eold.private or enew.private or isprivateName(name):
        entry.rank = BreakingRank.Low
    else:
        entry.rank = BreakingRank.High


@RuleEvals.rule
@evalrule
def AddFunction(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    attr = entry.new
    assert isinstance(attr, FunctionEntry)

    entry.rank = BreakingRank.Compatible

    if attr.scope != ItemScope.Static:
        entry.kind = "AddMethod"
        entry.message = f"Add method ({attr.id.rsplit('.', 1)[0]}): {attr.name}"


@RuleEvals.rule
@evalrule
def RemoveFunction(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    attr = entry.old
    assert isinstance(attr, FunctionEntry)

    if attr.private:
        entry.rank = BreakingRank.Low
    else:
        entry.rank = BreakingRank.High

    if attr.scope != ItemScope.Static:
        entry.kind = "RemoveMethod"
        entry.message = f"Remove method ({attr.id.rsplit('.', 1)[0]}): {attr.name}"


@RuleEvals.rule
@evalrule
def AddAttribute(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    attr = entry.new
    assert isinstance(attr, AttributeEntry)

    entry.rank = BreakingRank.Compatible

    if attr.scope == ItemScope.Instance:
        entry.kind = "AddInstanceAttribute"
        entry.message = f"Add instance attribute ({attr.id.rsplit('.', 1)[0]}): {attr.name}"


@RuleEvals.rule
@evalrule
def RemoveAttribute(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    attr = entry.old
    assert isinstance(attr, AttributeEntry)

    if attr.private:
        entry.rank = BreakingRank.Low
    else:
        entry.rank = BreakingRank.High

    if attr.scope == ItemScope.Instance:
        entry.kind = "RemoveInstanceAttribute"
        entry.message = f"Remove instance attribute ({attr.id.rsplit('.', 1)[0]}): {attr.name}"


@RuleEvals.rule
@evalrule
def AddAlias(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    entry.rank = BreakingRank.Compatible
    name = entry.data["name"]
    target = new.entries.get(entry.data["target"])
    if target is None or (isinstance(target, SpecialEntry) and target.kind == SpecialKind.External):
        entry.kind = "AddExternalAlias"


@RuleEvals.rule
@evalrule
def RemoveAlias(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    assert entry.old is not None
    entry.rank = BreakingRank.High
    name = entry.data["name"]
    target = old.entries.get(entry.data["target"])
    if isprivateName(name) or entry.old.private:
        entry.rank = BreakingRank.Low

    if target is None or (isinstance(target, SpecialEntry) and target.kind == SpecialKind.External):
        entry.rank = BreakingRank.Low
        entry.kind = "RemoveExternalAlias"


@RuleEvals.rule
@evalrule
def ChangeAlias(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    entry.rank = BreakingRank.Unknown
    name = entry.data["name"]
    oldtarget = old.entries.get(entry.data["old"])
    newtarget = new.entries.get(entry.data["new"])

    if isprivateName(name):
        entry.rank = BreakingRank.Unknown

    if oldtarget is None or (isinstance(oldtarget, SpecialEntry) and oldtarget.kind == SpecialKind.External):
        if newtarget is None or (isinstance(newtarget, SpecialEntry) and newtarget.kind == SpecialKind.External):
            entry.rank = BreakingRank.Unknown
            entry.kind = "ChangeExternalAlias"


@RuleEvals.rule
@evalrule
def ChangeParameterDefault(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    fa = entry.old
    fb = entry.new
    assert isinstance(fa, FunctionEntry) and isinstance(fb, FunctionEntry)
    data = entry.data

    parent = old.entries.get(fa.parent)
    if isinstance(parent, ClassEntry):
        entry.rank = BreakingRank.Medium
    else:
        entry.rank = BreakingRank.Compatible

    if fa.private:
        entry.rank = min(entry.rank, BreakingRank.Low)


@RuleEvals.rule
@evalrule
def ChangeParameterOptional(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> "None":
    fa = entry.old
    fb = entry.new
    assert isinstance(fa, FunctionEntry) and isinstance(fb, FunctionEntry)
    data = entry.data

    if data["newoptional"]:
        entry.kind = "AddParameterDefault"
        parent = old.entries.get(fa.parent)
        if isinstance(parent, ClassEntry):
            entry.rank = BreakingRank.Medium
        else:
            entry.rank = BreakingRank.Compatible
    else:
        entry.kind = "RemoveParameterDefault"
        entry.rank = BreakingRank.High

    if fa.private:
        entry.rank = min(entry.rank, BreakingRank.Low)


@RuleEvals.rule
@evalrule
def AddParameter(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> None:
    data = entry.data
    fa = entry.old
    fb = entry.new
    assert isinstance(fa, FunctionEntry) and isinstance(fb, FunctionEntry)

    para = fb.getParameter(data["new"])
    assert para is not None

    if para.kind == ParameterKind.VarPositional:
        entry.kind = "AddVarPositional"
        entry.rank = BreakingRank.Compatible
    elif para.kind == ParameterKind.VarKeyword:
        entry.kind = "AddVarKeyword"
        entry.rank = BreakingRank.Compatible
    elif para.kind == ParameterKind.VarKeywordCandidate:
        if para.optional:
            entry.kind = "AddOptionalCandidate"
            entry.rank = BreakingRank.Compatible
        else:
            entry.kind = "AddRequiredCandidate"
            entry.rank = BreakingRank.Medium
    elif para.optional:
        entry.kind = "AddOptionalParameter"
        parent = old.entries.get(fa.parent)
        if isinstance(parent, ClassEntry):
            entry.rank = BreakingRank.Medium
        else:
            entry.rank = BreakingRank.Compatible
    else:
        entry.kind = "AddRequiredParameter"
        entry.rank = BreakingRank.High

    if fa.private:
        entry.rank = min(entry.rank, BreakingRank.Low)


@RuleEvals.rule
@evalrule
def RemoveParameter(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> None:
    data = entry.data
    fa = entry.old
    fb = entry.new
    assert isinstance(fa, FunctionEntry) and isinstance(fb, FunctionEntry)

    para = fa.getParameter(data["old"])
    assert para is not None

    entry.rank = BreakingRank.High

    if para.kind == ParameterKind.VarPositional:
        entry.kind = "RemoveVarPositional"
    elif para.kind == ParameterKind.VarKeyword:
        entry.kind = "RemoveVarKeyword"
    elif para.kind == ParameterKind.VarKeywordCandidate:
        if para.source == fa.id and not fb.transmitKwargs:
            # local use and no transmit kwargs
            entry.rank = BreakingRank.Compatible
        else:
            entry.rank = BreakingRank.Medium
        if para.optional:
            entry.kind = "RemoveOptionalCandidate"
        else:
            entry.kind = "RemoveRequiredCandidate"
    elif para.optional:
        entry.kind = "RemoveOptionalParameter"
    else:
        entry.kind = "RemoveRequiredParameter"

    if fa.private:
        entry.rank = min(entry.rank, BreakingRank.Low)


@RuleEvals.rule
@evalrule
def ChangeAttributeType(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> None:
    eold = entry.old
    enew = entry.new
    assert isinstance(eold, AttributeEntry) and isinstance(
        enew, AttributeEntry)
    assert eold.type is not None and enew.type is not None

    if eold.type.type is not None and enew.type.type is not None:
        result = ApiTypeCompatibilityChecker(
            new).isCompatibleTo(enew.type.type, eold.type.type)
        if result == True:
            entry.rank = BreakingRank.Compatible
        elif result == False:
            entry.rank = BreakingRank.Medium

    if eold.private:
        entry.rank = min(entry.rank, BreakingRank.Low)


@RuleEvals.rule
@evalrule
def ChangeReturnType(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> None:
    eold = entry.old
    enew = entry.new
    assert isinstance(eold, FunctionEntry) and isinstance(enew, FunctionEntry)
    assert eold.returnType is not None and enew.returnType is not None

    if eold.returnType.type is not None and enew.returnType.type is not None:
        told = eold.returnType.type

        if isinstance(told, NoneType):
            told = TypeFactory.any()

        result = ApiTypeCompatibilityChecker(new).isCompatibleTo(
            enew.returnType.type, told)
        if result == True:
            entry.rank = BreakingRank.Compatible
        elif result == False:
            entry.rank = BreakingRank.Medium

    if eold.private:
        entry.rank = min(entry.rank, BreakingRank.Low)


@RuleEvals.rule
@evalrule
def ChangeParameterType(entry: "DiffEntry", diff: "ApiDifference", old: "ApiDescription", new: "ApiDescription") -> None:
    eold = entry.old
    enew = entry.new
    assert isinstance(eold, FunctionEntry) and isinstance(enew, FunctionEntry)

    pold = eold.getParameter(entry.data["old"])
    pnew = enew.getParameter(entry.data["new"])

    assert pold is not None and pold.type is not None
    assert pnew is not None and pnew.type is not None

    if pold.type.type is not None and pnew.type.type is not None:
        tnew = copyType(pnew.type.type)

        if isinstance(tnew, CallableType):
            if isinstance(tnew.ret, NoneType):
                # a parameter: any -> none, is same as any -> any (ignore return means return any thing is ok)
                tnew.ret = TypeFactory.any()

        result = ApiTypeCompatibilityChecker(
            new).isCompatibleTo(pold.type.type, tnew)

        if result == True:
            entry.rank = BreakingRank.Compatible
        elif result == False:
            entry.rank = BreakingRank.Medium

    if eold.private:
        entry.rank = min(entry.rank, BreakingRank.Low)
