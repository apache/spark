from aexpy.models import ApiDescription
from aexpy.models.description import ClassEntry
from aexpy.models.difference import DiffEntry
from aexpy.utils import getObjectId

from ..checkers import DiffConstraint, DiffConstraintCollection, diffcons, fortype
from . import add, remove

ClassConstraints = DiffConstraintCollection()

AddClass = DiffConstraint("AddClass", add).fortype(ClassEntry, True)
RemoveClass = DiffConstraint("RemoveClass", remove).fortype(ClassEntry, True)

ClassConstraints.cons(AddClass)
ClassConstraints.cons(RemoveClass)


@ClassConstraints.cons
@fortype(ClassEntry)
@diffcons
def AddBaseClass(a: ClassEntry, b: ClassEntry, old: "ApiDescription", new: "ApiDescription"):
    sa = set(a.bases)
    sb = set(b.bases)
    plus = sb - sa - {getObjectId(object)}

    return [DiffEntry(message=f"Add base class ({a.id}): {name}", data={"name": name}) for name in plus if name not in a.mro]


@ClassConstraints.cons
@fortype(ClassEntry)
@diffcons
def RemoveBaseClass(a: ClassEntry, b: ClassEntry, old: "ApiDescription", new: "ApiDescription"):
    sa = set(a.bases)
    sb = set(b.bases)
    minus = sa - sb - {getObjectId(object)}

    return [DiffEntry(message=f"Remove base class ({a.id}): {name}", data={"name": name}) for name in minus if name not in b.mro]


@ClassConstraints.cons
@fortype(ClassEntry)
@diffcons
def ImplementAbstractBaseClass(a: ClassEntry, b: ClassEntry, old: "ApiDescription", new: "ApiDescription"):
    sa = set(a.abcs)
    sb = set(b.abcs)
    plus = sb - sa

    return [DiffEntry(message=f"Implement abstract base class ({a.id}): {name}", data={"name": name}) for name in plus]


@ClassConstraints.cons
@fortype(ClassEntry)
@diffcons
def DeimplementAbstractBaseClass(a: ClassEntry, b: ClassEntry, old: "ApiDescription", new: "ApiDescription"):
    sa = set(a.abcs)
    sb = set(b.abcs)

    minus = sa - sb
    return [DiffEntry(message=f"Deimplement abstract base class ({a.id}): {name}", data={"name": name}) for name in minus]


@ClassConstraints.cons
@fortype(ClassEntry)
@diffcons
def ChangeMethodResolutionOrder(a: ClassEntry, b: ClassEntry, old: "ApiDescription", new: "ApiDescription"):
    sa = a.mro
    sb = a.mro

    changed = False
    for i in range(len(sa)):
        if changed:
            break
        if i >= len(sb):
            changed = True
        elif sa[i] != sb[i]:
            changed = True

    if changed:
        return [DiffEntry(message=f"Change method resolution order ({a.id}): {sa} -> {sb}", data={"oldmro": sa, "newmro": sb})]
    return []
