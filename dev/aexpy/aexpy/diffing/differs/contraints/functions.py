from aexpy.models.description import FunctionEntry

from ..checkers import DiffConstraint, DiffConstraintCollection
from . import add, remove

FunctionConstraints = DiffConstraintCollection()

AddFunction = DiffConstraint("AddFunction", add).fortype(FunctionEntry, True)
RemoveFunction = DiffConstraint(
    "RemoveFunction", remove).fortype(FunctionEntry, True)

FunctionConstraints.cons(AddFunction)
FunctionConstraints.cons(RemoveFunction)
