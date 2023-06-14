from aexpy.models.description import ModuleEntry

from ..checkers import DiffConstraint, DiffConstraintCollection
from . import add, remove

ModuleConstraints = DiffConstraintCollection()

AddModule = DiffConstraint("AddModule", add).fortype(ModuleEntry, True)
RemoveModule = DiffConstraint("RemoveModule", remove).fortype(ModuleEntry, True)

ModuleConstraints.cons(AddModule)
ModuleConstraints.cons(RemoveModule)
