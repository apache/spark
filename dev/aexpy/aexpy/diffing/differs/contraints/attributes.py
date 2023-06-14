from aexpy.models.description import AttributeEntry

from ..checkers import DiffConstraint, DiffConstraintCollection
from . import add, remove

AttributeConstraints = DiffConstraintCollection()

AddAttribute = DiffConstraint(
    "AddAttribute", add).fortype(AttributeEntry, True)
RemoveAttribute = DiffConstraint(
    "RemoveAttribute", remove).fortype(AttributeEntry, True)

AttributeConstraints.cons(AddAttribute)
AttributeConstraints.cons(RemoveAttribute)
