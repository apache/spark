#######################################################################
# Implements a topological sort algorithm.
#
# Copyright 2014 True Blade Systems, Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#
# Notes:
#  Based on http://code.activestate.com/recipes/578272-topological-sort
#   with these major changes:
#    Added unittests.
#    Deleted doctests (maybe not the best idea in the world, but it cleans
#     up the docstring).
#    Moved functools import to the top of the file.
#    Changed assert to a ValueError.
#    Changed iter[items|keys] to [items|keys], for python 3
#     compatibility. I don't think it matters for python 2 these are
#     now lists instead of iterables.
#    Copy the input so as to leave it unmodified.
#    Renamed function from toposort2 to toposort.
#    Handle empty input.
#    Switch tests to use set literals.
#
########################################################################

from functools import reduce as _reduce


__all__ = ['toposort', 'toposort_flatten']


def toposort(data):
    """Dependencies are expressed as a dictionary whose keys are items
and whose values are a set of dependent items. Output is a list of
sets in topological order. The first set consists of items with no
dependencies, each subsequent set consists of items that depend upon
items in the preceding sets.
"""

    # Special case empty input.
    if len(data) == 0:
        return

    # Copy the input so as to leave it unmodified.
    data = data.copy()

    # Ignore self dependencies.
    for k, v in data.items():
        v.discard(k)
    # Find all items that don't depend on anything.
    extra_items_in_deps = _reduce(set.union, data.values()) - set(data.keys())
    # Add empty dependencies where needed.
    data.update({item: set() for item in extra_items_in_deps})
    while True:
        ordered = set(item for item, dep in data.items() if len(dep) == 0)
        if not ordered:
            break
        yield ordered
        data = {item: (dep - ordered)
                for item, dep in data.items()
                if item not in ordered}
    if len(data) != 0:
        raise ValueError('Cyclic dependencies exist among these items: {}'.format(
            ', '.join(repr(x) for x in data.items())))


def toposort_flatten(data, sort=True):
    """Returns a single list of dependencies. For any set returned by
toposort(), those items are sorted and appended to the result (just to
make the results deterministic)."""

    result = []
    for d in toposort(data):
        result.extend((sorted if sort else list)(d))
    return result
