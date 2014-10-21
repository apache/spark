# -*- coding: UTF-8 -*-
'''
Module responsible for converting Java collection classes to Python collection
classes. This module is optional but loaded by default.


Created on Jan 22, 2010

:author: Barthelemy Dagenais
'''
from __future__ import unicode_literals, absolute_import

from collections import MutableMapping, Sequence, MutableSequence,\
        MutableSet, Set
import sys

from py4j.compat import iteritems, next, hasattr2
from py4j.java_gateway import JavaObject, JavaMember, get_method, JavaClass
from py4j.protocol import *


class JavaIterator(JavaObject):
    """Maps a Python list iterator to a Java list iterator.

    The `JavaIterator` follows the Python iterator protocol and raises a
    `StopIteration` error when the iterator can no longer iterate."""
    def __init__(self, target_id, gateway_client):
        JavaObject.__init__(self, target_id, gateway_client)
        self._next_name = 'next'
        # To bind lifecycle of this iterator to the java iterator. To prevent
        # gc of the iterator.

    def __iter__(self):
        return self

    def next(self):
        """This next method wraps the `next` method in Java iterators.

        The `Iterator.next()` method is called and if an exception occur (e.g.,
        NoSuchElementException), a StopIteration exception is raised."""
        if self._next_name not in self._methods:
            self._methods[self._next_name] = JavaMember(self._next_name, self,
                    self._target_id, self._gateway_client)
        try:
            return self._methods[self._next_name]()
        except Py4JError:
            raise StopIteration()

    __next__ = next


class JavaMap(JavaObject, MutableMapping):
    """Maps a Python Dictionary to a Java Map.

    All operations possible on a Python dict are implemented."""

    def __init__(self, target_id, gateway_client):
        JavaObject.__init__(self, target_id, gateway_client)
        self._get = get_method(self, 'get')

    def __getitem__(self, key):
        return self._get(key)

    def __setitem__(self, key, value):
        self.put(key, value)

    def __len__(self):
        return self.size()

    def __delitem__(self, key):
        self.remove(key)

    def __iter__(self):
        return self.keySet().iterator()

    def __contains__(self, key):
        return self.containsKey(key)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        items = ('{0}: {1}'.format(repr(k), repr(v))
                for k, v in iteritems(self))
        return '{{{0}}}'.format(', '.join(items))


class JavaSet(JavaObject, MutableSet):
    """Maps a Python Set to a Java Set.

    All operations possible on a Python set are implemented."""

    __EMPTY_SET = 'set([])' if sys.version_info[0] < 3 else 'set()'
    __SET_TEMPLATE = 'set([{0}])' if sys.version_info[0] < 3 else '{{{0}}}'

    def __init__(self, target_id, gateway_client):
        JavaObject.__init__(self, target_id, gateway_client)
        self._add = get_method(self, 'add')
        self._clear = get_method(self, 'clear')
        self._remove = get_method(self, 'remove')

    def add(self, value):
        self._add(value)

    def discard(self, value):
        self.remove(value)

    def remove(self, value):
        if value not in self:
            raise KeyError()
        else:
            self._remove(value)

    def clear(self):
        self._clear()

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.iterator()

    def __contains__(self, value):
        return self.contains(value)

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        if len(self):
            return self.__SET_TEMPLATE.format(', '.join(
                (repr(x) for x in self)))
        return self.__EMPTY_SET


class JavaArray(JavaObject, Sequence):
    """Maps a Java Array to a Semi-Mutable Sequence: elements inside the
    sequence can be modified, but the length of the sequence cannot change.

    The backing collection is a Sequence and not a Python array because
    these arrays only accept primitives whereas Java arrays work for any types.
    """

    def __init__(self, target_id, gateway_client):
        JavaObject.__init__(self, target_id, gateway_client)

    def __compute_index(self, key, adjustLast=False):
        size = len(self)
        if 0 <= key < size:
            return key
        elif key < 0 and abs(key) <= size:
            return size + key
        elif adjustLast:
            return size
        else:
            raise IndexError("list index out of range")

    def __compute_item(self, key):
        new_key = self.__compute_index(key)
        command = ARRAY_COMMAND_NAME + ARRAY_GET_SUB_COMMAND_NAME + \
                self._get_object_id() + '\n'
        command += get_command_part(new_key)
        command += END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def __get_slice(self, indices):
        command = ARRAY_COMMAND_NAME + ARRAY_SLICE_SUB_COMMAND_NAME + \
                self._get_object_id() + '\n'
        for index in indices:
            command += get_command_part(index)
        command += END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def __getitem__(self, key):
        if isinstance(key, slice):
            indices = key.indices(len(self))
            return self.__get_slice(range(*indices))
        elif isinstance(key, int):
            return self.__compute_item(key)
        else:
            raise TypeError("array indices must be integers, not {0}".format(
                key.__class__.__name__))

    def __repl_item_from_slice(self, range, iterable):
        value_iter = iter(iterable)
        for i in range:
            value = next(value_iter)
            self.__set_item(i, value)

    def __set_item(self, key, value):
        new_key = self.__compute_index(key)
        command = ARRAY_COMMAND_NAME + ARRAY_SET_SUB_COMMAND_NAME + \
                self._get_object_id() + '\n'
        command += get_command_part(new_key)
        command += get_command_part(value)
        command += END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def __setitem__(self, key, value):
        if isinstance(key, slice):
            self_len = len(self)
            indices = key.indices(self_len)
            self_range = range(*indices)
            lenr = len(self_range)
            lenv = len(value)
            if lenr != lenv:
                raise ValueError("attempt to assign sequence of size "
                        "{0} to extended slice of size {1}".format(lenv, lenr))
            else:
                return self.__repl_item_from_slice(self_range, value)

        elif isinstance(key, int):
            return self.__set_item(key, value)
        else:
            raise TypeError("list indices must be integers, not {0}".format(
                key.__class__.__name__))

    def __len__(self):
        command = ARRAY_COMMAND_NAME + ARRAY_LEN_SUB_COMMAND_NAME + \
                self._get_object_id() + '\n'
        command += END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)


class JavaList(JavaObject, MutableSequence):
    """Maps a Python list to a Java list.

    All operations possible on a Python list are implemented. For example,
    slicing (e.g., list[1:3]) will create a copy of the list on the JVM.
    Slicing is thus not equivalent to subList(), because a modification to a
    slice such as the addition of a new element will not affect the original
    list."""

    def __init__(self, target_id, gateway_client):
        JavaObject.__init__(self, target_id, gateway_client)
        self.java_remove = get_method(self, 'remove')

    def __len__(self):
        return self.size()

    def __iter__(self):
        return self.iterator()

    def __compute_index(self, key, adjustLast=False):
        size = self.size()
        if 0 <= key < size:
            return key
        elif key < 0 and abs(key) <= size:
            return size + key
        elif adjustLast:
            return size
        else:
            raise IndexError("list index out of range")

    def __compute_item(self, key):
        new_key = self.__compute_index(key)
        return self.get(new_key)

    def __set_item(self, key, value):
        new_key = self.__compute_index(key)
        self.set(new_key, value)

    def __set_item_from_slice(self, indices, iterable):
        offset = 0
        last = 0
        value_iter = iter(iterable)

        # First replace and delete if from_slice > to_slice
        for i in range(*indices):
            try:
                value = next(value_iter)
                self.__set_item(i, value)
            except StopIteration:
                self.__del_item(i)
                offset -= 1
            last = i + 1

        # Then insert if from_slice < to_slice
        for elem in value_iter:
            self.insert(last, elem)
            last += 1

    def __insert_item_from_slice(self, indices, iterable):
        index = indices[0]
        for elem in iterable:
            self.insert(index, elem)
            index += 1

    def __repl_item_from_slice(self, range, iterable):
        value_iter = iter(iterable)
        for i in range:
            value = value = next(value_iter)
            self.__set_item(i, value)

    def __append_item_from_slice(self, range, iterable):
        for value in iterable:
            self.append(value)

    def __del_item(self, key):
        new_key = self.__compute_index(key)
        self.java_remove(new_key)

    def __setitem__(self, key, value):
        if isinstance(key, slice):
            self_len = len(self)
            indices = key.indices(self_len)
            if indices[0] >= self_len:
                self.__append_item_from_slice(range, value)
            elif indices[0] == indices[1]:
                self.__insert_item_from_slice(indices, value)
            elif indices[2] == 1:
                self.__set_item_from_slice(indices, value)
            else:
                self_range = range(*indices)
                lenr = len(self_range)
                lenv = len(value)
                if lenr != lenv:
                    raise ValueError("attempt to assign sequence of size "
                        "{0} to extended slice of size {1}".format(lenv, lenr))
                else:
                    return self.__repl_item_from_slice(self_range, value)

        elif isinstance(key, int):
            return self.__set_item(key, value)
        else:
            raise TypeError("list indices must be integers, not {0}".format(
                key.__class__.__name__))

    def __get_slice(self, indices):
        command = LIST_COMMAND_NAME + LIST_SLICE_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n'
        for index in indices:
            command += get_command_part(index)
        command += END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def __getitem__(self, key):
        if isinstance(key, slice):
            indices = key.indices(len(self))
            return self.__get_slice(range(*indices))
        elif isinstance(key, int):
            return self.__compute_item(key)
        else:
            raise TypeError("list indices must be integers, not {0}".format(
                key.__class__.__name__))

    def __delitem__(self, key):
        if isinstance(key, slice):
            indices = key.indices(len(self))
            offset = 0
            for i in range(*indices):
                self.__del_item(i + offset)
                offset -= 1
        elif isinstance(key, int):
            return self.__del_item(key)
        else:
            raise TypeError("list indices must be integers, not {0}".format(
                key.__class__.__name__))

    def __contains__(self, item):
        return self.contains(item)

    def __add__(self, other):
        command = LIST_COMMAND_NAME + LIST_CONCAT_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n' + other._get_object_id() + \
                '\n' + END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def __radd__(self, other):
        return self.__add__(other)

    def __iadd__(self, other):
        self.extend(other)
        return self

    def __mul__(self, other):
        command = LIST_COMMAND_NAME + LIST_MULT_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n' + get_command_part(other) + \
                END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def __rmul__(self, other):
        return self.__mul__(other)

    def __imul__(self, other):
        command = LIST_COMMAND_NAME + LIST_IMULT_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n' + get_command_part(other) + \
                END_COMMAND_PART
        self._gateway_client.send_command(command)
        return self

    def append(self, value):
        self.add(value)

    def insert(self, key, value):
        if isinstance(key, int):
            new_key = self.__compute_index(key, True)
            return self.add(new_key, value)
        else:
            raise TypeError("list indices must be integers, not {0}".format(
                key.__class__.__name__))

    def extend(self, other_list):
        self.addAll(other_list)

    def pop(self, key=None):
        if key == None:
            new_key = self.size() - 1
        else:
            new_key = self.__compute_index(key)
        return self.java_remove(new_key)

    def index(self, value):
        return self.indexOf(value)

    def count(self, value):
        command = LIST_COMMAND_NAME + LIST_COUNT_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n' + get_command_part(value) + \
                END_COMMAND_PART
        answer = self._gateway_client.send_command(command)
        return get_return_value(answer, self._gateway_client)

    def sort(self):
        command = LIST_COMMAND_NAME + LIST_SORT_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n' + END_COMMAND_PART
        self._gateway_client.send_command(command)

    def reverse(self):
        command = LIST_COMMAND_NAME + LIST_REVERSE_SUBCOMMAND_NAME + \
                self._get_object_id() + '\n' + END_COMMAND_PART
        self._gateway_client.send_command(command)

    def remove(self, value):
        # Ensures that we are deleting the int value and not the index
        # (Java API)
        if isinstance(value, int):
            new_value = self.indexOf(value)
        else:
            new_value = value
        success = self.java_remove(new_value)
        if not success:
            raise ValueError('java_list.remove(x): x not in java_list')

    def __str__(self):
        return self.__repr__()

    def __repr__(self):
        items = (repr(x) for x in self)
        return '[{0}]'.format(', '.join(items))


class SetConverter(object):
    def can_convert(self, object):
        return isinstance(object, Set)

    def convert(self, object, gateway_client):
        JavaSet = JavaClass('java.util.HashSet', gateway_client)
        java_set = JavaSet()
        for element in object:
            java_set.add(element)
        return java_set


class ListConverter(object):
    def can_convert(self, object):
        return hasattr2(object, '__iter__')

    def convert(self, object, gateway_client):
        ArrayList = JavaClass('java.util.ArrayList', gateway_client)
        java_list = ArrayList()
        for element in object:
            java_list.add(element)
        return java_list


class MapConverter(object):
    def can_convert(self, object):
        return hasattr2(object, 'keys') and hasattr2(object, '__getitem__')

    def convert(self, object, gateway_client):
        HashMap = JavaClass('java.util.HashMap', gateway_client)
        java_map = HashMap()
        for key in object.keys():
            java_map[key] = object[key]
        return java_map

register_input_converter(SetConverter())
register_input_converter(MapConverter())
register_input_converter(ListConverter())

register_output_converter(MAP_TYPE, lambda target_id, gateway_client:
        JavaMap(target_id, gateway_client))
register_output_converter(LIST_TYPE, lambda target_id, gateway_client:
        JavaList(target_id, gateway_client))
register_output_converter(ARRAY_TYPE, lambda target_id, gateway_client:
        JavaArray(target_id, gateway_client))
register_output_converter(SET_TYPE, lambda target_id, gateway_client:
        JavaSet(target_id, gateway_client))
register_output_converter(ITERATOR_TYPE, lambda target_id, gateway_client:
        JavaIterator(target_id, gateway_client))
