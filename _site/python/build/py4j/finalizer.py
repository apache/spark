# -*- coding: UTF-8 -*-
"""
Module that defines a Finalizer class responsible for registering and cleaning
finalizer

Created on Mar 7, 2010

:author: Barthelemy Dagenais
"""
from __future__ import unicode_literals, absolute_import

from threading import RLock

from py4j.compat import items


class ThreadSafeFinalizer(object):
    """A `ThreadSafeFinalizer` is a global class used to register weak reference finalizers
    (i.e., a weak reference with a callback).

    This class is useful when one wants to register a finalizer of an object with circular references.
    The finalizer of an object with circular references might never be called if the object's finalizer
    is kept by the same object.

    For example, if object A refers to B and B refers to A, A should not keep a weak reference to itself.

    `ThreadSafeFinalizer` is thread-safe and uses reentrant lock on each operation."""

    finalizers = {}
    lock = RLock()

    @classmethod
    def add_finalizer(cls, id, weak_ref):
        """Registers a finalizer with an id.

        :param id: The id of the object referenced by the weak reference.
        :param weak_ref: The weak reference to register.
        """
        with cls.lock:
            cls.finalizers[id] = weak_ref

    @classmethod
    def remove_finalizer(cls, id):
        """Removes a finalizer associated with this id.

        :param id: The id of the object for which the finalizer will be deleted.
        """
        with cls.lock:
            cls.finalizers.pop(id, None)

    @classmethod
    def clear_finalizers(cls, clear_all=False):
        """Removes all registered finalizers.

        :param clear_all: If `True`, all finalizers are deleted. Otherwise, only the finalizers from
                          an empty weak reference are deleted (i.e., weak references pointing to
                          inexistent objects).
        """
        with cls.lock:
            if clear_all:
                cls.finalizers.clear()
            else:
                for id, ref in items(cls.finalizers):
                    if ref() is None:
                        cls.finalizers.pop(id, None)


class Finalizer(object):
    """A `Finalizer` is a global class used to register weak reference finalizers
    (i.e., a weak reference with a callback).

    This class is useful when one wants to register a finalizer of an object with circular references.
    The finalizer of an object with circular references might never be called if the object's finalizer
    is kept by the same object.

    For example, if object A refers to B and B refers to A, A should not keep a weak reference to itself.

    `Finalizer` is not thread-safe and should only be used by single-threaded programs."""

    finalizers = {}

    @classmethod
    def add_finalizer(cls, id, weak_ref):
        """Registers a finalizer with an id.

        :param id: The id of the object referenced by the weak reference.
        :param weak_ref: The weak reference to register.
        """
        cls.finalizers[id] = weak_ref

    @classmethod
    def remove_finalizer(cls, id):
        """Removes a finalizer associated with this id.

        :param id: The id of the object for which the finalizer will be deleted.
        """
        cls.finalizers.pop(id, None)

    @classmethod
    def clear_finalizers(cls, clear_all=False):
        """Removes all registered finalizers.

        :param clear_all: If `True`, all finalizers are deleted. Otherwise, only the finalizers from
                          an empty weak reference are deleted (i.e., weak references pointing to
                          inexistent objects).
        """
        if clear_all:
            cls.finalizers.clear()
        else:
            for id, ref in items(cls.finalizers):
                if ref() is None:
                    cls.finalizers.pop(id, None)


def clear_finalizers(clear_all=False):
    """Removes all registered finalizers in :class:`ThreadSafeFinalizer` and :class:`Finalizer`.

    :param clear_all: If `True`, all finalizers are deleted. Otherwise, only the finalizers from
                      an empty weak reference are deleted (i.e., weak references pointing to
                      inexistent objects).
    """
    ThreadSafeFinalizer.clear_finalizers(clear_all)
    Finalizer.clear_finalizers(clear_all)
