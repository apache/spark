'''
Created on Mar 7, 2010

@author: barthelemy
'''
from __future__ import unicode_literals, absolute_import

import gc
import unittest
from weakref import ref

from py4j.finalizer import ThreadSafeFinalizer, Finalizer, clear_finalizers


def deleted(accumulator, id):
    print(id)
    accumulator.acc += 1


class Accumulator(object):
    def __init__(self):
        self.acc = 0


class AClass(object):
    def __init__(self, id, acc):
        self.id = id
        self.acc = acc
        ThreadSafeFinalizer.add_finalizer(id,
                ref(self, lambda wr, i=self.id, a=self.acc: deleted(a, i)))


class AClass2(object):
    def __init__(self, id, acc):
        self.id = id
        self.acc = acc
        Finalizer.add_finalizer(id,
                ref(self, lambda wr, i=self.id, a=self.acc: deleted(a, i)))


class JavaObjecTest(object):
    def __init__(self, id, acc):
        self.id = id
        self.acc = acc
        self.methods = []
        ThreadSafeFinalizer.add_finalizer(id,
                ref(self, lambda wr, i=self.id, a=self.acc: deleted(a, i)))


class JavaMemberTest(object):
    def __init__(self, name, container):
        self.name = name
        self.container = container


class TestThreadSafeFinalizer(unittest.TestCase):
    def tearDown(self):
        clear_finalizers(True)

    def work1(self, acc):
        a1 = AClass(1, acc)
        a2 = AClass(2, acc)

    def work1b(self, acc):
        a1 = AClass(1, acc)
        a2 = AClass(2, acc)
        a1.foo = a2
        a2.foo = a1

    def work2(self, acc):
        a1 = AClass(1, acc)
        a2 = AClass(2, acc)
        return a1

    def testFinalizer(self):
        acc = Accumulator()
        self.work1(acc)
        self.assertEqual(2, acc.acc)
        self.work2(acc)
        self.assertEqual(4, acc.acc)

    def work_circ(self, acc):
        jobj = JavaObjecTest(1, acc)
        jmem1 = JavaMemberTest('append', jobj)
        jobj.methods.append(jmem1)

    def testCircularReference2(self):
        acc = Accumulator()
        self.work_circ(acc)
        # Necessary because of circular references...
        gc.collect()
        self.assertEqual(1, acc.acc)

    def testCleanUp(self):
        acc = Accumulator()
        a1 = self.work2(acc)
        self.assertEqual(1, acc.acc)
        self.assertEqual(2, len(ThreadSafeFinalizer.finalizers))
        clear_finalizers(False)
        self.assertEqual(1, acc.acc)
        self.assertEqual(1, len(ThreadSafeFinalizer.finalizers))
        a1.foo = "hello"
        del(a1)
        self.assertEqual(2, acc.acc)
        clear_finalizers(False)
        self.assertEqual(0, len(ThreadSafeFinalizer.finalizers))

    def testCleanUpAll(self):
        acc = Accumulator()
        a1 = self.work2(acc)
        self.assertEqual(1, acc.acc)
        self.assertEqual(2, len(ThreadSafeFinalizer.finalizers))
        clear_finalizers(True)
        self.assertEqual(1, acc.acc)
        self.assertEqual(0, len(ThreadSafeFinalizer.finalizers))
        a1.foo = "hello"
        del(a1)
        self.assertEqual(1, acc.acc)


class TestFinalizer(unittest.TestCase):
    def tearDown(self):
        clear_finalizers(True)

    def work1(self, acc):
        a1 = AClass2(1, acc)
        a2 = AClass2(2, acc)

    def work2(self, acc):
        a1 = AClass2(1, acc)
        a2 = AClass2(2, acc)
        return a1

    def testFinalizer(self):
        acc = Accumulator()
        self.work1(acc)
        self.assertEqual(2, acc.acc)

    def testCleanUp(self):
        acc = Accumulator()
        a1 = self.work2(acc)
        self.assertEqual(1, acc.acc)
        self.assertEqual(2, len(Finalizer.finalizers))
        clear_finalizers(False)
        self.assertEqual(1, acc.acc)
        self.assertEqual(1, len(Finalizer.finalizers))
        a1.foo = "hello"
        del(a1)
        self.assertEqual(2, acc.acc)
        clear_finalizers(False)
        self.assertEqual(0, len(Finalizer.finalizers))

    def testCleanUpAll(self):
        acc = Accumulator()
        a1 = self.work2(acc)
        self.assertEqual(1, acc.acc)
        self.assertEqual(2, len(Finalizer.finalizers))
        clear_finalizers(True)
        self.assertEqual(1, acc.acc)
        self.assertEqual(0, len(Finalizer.finalizers))
        a1.foo = "hello"
        del(a1)
        self.assertEqual(1, acc.acc)

if __name__ == "__main__":
    unittest.main()
