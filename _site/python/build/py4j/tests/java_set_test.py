'''
Created on Mar 26, 2010

@author: Barthelemy Dagenais
'''
from __future__ import unicode_literals, absolute_import

from multiprocessing import Process
import subprocess
import time
import unittest

from py4j.java_gateway import JavaGateway
from py4j.tests.java_gateway_test import PY4J_JAVA_PATH, safe_shutdown


def start_example_server():
    subprocess.call(["java", "-cp", PY4J_JAVA_PATH,
        "py4j.examples.ExampleApplication"])


def start_example_app_process():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_example_server)
    p.start()
    return p


class AutoConvertTest(unittest.TestCase):
    def setUp(self):
#        logger = logging.getLogger("py4j")
#        logger.setLevel(logging.DEBUG)
#        logger.addHandler(logging.StreamHandler())
        self.p = start_example_app_process()
        time.sleep(0.5)
        self.gateway = JavaGateway(auto_convert=True)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()
        time.sleep(0.5)

    def testAutoConvert(self):
        sj = self.gateway.jvm.java.util.HashSet()
        sj.add('b')
        sj.add(1)
        sp = set([1, 'b'])
        self.assertTrue(sj.equals(sp))


class Test(unittest.TestCase):
    def setUp(self):
#        logger = logging.getLogger("py4j")
#        logger.setLevel(logging.DEBUG)
#        logger.addHandler(logging.StreamHandler())
        self.p = start_example_app_process()
        time.sleep(0.5)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()
        time.sleep(0.5)

    def testTreeSet(self):
#        self.gateway.jvm.py4j.GatewayServer.turnLoggingOn()
        set1 = set()
        set2 = self.gateway.jvm.java.util.TreeSet()
        set1.add('a')
        set2.add('a')
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual(repr(set1), repr(set2))

        set1.add('b')
        set2.add('b')
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual('b' in set1, 'b' in set2)
        # not a good assumption with Python 3.3. Oh dear.
        #self.assertEqual(repr(set1), repr(set2))

        set1.remove('a')
        set2.remove('a')
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual('b' in set1, 'b' in set2)
        #self.assertEqual(repr(set1), repr(set2))

        set1.clear()
        set2.clear()
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual('b' in set1, 'b' in set2)
        #self.assertEqual(repr(set1), repr(set2))

    def testHashSet(self):
        set1 = set()
        set2 = self.gateway.jvm.java.util.HashSet()
        set1.add('a')
        set2.add('a')
        set1.add(1)
        set2.add(1)
        set1.add('b')
        set2.add('b')
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual('b' in set1, 'b' in set2)
        self.assertEqual(1 in set1, 1 in set2)

        set1.remove(1)
        set2.remove(1)
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual('b' in set1, 'b' in set2)
        self.assertEqual(1 in set1, 1 in set2)

        set1.clear()
        set2.clear()
        self.assertEqual(len(set1), len(set2))
        self.assertEqual('a' in set1, 'a' in set2)
        self.assertEqual('b' in set1, 'b' in set2)
        self.assertEqual(1 in set1, 1 in set2)


if __name__ == "__main__":
    unittest.main()
