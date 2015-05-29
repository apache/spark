'''
Created on Mar 24, 2010

@author: Barthelemy Dagenais
'''
from __future__ import unicode_literals, absolute_import

from multiprocessing import Process
import subprocess
import time
import unittest

from py4j.java_gateway import JavaGateway
from py4j.tests.java_gateway_test import PY4J_JAVA_PATH


def start_example_server():
    subprocess.call(["java", "-cp", PY4J_JAVA_PATH,
        "py4j.examples.ExampleApplication"])


def start_example_app_process():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_example_server)
    p.start()
    return p


class Test(unittest.TestCase):
    def setUp(self):
#        logger = logging.getLogger("py4j")
#        logger.setLevel(logging.DEBUG)
#        logger.addHandler(logging.StreamHandler())
        self.p = start_example_app_process()
        time.sleep(0.5)
        self.gateway = JavaGateway()

    def tearDown(self):
        self.p.terminate()
        self.gateway.shutdown()
        time.sleep(0.5)

    def testArray(self):
        example = self.gateway.entry_point.getNewExample()
        array1 = example.getStringArray()
        array2 = example.getIntArray()
        self.assertEqual(3, len(array1))
        self.assertEqual(4, len(array2))

        self.assertEqual('333', array1[2])
        self.assertEqual(5, array2[1])

        array1[2] = 'aaa'
        array2[1] = 6
        self.assertEqual('aaa', array1[2])
        self.assertEqual(6, array2[1])

        new_array = array2[1:3]
        self.assertEqual(2, len(new_array))
        self.assertEqual(1, new_array[1])

    def testCreateArray(self):
        int_class = self.gateway.jvm.int
        string_class = self.gateway.jvm.java.lang.String
        int_array = self.gateway.new_array(int_class, 2)
        string_array = self.gateway.new_array(string_class, 3, 5)
        self.assertEqual(2, len(int_array))
        self.assertEqual(3, len(string_array))
        self.assertEqual(5, len(string_array[0]))


if __name__ == "__main__":
    unittest.main()
