# -*- coding: UTF-8 -*-
from __future__ import absolute_import

import unittest
import time

from py4j.java_gateway import JavaGateway
from py4j.protocol import smart_decode
from py4j.tests.java_gateway_test import start_example_app_process,\
        safe_shutdown


class ByteStringTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testByteString(self):
        # NOTE: this is only a byte string if executed by Python 2.
        ex = self.gateway.jvm.py4j.examples.UTFExample()
        s1 = 'allo'
        s2 = smart_decode('alloé')
        array1 = ex.getUtfValue(s1)
        array2 = ex.getUtfValue(s2)
        self.assertEqual(len(s1), len(array1))
        self.assertEqual(len(s2), len(array2))
        self.assertEqual(ord(s1[0]), array1[0])
        self.assertEqual(ord(s2[4]), array2[4])

if __name__ == "__main__":
#    logger = logging.getLogger("py4j")
#    logger.setLevel(logging.DEBUG)
#    logger.addHandler(logging.StreamHandler())
    unittest.main()
