'''
Created on Apr 5, 2010

@author: Barthelemy Dagenais
'''
from __future__ import unicode_literals, absolute_import

from multiprocessing import Process
import subprocess
from threading import Thread
import time
from traceback import print_exc
import unittest

from py4j.compat import range
from py4j.java_gateway import JavaGateway, PythonProxyPool
from py4j.tests.java_gateway_test import PY4J_JAVA_PATH, safe_shutdown


def start_example_server():
    subprocess.call(["java", "-cp", PY4J_JAVA_PATH,
        "py4j.examples.ExampleApplication"])


def start_example_server2():
    subprocess.call(["java", "-cp", PY4J_JAVA_PATH,
        "py4j.examples.OperatorExampleTest"])


def start_example_server3():
    subprocess.call(["java", "-Xmx512m", "-cp", PY4J_JAVA_PATH,
        "py4j.examples.InterfaceExample"])


def start_example_app_process():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_example_server)
    p.start()
    return p


def start_example_app_process2():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_example_server2)
    p.start()
    return p


def start_example_app_process3():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_example_server3)
    p.start()
    return p


class FalseAddition(object):
    def doOperation(self, i, j, k=None):
        if k == None:
            # Integer overflow!
            return 3722507311
        else:
            return 3722507311

    class Java:
        implements = ['py4j.examples.Operator']


class GoodAddition(object):
    def doOperation(self, i, j):
        return i + j

    class Java:
        implements = ['py4j.examples.Operator']


class CustomBytesOperator(object):
    def returnBytes(self, byte_array):
        try:
            b = bytearray()
            for abyte in byte_array:
                b.append(abyte + 1)
            return b
        except Exception:
            print_exc()
            return None

    class Java:
        implements = ['py4j.examples.BytesOperator']


class Runner(Thread):
    def __init__(self, runner_range, pool):
        Thread.__init__(self)
        self.range = runner_range
        self.pool = pool
        self.ok = True

    def run(self):
        for i in self.range:
            try:
                id = self.pool.put(i)
                self.ok = id in self.pool and self.pool[id] == i
                if not self.ok:
                    break
            except Exception as e:
                print(e)
                self.ok = False
                break


class TestPool(unittest.TestCase):

    def testPool(self):
        pool = PythonProxyPool()
        runners = [Runner(range(0, 10000), pool) for _ in range(0, 3)]
        for runner in runners:
            runner.start()

        for runner in runners:
            runner.join()

        for runner in runners:
            self.assertTrue(runner.ok)


class SimpleProxy(object):
    def hello(self, i, j):
        return 'Hello\nWorld' + str(i) + str(j)


class IHelloImpl(object):
    def sayHello(self, i=None, s=None):
        if i == None:
            return 'This is Hello!'
        else:
            return 'This is Hello;\n{0}{1}'.format(i, s)

    class Java:
        implements = ['py4j.examples.IHello']


class TestIntegration(unittest.TestCase):
    def setUp(self):
#        logger = logging.getLogger("py4j")
#        logger.setLevel(logging.DEBUG)
#        logger.addHandler(logging.StreamHandler())
        time.sleep(1)
        self.p = start_example_app_process()
        time.sleep(1)
        self.gateway = JavaGateway(start_callback_server=True)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()
        time.sleep(1)

#    Does not work when combined with other tests... because of TCP_WAIT
    def testShutdown(self):
        example = self.gateway.entry_point.getNewExample()
        impl = IHelloImpl()
        self.assertEqual('This is Hello!', example.callHello(impl))
        self.assertEqual('This is Hello;\n10MyMy!\n;',
                example.callHello2(impl))
        self.gateway.shutdown()
        self.assertEqual(0, len(self.gateway.gateway_property.pool))

    def testProxy(self):
#        self.gateway.jvm.py4j.GatewayServer.turnLoggingOn()
        time.sleep(1)
        example = self.gateway.entry_point.getNewExample()
        impl = IHelloImpl()
        self.assertEqual('This is Hello!', example.callHello(impl))
        self.assertEqual('This is Hello;\n10MyMy!\n;',
                example.callHello2(impl))

    def testGC(self):
        # This will only work with some JVM.
        time.sleep(1)
        example = self.gateway.entry_point.getNewExample()
        impl = IHelloImpl()
        self.assertEqual('This is Hello!', example.callHello(impl))
        self.assertEqual('This is Hello;\n10MyMy!\n;',
                example.callHello2(impl))
        self.assertEqual(2, len(self.gateway.gateway_property.pool))
        self.gateway.jvm.java.lang.System.gc()
        time.sleep(2)
        self.assertTrue(len(self.gateway.gateway_property.pool) < 2)

    def testDoubleCallbackServer(self):
        try:
            self.gateway2 = JavaGateway(start_callback_server=True)
            self.fail()
        except Exception:
            self.assertTrue(True)

    def testMethodConstructor(self):
        time.sleep(1)
        goodAddition = GoodAddition()
        oe1 = self.gateway.jvm.py4j.examples.OperatorExample()
        # Test method
        oe1.randomBinaryOperator(goodAddition)
        # Test constructor
        oe2 = self.gateway.jvm.py4j.examples.OperatorExample(goodAddition)
        self.assertTrue(oe2 is not None)


class TestPeriodicCleanup(unittest.TestCase):
    def setUp(self):
#        logger = logging.getLogger("py4j")
#        logger.setLevel(logging.DEBUG)
#        logger.addHandler(logging.StreamHandler())
        time.sleep(1)
        self.p = start_example_app_process2()
        time.sleep(1)
        self.gateway = JavaGateway(start_callback_server=True)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()
        time.sleep(1)

    def testPeriodicCleanup(self):
        operator = FalseAddition()
        self.assertRaises(
            Exception, self.gateway.entry_point.randomTernaryOperator,
            operator)
        # Time for periodic cleanup
        time.sleep(5)
        self.assertRaises(
            Exception, self.gateway.entry_point.randomTernaryOperator,
            operator)

    def testBytes(self):
        time.sleep(1)
        operator = CustomBytesOperator()
        returnbytes = self.gateway.entry_point.callBytesOperator(operator)
        self.assertEqual(2, returnbytes[0])
        self.assertEqual(6, returnbytes[-1])
        time.sleep(2)


class A(object):
    class Java:
        implements = ['py4j.examples.InterfaceA']


class B(object):
    def getA(self):
        return A()

    class Java:
        implements = ['py4j.examples.InterfaceB']


class InterfaceTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process3()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway(start_callback_server=True)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testByteString(self):
        try:
            self.gateway.entry_point.test(B())
        except Exception:
            print_exc()
            self.fail()


if __name__ == "__main__":
    unittest.main()
