# -*- coding: UTF-8 -*-
'''
Created on Dec 10, 2009

@author: barthelemy
'''
from __future__ import unicode_literals, absolute_import

from decimal import Decimal
import gc
from multiprocessing import Process
import os
from socket import AF_INET, SOCK_STREAM, socket
import subprocess
from threading import Thread
import time
from traceback import print_exc
import unittest

from py4j.compat import range, isbytearray, bytearray2, long
from py4j.finalizer import ThreadSafeFinalizer
from py4j.java_gateway import JavaGateway, JavaMember, get_field, get_method, \
     GatewayClient, set_field, java_import, JavaObject, is_instance_of
from py4j.protocol import *


SERVER_PORT = 25333
TEST_PORT = 25332
PY4J_JAVA_PATH = os.path.join(os.path.dirname(os.path.realpath(__file__)),
    '../../../../py4j-java/bin')


def start_echo_server():
    subprocess.call(["java", "-cp", PY4J_JAVA_PATH, "py4j.EchoServer"])


def start_echo_server_process():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_echo_server)
    p.start()
    return p


def start_example_server():
    subprocess.call(["java", "-Xmx512m", "-cp", PY4J_JAVA_PATH,
        "py4j.examples.ExampleApplication"])


def start_example_app_process():
    # XXX DO NOT FORGET TO KILL THE PROCESS IF THE TEST DOES NOT SUCCEED
    p = Process(target=start_example_server)
    p.start()
    return p


def get_socket():
    testSocket = socket(AF_INET, SOCK_STREAM)
    testSocket.connect(('127.0.0.1', TEST_PORT))
    return testSocket


def safe_shutdown(instance):
    try:
        instance.gateway.shutdown()
    except Exception:
        print_exc()


class TestConnection(object):
    """Connection that does nothing. Useful for testing."""

    counter = -1

    def __init__(self, return_message='yro'):
        self.address = '127.0.0.1'
        self.port = 1234
        self.return_message = return_message
        self.is_connected = True

    def start(self):
        pass

    def stop(self):
        pass

    def send_command(self, command):
        TestConnection.counter += 1
        if not command.startswith('m\nd\n'):
            self.last_message = command
        return self.return_message + str(TestConnection.counter)


class ProtocolTest(unittest.TestCase):
    def tearDown(self):
        # Safety check in case there was an exception...
        safe_shutdown(self)

    def testEscape(self):
        self.assertEqual("Hello\t\rWorld\n\\", unescape_new_line(
            escape_new_line("Hello\t\rWorld\n\\")))
        self.assertEqual("Hello\t\rWorld\n\\", unescape_new_line(
            escape_new_line("Hello\t\rWorld\n\\")))

    def testProtocolSend(self):
        testConnection = TestConnection()
        self.gateway = JavaGateway(testConnection, False)
        e = self.gateway.getExample()
        self.assertEqual('c\nt\ngetExample\ne\n', testConnection.last_message)
        e.method1(1, True, 'Hello\nWorld', e, None, 1.5)
        self.assertEqual(
                'c\no0\nmethod1\ni1\nbTrue\nsHello\\nWorld\nro0\nn\nd1.5\ne\n',
                testConnection.last_message)
        del(e)

    def testProtocolReceive(self):
        p = start_echo_server_process()
        time.sleep(1)
        try:
            testSocket = get_socket()
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('yro0\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('ysHello World\n'.encode('utf-8'))
            # No extra echange (method3) because it is already cached.
            testSocket.sendall('yi123\n'.encode('utf-8'))
            testSocket.sendall('yd1.25\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('yn\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('ybTrue\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('yL123\n'.encode('utf-8'))
            testSocket.close()
            time.sleep(1)

            self.gateway = JavaGateway(auto_field=True)
            ex = self.gateway.getNewExample()
            self.assertEqual('Hello World', ex.method3(1, True))
            self.assertEqual(123, ex.method3())
            self.assertAlmostEqual(1.25, ex.method3())
            self.assertTrue(ex.method2() is None)
            self.assertTrue(ex.method4())
            self.assertEqual(long(123), ex.method8())
            self.gateway.shutdown()

        except Exception:
            print_exc()
            self.fail('Problem occurred')
        p.join()


class IntegrationTest(unittest.TestCase):
    def setUp(self):
        self.p = start_echo_server_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)

    def tearDown(self):
        # Safety check in case there was an exception...
        safe_shutdown(self)
        self.p.join()

    def testIntegration(self):
        try:
            testSocket = get_socket()
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('yro0\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('ysHello World\n'.encode('utf-8'))
            testSocket.sendall('yro1\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('ysHello World2\n'.encode('utf-8'))
            testSocket.close()
            time.sleep(1)

            self.gateway = JavaGateway(auto_field=True)
            ex = self.gateway.getNewExample()
            response = ex.method3(1, True)
            self.assertEqual('Hello World', response)
            ex2 = self.gateway.entry_point.getNewExample()
            response = ex2.method3(1, True)
            self.assertEqual('Hello World2', response)
            self.gateway.shutdown()
        except Exception:
            self.fail('Problem occurred')

    def testException(self):
        try:
            testSocket = get_socket()
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall('yro0\n'.encode('utf-8'))
            testSocket.sendall('yo\n'.encode('utf-8'))
            testSocket.sendall(b'x\n')
            testSocket.close()
            time.sleep(1)

            self.gateway = JavaGateway(auto_field=True)
            ex = self.gateway.getNewExample()

            self.assertRaises(Py4JError, lambda: ex.method3(1, True))
            self.gateway.shutdown()
        except Exception:
            self.fail('Problem occurred')


class CloseTest(unittest.TestCase):
    def testNoCallbackServer(self):
        # Test that the program can continue to move on and that no close
        # is required.
        JavaGateway()
        self.assertTrue(True)

    def testCallbackServer(self):
        # A close is required to stop the thread.
        gateway = JavaGateway(start_callback_server=True)
        gateway.close()
        self.assertTrue(True)
        time.sleep(1)


class MethodTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testNoneArg(self):
        ex = self.gateway.getNewExample()
        try:
            ex.method2(None)
            ex2 = ex.method4(None)
            self.assertEquals(ex2.getField1(), 3)
            self.assertEquals(2, ex.method7(None))
        except Exception:
            print_exc()
            self.fail()

    def testUnicode(self):
        sb = self.gateway.jvm.java.lang.StringBuffer()
        sb.append('\r\n\tHello\r\n\t')
        self.assertEqual('\r\n\tHello\r\n\t', sb.toString())

    def testEscape(self):
        sb = self.gateway.jvm.java.lang.StringBuffer()
        sb.append('\r\n\tHello\r\n\t')
        self.assertEqual('\r\n\tHello\r\n\t', sb.toString())


class FieldTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testAutoField(self):
        self.gateway = JavaGateway(auto_field=True)
        ex = self.gateway.getNewExample()
        self.assertEqual(ex.field10, 10)
        self.assertEqual(ex.field11, long(11))
        sb = ex.field20
        sb.append('Hello')
        self.assertEqual('Hello', sb.toString())
        self.assertTrue(ex.field21 == None)

    def testNoField(self):
        self.gateway = JavaGateway(auto_field=True)
        ex = self.gateway.getNewExample()
        member = ex.field50
        self.assertTrue(isinstance(member, JavaMember))

    def testNoAutoField(self):
        self.gateway = JavaGateway(auto_field=False)
        ex = self.gateway.getNewExample()
        self.assertTrue(isinstance(ex.field10, JavaMember))
        self.assertTrue(isinstance(ex.field50, JavaMember))
        self.assertEqual(10, get_field(ex, 'field10'))

        # This field does not exist
        self.assertRaises(Exception, get_field, ex, 'field50')

        # With auto field = True
        ex._auto_field = True
        sb = ex.field20
        sb.append('Hello')
        self.assertEqual('Hello', sb.toString())


    def testSetField(self):
        self.gateway = JavaGateway(auto_field=False)
        ex = self.gateway.getNewExample()

        set_field(ex, 'field10', 2334)
        self.assertEquals(get_field(ex, 'field10'), 2334)

        sb = self.gateway.jvm.java.lang.StringBuffer('Hello World!')
        set_field(ex, 'field21', sb)
        self.assertEquals(get_field(ex, 'field21').toString(), 'Hello World!')

        self.assertRaises(Exception, set_field, ex, 'field1', 123)

    def testGetMethod(self):
        # This is necessary if a field hides a method...
        self.gateway = JavaGateway()
        ex = self.gateway.getNewExample()
        self.assertEqual(1, get_method(ex, 'method1')())


class UtilityTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testIsInstance(self):
        a_list = self.gateway.jvm.java.util.ArrayList()
        a_map = self.gateway.jvm.java.util.HashMap()

        # FQN
        self.assertTrue(is_instance_of(self.gateway, a_list, "java.util.List"))
        self.assertFalse(is_instance_of(self.gateway, a_list, "java.lang.String"))

        # JavaClass
        self.assertTrue(is_instance_of(self.gateway, a_list,
            self.gateway.jvm.java.util.List))
        self.assertFalse(is_instance_of(self.gateway, a_list,
            self.gateway.jvm.java.lang.String))

        # JavaObject
        self.assertTrue(is_instance_of(self.gateway, a_list, a_list))
        self.assertFalse(is_instance_of(self.gateway, a_list, a_map))


class MemoryManagementTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()
        gc.collect()

    def testNoAttach(self):
        self.gateway = JavaGateway()
        gateway2 = JavaGateway()
        sb = self.gateway.jvm.java.lang.StringBuffer()
        sb.append('Hello World')
        self.gateway.shutdown()

        self.assertRaises(Exception, lambda : sb.append('Python'))

        self.assertRaises(Exception,
                lambda : gateway2.jvm.java.lang.StringBuffer())

    def testDetach(self):
        self.gateway = JavaGateway()
        gc.collect()
        finalizers_size_start = len(ThreadSafeFinalizer.finalizers)

        sb = self.gateway.jvm.java.lang.StringBuffer()
        sb.append('Hello World')
        self.gateway.detach(sb)
        sb2 = self.gateway.jvm.java.lang.StringBuffer()
        sb2.append('Hello World')
        sb2._detach()
        gc.collect()

        self.assertEqual(len(ThreadSafeFinalizer.finalizers) -
                finalizers_size_start, 0)
        self.gateway.shutdown()


class TypeConversionTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testLongInt(self):
        ex = self.gateway.getNewExample()
        self.assertEqual(1, ex.method7(1234))
        self.assertEqual(4, ex.method7(2147483648))
        self.assertEqual(4, ex.method7(long(2147483648)))
        self.assertEqual(long(4), ex.method8(3))
        self.assertEqual(4, ex.method8(3))
        self.assertEqual(long(4), ex.method8(long(3)))
        self.assertEqual(long(4), ex.method9(long(3)))

    def testBigDecimal(self):
        ex = self.gateway.getNewExample()
        self.assertEqual(Decimal("2147483.647"), ex.method10(2147483647, 3))
        self.assertEqual(Decimal("-13.456"), ex.method10(Decimal("-14.456")))


class UnicodeTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    #def testUtfMethod(self):
        #ex = self.gateway.jvm.py4j.examples.UTFExample()

        ## Only works for Python 3
        #self.assertEqual(2, ex.strangeMéthod())

    def testUnicodeString(self):
        # NOTE: this is unicode because of import future unicode literal...
        ex = self.gateway.jvm.py4j.examples.UTFExample()
        s1 = 'allo'
        s2 = 'alloé'
        array1 = ex.getUtfValue(s1)
        array2 = ex.getUtfValue(s2)
        self.assertEqual(len(s1), len(array1))
        self.assertEqual(len(s2), len(array2))
        self.assertEqual(ord(s1[0]), array1[0])
        self.assertEqual(ord(s2[4]), array2[4])


class ByteTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testJavaByteConversion(self):
        ex = self.gateway.jvm.py4j.examples.UTFExample()
        ba = bytearray([0, 1, 127, 128, 255, 216, 1, 220])
        self.assertEqual(0, ex.getPositiveByteValue(ba[0]))
        self.assertEqual(1, ex.getPositiveByteValue(ba[1]))
        self.assertEqual(127, ex.getPositiveByteValue(ba[2]))
        self.assertEqual(128, ex.getPositiveByteValue(ba[3]))
        self.assertEqual(255, ex.getPositiveByteValue(ba[4]))
        self.assertEqual(216, ex.getPositiveByteValue(ba[5]))
        self.assertEqual(0, ex.getJavaByteValue(ba[0]))
        self.assertEqual(1, ex.getJavaByteValue(ba[1]))
        self.assertEqual(127, ex.getJavaByteValue(ba[2]))
        self.assertEqual(-128, ex.getJavaByteValue(ba[3]))
        self.assertEqual(-1, ex.getJavaByteValue(ba[4]))

    def testProtocolConversion(self):
        #b1 = tobytestr('abc\n')
        b2 = bytearray([1, 2, 3, 255, 0, 128, 127])

        #encoded1 = encode_bytearray(b1)
        encoded2 = encode_bytearray(b2)

        #self.assertEqual(b1, decode_bytearray(encoded1))
        self.assertEqual(b2, decode_bytearray(encoded2))

    def testBytesType(self):
        ex = self.gateway.jvm.py4j.examples.UTFExample()
        int_list = [0, 1, 10, 127, 128, 255]
        ba1 = bytearray(int_list)
        # Same for Python2, bytes for Python 3
        ba2 = bytearray2(int_list)
        a1 = ex.getBytesValue(ba1)
        a2 = ex.getBytesValue(ba2)
        for i1, i2 in zip(a1, int_list):
            self.assertEqual(i1, i2)

        for i1, i2 in zip(a2, int_list):
            self.assertEqual(i1, i2)

    def testBytesType2(self):
        ex = self.gateway.jvm.py4j.examples.UTFExample()
        int_list = [0, 1, 10, 127, 255, 128]
        a1 = ex.getBytesValue()
        # Python 2: bytearray (because str is too easy to confuse with normal
        # strings)
        # Python 3: bytes (because bytes is closer to the byte[] representation
        # in Java)
        self.assertTrue(isbytearray(a1) or ispython3bytestr(a1))
        for i1, i2 in zip(a1, int_list):
            self.assertEqual(i1, i2)

    def testLargeByteArray(self):
        # Regression test for #109, an error when passing large byte arrays.
        self.gateway.jvm.java.nio.ByteBuffer.wrap(bytearray(range(255)))


class ExceptionTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testJavaError(self):
        try:
            self.gateway.jvm.Integer.valueOf('allo')
        except Py4JJavaError as e:
            self.assertEqual('java.lang.NumberFormatException',
                    e.java_exception.getClass().getName())
        except Exception:
            self.fail()

    def testJavaConstructorError(self):
        try:
            self.gateway.jvm.Integer('allo')
        except Py4JJavaError as e:
            self.assertEqual('java.lang.NumberFormatException',
                    e.java_exception.getClass().getName())
        except Exception:
            self.fail()

    def doError(self):
        id = ''
        try:
            self.gateway.jvm.Integer.valueOf('allo')
        except Py4JJavaError as e:
            id = e.java_exception._target_id
        return id

    def testJavaErrorGC(self):
        id = self.doError()
        java_object = JavaObject(id, self.gateway._gateway_client)
        try:
            # Should fail because it should have been garbage collected...
            java_object.getCause()
            self.fail()
        except Py4JError:
            self.assertTrue(True)

    def testReflectionError(self):
        try:
            self.gateway.jvm.Integer.valueOf2('allo')
        except Py4JJavaError:
            self.fail()
        except Py4JNetworkError:
            self.fail()
        except Py4JError:
            self.assertTrue(True)

    def testStrError(self):
        try:
            self.gateway.jvm.Integer.valueOf('allo')
        except Py4JJavaError as e:
            self.assertTrue(str(e).startswith(
                'An error occurred while calling z:java.lang.Integer.valueOf.'
                '\n: java.lang.NumberFormatException:'))
        except Exception:
            self.fail()


class JVMTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testConstructors(self):
        jvm = self.gateway.jvm
        sb = jvm.java.lang.StringBuffer('hello')
        sb.append('hello world')
        sb.append(1)
        self.assertEqual(sb.toString(), 'hellohello world1')

        l1 = jvm.java.util.ArrayList()
        l1.append('hello world')
        l1.append(1)
        self.assertEqual(2, len(l1))
        self.assertEqual('hello world', l1[0])
        l2 = ['hello world', 1]
        self.assertEqual(str(l2), str(l1))

    def testStaticMethods(self):
        System = self.gateway.jvm.java.lang.System
        self.assertTrue(System.currentTimeMillis() > 0)
        self.assertEqual('123', self.gateway.jvm.java.lang.String.valueOf(123))

    def testStaticFields(self):
        Short = self.gateway.jvm.java.lang.Short
        self.assertEqual(-32768, Short.MIN_VALUE)
        System = self.gateway.jvm.java.lang.System
        self.assertFalse(System.out.checkError())

    def testDefaultImports(self):
        self.assertTrue(self.gateway.jvm.System.currentTimeMillis() > 0)
        self.assertEqual('123', self.gateway.jvm.String.valueOf(123))

    def testNone(self):
        ex = self.gateway.entry_point.getNewExample()
        ex.method4(None)

    def testJVMView(self):
        newView = self.gateway.new_jvm_view('myjvm')
        time = newView.System.currentTimeMillis()
        self.assertTrue(time > 0)
        time = newView.java.lang.System.currentTimeMillis()
        self.assertTrue(time > 0)

    def testImport(self):
        newView = self.gateway.new_jvm_view('myjvm')
        java_import(self.gateway.jvm, 'java.util.*')
        java_import(self.gateway.jvm, 'java.io.File')
        self.assertTrue(self.gateway.jvm.ArrayList() is not None)
        self.assertTrue(self.gateway.jvm.File('hello.txt') is not None)
        self.assertRaises(Exception, lambda : newView.File('test.txt'))

        java_import(newView, 'java.util.HashSet')
        self.assertTrue(newView.HashSet() is not None)

    def testEnum(self):
        self.assertEqual('FOO', str(self.gateway.jvm.py4j.examples.Enum2.FOO))

    def testInnerClass(self):
        self.assertEqual('FOO',
            str(self.gateway.jvm.py4j.examples.EnumExample.MyEnum.FOO))
        self.assertEqual('HELLO2',
            self.gateway.jvm.py4j.examples.EnumExample.InnerClass.MY_CONSTANT2)


class HelpTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        self.gateway = JavaGateway()

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testHelpObject(self):
        ex = self.gateway.getNewExample()
        help_page = self.gateway.help(ex, short_name=True, display=False)
        #print(help_page)
        self.assertTrue(len(help_page) > 1)

    def testHelpObjectWithPattern(self):
        ex = self.gateway.getNewExample()
        help_page = self.gateway.help(ex, pattern='m*', short_name=True,
                display=False)
        #print(help_page)
        self.assertTrue(len(help_page) > 1)

    def testHelpClass(self):
        String = self.gateway.jvm.java.lang.String
        help_page = self.gateway.help(String, short_name=False, display=False)
        #print(help_page)
        self.assertTrue(len(help_page) > 1)
        self.assertTrue("String" in help_page)


class Runner(Thread):
    def __init__(self, runner_range, gateway):
        Thread.__init__(self)
        self.range = runner_range
        self.gateway = gateway
        self.ok = True

    def run(self):
        ex = self.gateway.getNewExample()
        for i in self.range:
            try:
                l = ex.getList(i)
                if len(l) != i:
                    self.ok = False
                    break
                self.gateway.detach(l)
#                gc.collect()
            except Exception:
                self.ok = False
                break


class ThreadTest(unittest.TestCase):
    def setUp(self):
        self.p = start_example_app_process()
        # This is to ensure that the server is started before connecting to it!
        time.sleep(1)
        gateway_client = GatewayClient()
        self.gateway = JavaGateway(gateway_client=gateway_client)

    def tearDown(self):
        safe_shutdown(self)
        self.p.join()

    def testStress(self):
        # Real stress test!
#        runner1 = Runner(xrange(1,10000,2),self.gateway)
#        runner2 = Runner(xrange(1000,1000000,10000), self.gateway)
#        runner3 = Runner(xrange(1000,1000000,10000), self.gateway)
        # Small stress test
        runner1 = Runner(range(1, 10000, 1000), self.gateway)
        runner2 = Runner(range(1000, 1000000, 100000), self.gateway)
        runner3 = Runner(range(1000, 1000000, 100000), self.gateway)
        runner1.start()
        runner2.start()
        runner3.start()
        runner1.join()
        runner2.join()
        runner3.join()
        self.assertTrue(runner1.ok)
        self.assertTrue(runner2.ok)
        self.assertTrue(runner3.ok)


class GatewayLauncherTest(unittest.TestCase):
    def tearDown(self):
        safe_shutdown(self)

    def testDefaults(self):
        self.gateway = JavaGateway.launch_gateway()
        self.assertTrue(self.gateway.jvm)

    def testJavaopts(self):
        self.gateway = JavaGateway.launch_gateway(javaopts=["-Xmx64m"])
        self.assertTrue(self.gateway.jvm)


if __name__ == "__main__":
    unittest.main()
