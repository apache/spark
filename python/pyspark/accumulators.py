#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""
>>> from pyspark.context import SparkContext
>>> sc = SparkContext('local', 'test')
>>> a = sc.accumulator(1)
>>> a.value
1
>>> a.value = 2
>>> a.value
2
>>> a += 5
>>> a.value
7

>>> sc.accumulator(1.0).value
1.0

>>> sc.accumulator(1j).value
1j

>>> rdd = sc.parallelize([1,2,3])
>>> def f(x):
...     global a
...     a += x
>>> rdd.foreach(f)
>>> a.value
13

>>> b = sc.accumulator(0)
>>> def g(x):
...     b.add(x)
>>> rdd.foreach(g)
>>> b.value
6

>>> from pyspark.accumulators import AccumulatorParam
>>> class VectorAccumulatorParam(AccumulatorParam):
...     def zero(self, value):
...         return [0.0] * len(value)
...     def addInPlace(self, val1, val2):
...         for i in range(len(val1)):
...              val1[i] += val2[i]
...         return val1
>>> va = sc.accumulator([1.0, 2.0, 3.0], VectorAccumulatorParam())
>>> va.value
[1.0, 2.0, 3.0]
>>> def g(x):
...     global va
...     va += [x] * 3
>>> rdd.foreach(g)
>>> va.value
[7.0, 8.0, 9.0]

>>> rdd.map(lambda x: a.value).collect() # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Py4JJavaError:...

>>> def h(x):
...     global a
...     a.value = 7
>>> rdd.foreach(h) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
Py4JJavaError:...

>>> sc.accumulator([1.0, 2.0, 3.0]) # doctest: +IGNORE_EXCEPTION_DETAIL
Traceback (most recent call last):
    ...
TypeError:...
"""

import sys
import select
import struct
if sys.version < '3':
    import SocketServer
else:
    import socketserver as SocketServer
import threading
from pyspark.cloudpickle import CloudPickler
from pyspark.serializers import read_int, PickleSerializer


__all__ = ['Accumulator', 'AccumulatorParam']


pickleSer = PickleSerializer()

# Holds accumulators registered on the current machine, keyed by ID. This is then used to send
# the local accumulator updates back to the driver program at the end of a task.
_accumulatorRegistry = {}


def _deserialize_accumulator(aid, zero_value, accum_param):
    from pyspark.accumulators import _accumulatorRegistry
    accum = Accumulator(aid, zero_value, accum_param)
    accum._deserialized = True
    _accumulatorRegistry[aid] = accum
    return accum


class Accumulator(object):

    """
    A shared variable that can be accumulated, i.e., has a commutative and associative "add"
    operation. Worker tasks on a Spark cluster can add values to an Accumulator with the C{+=}
    operator, but only the driver program is allowed to access its value, using C{value}.
    Updates from the workers get propagated automatically to the driver program.

    While C{SparkContext} supports accumulators for primitive data types like C{int} and
    C{float}, users can also define accumulators for custom types by providing a custom
    L{AccumulatorParam} object. Refer to the doctest of this module for an example.
    """

    def __init__(self, aid, value, accum_param):
        """Create a new Accumulator with a given initial value and AccumulatorParam object"""
        from pyspark.accumulators import _accumulatorRegistry
        self.aid = aid
        self.accum_param = accum_param
        self._value = value
        self._deserialized = False
        _accumulatorRegistry[aid] = self

    def __reduce__(self):
        """Custom serialization; saves the zero value from our AccumulatorParam"""
        param = self.accum_param
        return (_deserialize_accumulator, (self.aid, param.zero(self._value), param))

    @property
    def value(self):
        """Get the accumulator's value; only usable in driver program"""
        if self._deserialized:
            raise Exception("Accumulator.value cannot be accessed inside tasks")
        return self._value

    @value.setter
    def value(self, value):
        """Sets the accumulator's value; only usable in driver program"""
        if self._deserialized:
            raise Exception("Accumulator.value cannot be accessed inside tasks")
        self._value = value

    def add(self, term):
        """Adds a term to this accumulator's value"""
        self._value = self.accum_param.addInPlace(self._value, term)

    def __iadd__(self, term):
        """The += operator; adds a term to this accumulator's value"""
        self.add(term)
        return self

    def __str__(self):
        return str(self._value)

    def __repr__(self):
        return "Accumulator<id=%i, value=%s>" % (self.aid, self._value)


class AccumulatorParam(object):

    """
    Helper object that defines how to accumulate values of a given type.
    """

    def zero(self, value):
        """
        Provide a "zero value" for the type, compatible in dimensions with the
        provided C{value} (e.g., a zero vector)
        """
        raise NotImplementedError

    def addInPlace(self, value1, value2):
        """
        Add two values of the accumulator's data type, returning a new value;
        for efficiency, can also update C{value1} in place and return it.
        """
        raise NotImplementedError


class AddingAccumulatorParam(AccumulatorParam):

    """
    An AccumulatorParam that uses the + operators to add values. Designed for simple types
    such as integers, floats, and lists. Requires the zero value for the underlying type
    as a parameter.
    """

    def __init__(self, zero_value):
        self.zero_value = zero_value

    def zero(self, value):
        return self.zero_value

    def addInPlace(self, value1, value2):
        value1 += value2
        return value1


# Singleton accumulator params for some standard types
INT_ACCUMULATOR_PARAM = AddingAccumulatorParam(0)
FLOAT_ACCUMULATOR_PARAM = AddingAccumulatorParam(0.0)
COMPLEX_ACCUMULATOR_PARAM = AddingAccumulatorParam(0.0j)


class _UpdateRequestHandler(SocketServer.StreamRequestHandler):

    """
    This handler will keep polling updates from the same socket until the
    server is shutdown.
    """

    def handle(self):
        from pyspark.accumulators import _accumulatorRegistry
        auth_token = self.server.auth_token

        def poll(func):
            while not self.server.server_shutdown:
                # Poll every 1 second for new data -- don't block in case of shutdown.
                r, _, _ = select.select([self.rfile], [], [], 1)
                if self.rfile in r:
                    if func():
                        break

        def accum_updates():
            num_updates = read_int(self.rfile)
            for _ in range(num_updates):
                (aid, update) = pickleSer._read_with_length(self.rfile)
                _accumulatorRegistry[aid] += update
            # Write a byte in acknowledgement
            self.wfile.write(struct.pack("!b", 1))
            return False

        def authenticate_and_accum_updates():
            received_token = self.rfile.read(len(auth_token))
            if isinstance(received_token, bytes):
                received_token = received_token.decode("utf-8")
            if (received_token == auth_token):
                accum_updates()
                # we've authenticated, we can break out of the first loop now
                return True
            else:
                raise Exception(
                    "The value of the provided token to the AccumulatorServer is not correct.")

        # first we keep polling till we've received the authentication token
        poll(authenticate_and_accum_updates)
        # now we've authenticated, don't need to check for the token anymore
        poll(accum_updates)


class AccumulatorServer(SocketServer.TCPServer):

    def __init__(self, server_address, RequestHandlerClass, auth_token):
        SocketServer.TCPServer.__init__(self, server_address, RequestHandlerClass)
        self.auth_token = auth_token

    """
    A simple TCP server that intercepts shutdown() in order to interrupt
    our continuous polling on the handler.
    """
    server_shutdown = False

    def shutdown(self):
        self.server_shutdown = True
        SocketServer.TCPServer.shutdown(self)
        self.server_close()


def _start_update_server(auth_token):
    """Start a TCP server to receive accumulator updates in a daemon thread, and returns it"""
    server = AccumulatorServer(("localhost", 0), _UpdateRequestHandler, auth_token)
    thread = threading.Thread(target=server.serve_forever)
    thread.daemon = True
    thread.start()
    return server

if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        exit(-1)
