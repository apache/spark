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

import gc
import os
import sys
from tempfile import NamedTemporaryFile
import threading
import pickle

from pyspark.java_gateway import local_connect_and_auth
from pyspark.serializers import ChunkedStream, pickle_protocol
from pyspark.util import print_exec


__all__ = ['Broadcast']


# Holds broadcasted data received from Java, keyed by its id.
_broadcastRegistry = {}


def _from_id(bid):
    from pyspark.broadcast import _broadcastRegistry
    if bid not in _broadcastRegistry:
        raise Exception("Broadcast variable '%s' not loaded!" % bid)
    return _broadcastRegistry[bid]


class Broadcast(object):

    """
    A broadcast variable created with :meth:`SparkContext.broadcast`.
    Access its value through :attr:`value`.

    Examples
    --------
    >>> from pyspark.context import SparkContext
    >>> sc = SparkContext('local', 'test')
    >>> b = sc.broadcast([1, 2, 3, 4, 5])
    >>> b.value
    [1, 2, 3, 4, 5]
    >>> sc.parallelize([0, 0]).flatMap(lambda x: b.value).collect()
    [1, 2, 3, 4, 5, 1, 2, 3, 4, 5]
    >>> b.unpersist()

    >>> large_broadcast = sc.broadcast(range(10000))
    """

    def __init__(self, sc=None, value=None, pickle_registry=None, path=None,
                 sock_file=None):
        """
        Should not be called directly by users -- use :meth:`SparkContext.broadcast`
        instead.
        """
        if sc is not None:
            # we're on the driver.  We want the pickled data to end up in a file (maybe encrypted)
            f = NamedTemporaryFile(delete=False, dir=sc._temp_dir)
            self._path = f.name
            self._sc = sc
            self._python_broadcast = sc._jvm.PythonRDD.setupBroadcast(self._path)
            if sc._encryption_enabled:
                # with encryption, we ask the jvm to do the encryption for us, we send it data
                # over a socket
                port, auth_secret = self._python_broadcast.setupEncryptionServer()
                (encryption_sock_file, _) = local_connect_and_auth(port, auth_secret)
                broadcast_out = ChunkedStream(encryption_sock_file, 8192)
            else:
                # no encryption, we can just write pickled data directly to the file from python
                broadcast_out = f
            self.dump(value, broadcast_out)
            if sc._encryption_enabled:
                self._python_broadcast.waitTillDataReceived()
            self._jbroadcast = sc._jsc.broadcast(self._python_broadcast)
            self._pickle_registry = pickle_registry
        else:
            # we're on an executor
            self._jbroadcast = None
            self._sc = None
            self._python_broadcast = None
            if sock_file is not None:
                # the jvm is doing decryption for us.  Read the value
                # immediately from the sock_file
                self._value = self.load(sock_file)
            else:
                # the jvm just dumps the pickled data in path -- we'll unpickle lazily when
                # the value is requested
                assert(path is not None)
                self._path = path

    def dump(self, value, f):
        try:
            pickle.dump(value, f, pickle_protocol)
        except pickle.PickleError:
            raise
        except Exception as e:
            msg = "Could not serialize broadcast: %s: %s" \
                  % (e.__class__.__name__, str(e))
            print_exec(sys.stderr)
            raise pickle.PicklingError(msg)
        f.close()

    def load_from_path(self, path):
        with open(path, 'rb', 1 << 20) as f:
            return self.load(f)

    def load(self, file):
        # "file" could also be a socket
        gc.disable()
        try:
            return pickle.load(file)
        finally:
            gc.enable()

    @property
    def value(self):
        """ Return the broadcasted value
        """
        if not hasattr(self, "_value") and self._path is not None:
            # we only need to decrypt it here when encryption is enabled and
            # if its on the driver, since executor decryption is handled already
            if self._sc is not None and self._sc._encryption_enabled:
                port, auth_secret = self._python_broadcast.setupDecryptionServer()
                (decrypted_sock_file, _) = local_connect_and_auth(port, auth_secret)
                self._python_broadcast.waitTillBroadcastDataSent()
                return self.load(decrypted_sock_file)
            else:
                self._value = self.load_from_path(self._path)
        return self._value

    def unpersist(self, blocking=False):
        """
        Delete cached copies of this broadcast on the executors. If the
        broadcast is used after this is called, it will need to be
        re-sent to each executor.

        Parameters
        ----------
        blocking : bool, optional
            Whether to block until unpersisting has completed
        """
        if self._jbroadcast is None:
            raise Exception("Broadcast can only be unpersisted in driver")
        self._jbroadcast.unpersist(blocking)

    def destroy(self, blocking=False):
        """
        Destroy all data and metadata related to this broadcast variable.
        Use this with caution; once a broadcast variable has been destroyed,
        it cannot be used again.

        .. versionchanged:: 3.0.0
           Added optional argument `blocking` to specify whether to block until all
           blocks are deleted.

        Parameters
        ----------
        blocking : bool, optional
            Whether to block until unpersisting has completed
        """
        if self._jbroadcast is None:
            raise Exception("Broadcast can only be destroyed in driver")
        self._jbroadcast.destroy(blocking)
        os.unlink(self._path)

    def __reduce__(self):
        if self._jbroadcast is None:
            raise Exception("Broadcast can only be serialized in driver")
        self._pickle_registry.add(self)
        return _from_id, (self._jbroadcast.id(),)


class BroadcastPickleRegistry(threading.local):
    """ Thread-local registry for broadcast variables that have been pickled
    """

    def __init__(self):
        self.__dict__.setdefault("_registry", set())

    def __iter__(self):
        for bcast in self._registry:
            yield bcast

    def add(self, bcast):
        self._registry.add(bcast)

    def clear(self):
        self._registry.clear()


if __name__ == "__main__":
    import doctest
    (failure_count, test_count) = doctest.testmod()
    if failure_count:
        sys.exit(-1)
