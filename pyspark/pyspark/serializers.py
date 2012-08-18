"""
Data serialization methods.

The Spark Python API is built on top of the Spark Java API.  RDDs created in
Python are stored in Java as RDD[Array[Byte]].  Python objects are
automatically serialized/deserialized, so this representation is transparent to
the end-user.
"""
from collections import namedtuple
import cPickle
import struct


Serializer = namedtuple("Serializer", ["dumps","loads"])


PickleSerializer = Serializer(
    lambda obj: cPickle.dumps(obj, -1),
    cPickle.loads)


def dumps(obj, stream):
    # TODO: determining the length of non-byte objects.
    stream.write(struct.pack("!i", len(obj)))
    stream.write(obj)


def loads(stream):
    length = stream.read(4)
    if length == "":
        raise EOFError
    length = struct.unpack("!i", length)[0]
    obj = stream.read(length)
    if obj == "":
        raise EOFError
    return obj
