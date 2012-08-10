"""
Data serialization methods.

The Spark Python API is built on top of the Spark Java API.  RDDs created in
Python are stored in Java as RDDs of Strings.  Python objects are automatically
serialized/deserialized, so this representation is transparent to the end-user.

------------------
Serializer objects
------------------

`Serializer` objects are used to customize how an RDD's values are serialized.

Each `Serializer` is a named tuple with four fields:

    - A `dumps` function, for serializing a Python object to a string.

    - A `loads` function, for deserializing a Python object from a string.

    - An `is_comparable` field, True if equal Python objects are serialized to
      equal strings, and False otherwise.

    - A `name` field, used to identify the Serializer.  Serializers are
      compared for equality by comparing their names.

The serializer's output should be base64-encoded.

------------------------------------------------------------------
`is_comparable`: comparing serialized representations for equality
------------------------------------------------------------------

If `is_comparable` is False, the serializer's representations of equal objects
are not required to be equal:

>>> import pickle
>>> a = {1: 0, 9: 0}
>>> b = {9: 0, 1: 0}
>>> a == b
True
>>> pickle.dumps(a) == pickle.dumps(b)
False

RDDs with comparable serializers can use native Java implementations of
operations like join() and distinct(), which may lead to better performance by
eliminating deserialization and Python comparisons.

The default JSONSerializer produces comparable representations of common Python
data structures.

--------------------------------------
Examples of serialized representations
--------------------------------------

The RDD transformations that use Python UDFs are implemented in terms of
a modified `PipedRDD.pipe()` function.  For each record `x` in the RDD, the
`pipe()` function pipes `x.toString()` to a Python worker process, which
deserializes the string into a Python object, executes user-defined functions,
and outputs serialized Python objects.

The regular `toString()` method returns an ambiguous representation, due to the
way that Scala `Option` instances are printed:

>>> from context import SparkContext
>>> sc = SparkContext("local", "SerializerDocs")
>>> x = sc.parallelizePairs([("a", 1), ("b", 4)])
>>> y = sc.parallelizePairs([("a", 2)])

>>> print y.rightOuterJoin(x)._jrdd.first().toString()
(ImEi,(Some(Mg==),MQ==))

In Java, preprocessing is performed to handle Option instances, so the Python
process receives unambiguous input:

>>> print sc.python_dump(y.rightOuterJoin(x)._jrdd.first())
(ImEi,(Mg==,MQ==))

The base64-encoding eliminates the need to escape newlines, parentheses and
other special characters.

----------------------
Serializer composition
----------------------

In order to handle nested structures, which could contain object serialized
with different serializers, the RDD module composes serializers.  For example,
the serializers in the previous example are:

>>> print x.serializer.name
PairSerializer<JSONSerializer, JSONSerializer>

>>> print y.serializer.name
PairSerializer<JSONSerializer, JSONSerializer>

>>> print y.rightOuterJoin(x).serializer.name
PairSerializer<JSONSerializer, PairSerializer<OptionSerializer<JSONSerializer>, JSONSerializer>>
"""
from base64 import standard_b64encode, standard_b64decode
from collections import namedtuple
import cPickle
import simplejson


Serializer = namedtuple("Serializer",
    ["dumps","loads", "is_comparable", "name"])


NopSerializer = Serializer(str, str, True, "NopSerializer")


JSONSerializer = Serializer(
    lambda obj: standard_b64encode(simplejson.dumps(obj, sort_keys=True,
        separators=(',', ':'))),
    lambda s: simplejson.loads(standard_b64decode(s)),
    True,
    "JSONSerializer"
)


PickleSerializer = Serializer(
    lambda obj: standard_b64encode(cPickle.dumps(obj)),
    lambda s: cPickle.loads(standard_b64decode(s)),
    False,
    "PickleSerializer"
)


def OptionSerializer(serializer):
    """
    >>> ser = OptionSerializer(NopSerializer)
    >>> ser.loads(ser.dumps("Hello, World!"))
    'Hello, World!'
    >>> ser.loads(ser.dumps(None)) is None
    True
    """
    none_placeholder = '*'

    def dumps(x):
        if x is None:
            return none_placeholder
        else:
            return serializer.dumps(x)

    def loads(x):
        if x == none_placeholder:
            return None
        else:
            return serializer.loads(x)

    name = "OptionSerializer<%s>" % serializer.name
    return Serializer(dumps, loads, serializer.is_comparable, name)


def PairSerializer(keySerializer, valSerializer):
    """
    Returns a Serializer for a (key, value) pair.

    >>> ser = PairSerializer(JSONSerializer, JSONSerializer)
    >>> ser.loads(ser.dumps((1, 2)))
    (1, 2)

    >>> ser = PairSerializer(JSONSerializer, ser)
    >>> ser.loads(ser.dumps((1, (2, 3))))
    (1, (2, 3))
    """
    def loads(kv):
        try:
            (key, val) = kv[1:-1].split(',', 1)
            key = keySerializer.loads(key)
            val = valSerializer.loads(val)
            return (key, val)
        except:
            print "Error in deserializing pair from '%s'" % str(kv)
            raise

    def dumps(kv):
        (key, val) = kv
        return"(%s,%s)" % (keySerializer.dumps(key), valSerializer.dumps(val))
    is_comparable = \
        keySerializer.is_comparable and valSerializer.is_comparable
    name = "PairSerializer<%s, %s>" % (keySerializer.name, valSerializer.name)
    return Serializer(dumps, loads, is_comparable, name)


def ArraySerializer(serializer):
    """
    >>> ser = ArraySerializer(JSONSerializer)
    >>> ser.loads(ser.dumps([1, 2, 3, 4]))
    [1, 2, 3, 4]
    >>> ser = ArraySerializer(PairSerializer(JSONSerializer, PickleSerializer))
    >>> ser.loads(ser.dumps([('a', 1), ('b', 2)]))
    [('a', 1), ('b', 2)]
    >>> ser.loads(ser.dumps([('a', 1)]))
    [('a', 1)]
    >>> ser.loads(ser.dumps([]))
    []
    """
    def dumps(arr):
        if arr == []:
            return '[]'
        else:
            return '[' + '|'.join(serializer.dumps(x) for x in arr) + ']'

    def loads(s):
        if s == '[]':
            return []
        items = s[1:-1]
        if '|' in items:
            items = items.split('|')
        else:
            items = [items]
        return [serializer.loads(x) for x in items]

    name = "ArraySerializer<%s>" % serializer.name
    return Serializer(dumps, loads, serializer.is_comparable, name)


# TODO: IntegerSerializer


# TODO: DoubleSerializer


def _test():
    import doctest
    doctest.testmod()


if __name__ == "__main__":
    _test()
