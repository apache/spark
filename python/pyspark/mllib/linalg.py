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
MLlib utilities for working with vectors. For dense vectors, MLlib
uses the NumPy C{array} type, so you can simply pass NumPy arrays
around. For sparse vectors, users can construct a L{SparseVector}
object from MLlib or pass SciPy C{scipy.sparse} column vectors if
SciPy is available in their environment.
"""

from numpy import array, array_equal, ndarray


class SparseVector(object):
    """
    A simple sparse vector class for passing data to MLlib. Users may
    alternatively pass use SciPy's {scipy.sparse} data types.
    """

    def __init__(self, size, *args):
        """
        Create a sparse vector, using either an array of (index, value) pairs
        or two separate arrays of indices and values (sorted by index).

        >>> print SparseVector(4, [(1, 1.0), (3, 5.5)])
        [1: 1.0, 3: 5.5]
        >>> print SparseVector(4, [1, 3], [1.0, 5.5])
        [1: 1.0, 3: 5.5]
        """
        assert type(size) == int, "first argument must be an int"
        self.size = size
        assert 1 <= len(args) <= 2, "must pass either 1 or 2 arguments"
        if len(args) == 1:
            pairs = sorted(args[0])
            self.indices = array([p[0] for p in pairs], dtype='int32')
            self.values = array([p[1] for p in pairs], dtype='float64')
        else:
            assert len(args[0]) == len(args[1]), "index and value arrays not same length"
            self.indices = array(args[0], dtype='int32')
            self.values = array(args[1], dtype='float64')
            for i in xrange(len(self.indices) - 1):
                if self.indices[i] >= self.indices[i + 1]:
                    raise TypeError("indices array must be sorted")

    def dot(self, other):
        """
        Dot product with another SparseVector or NumPy array.

        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.dot(a)
        25.0
        >>> a.dot(array([1., 2., 3., 4.]))
        22.0
        >>> b = SparseVector(4, [2, 4], [1.0, 2.0])
        >>> a.dot(b)
        0.0
        """
        result = 0.0
        if type(other) == ndarray:
            for i in xrange(len(self.indices)):
                result += self.values[i] * other[self.indices[i]]
        else:
            i, j = 0, 0
            while i < len(self.indices) and j < len(other.indices):
                if self.indices[i] == other.indices[j]:
                    result += self.values[i] * other.values[j]
                    i += 1
                    j += 1
                elif self.indices[i] < other.indices[j]:
                    i += 1
                else:
                    j += 1
        return result

    def __str__(self):
        inds = self.indices
        vals = self.values
        entries = ", ".join(["{0}: {1}".format(inds[i], vals[i]) for i in xrange(len(inds))])
        return "[" + entries + "]"

    def __repr__(self):
        inds = self.indices
        vals = self.values
        entries = ", ".join(["({0}, {1})".format(inds[i], vals[i]) for i in xrange(len(inds))])
        return "SparseVector({0}, [{1}])".format(self.size, entries)

    def __eq__(self, other):
        """
        Test SparseVectors for equality.

        >>> v1 = SparseVector(4, [(1, 1.0), (3, 5.5)])
        >>> v2 = SparseVector(4, [(1, 1.0), (3, 5.5)])
        >>> v1 == v2
        True
        >>> v1 != v2
        False
        """

        return (isinstance(other, self.__class__)
            and other.size == self.size
            and array_equal(other.indices, self.indices)
            and array_equal(other.values, self.values))

    def __ne__(self, other):
        return not self.__eq__(other)



class Vectors(object):
    """
    Factory methods to create MLlib vectors. Note that dense vectors
    are simply represented as NumPy array objects, so there is no need
    to covert them for use in MLlib. For sparse vectors, the factory
    methods in this class create an MLlib-compatible type, or users
    can pass in SciPy's C{scipy.sparse} column vectors.
    """

    @staticmethod
    def sparse(size, *args):
        """
        Create a sparse vector, using either an array of (index, value) pairs
        or two separate arrays of indices and values (sorted by index).

        >>> print Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        [1: 1.0, 3: 5.5]
        >>> print Vectors.sparse(4, [1, 3], [1.0, 5.5])
        [1: 1.0, 3: 5.5]
        """
        return SparseVector(size, *args)

    @staticmethod
    def dense(elements):
        """
        Create a dense vector of 64-bit floats from a Python list. Always
        returns a NumPy array.

        >>> Vectors.dense([1, 2, 3])
        array([ 1.,  2.,  3.])
        """
        return array(elements, dtype='float64')


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
