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
MLlib utilities for linear algebra. For dense vectors, MLlib
uses the NumPy C{array} type, so you can simply pass NumPy arrays
around. For sparse vectors, users can construct a L{SparseVector}
object from MLlib or pass SciPy C{scipy.sparse} column vectors if
SciPy is available in their environment.
"""

from numpy import array, array_equal, ndarray, float64, int32


class SparseVector(object):
    """
    A simple sparse vector class for passing data to MLlib. Users may
    alternatively pass SciPy's {scipy.sparse} data types.
    """

    def __init__(self, size, *args):
        """
        Create a sparse vector, using either a dictionary, a list of
        (index, value) pairs, or two separate arrays of indices and
        values (sorted by index).

        @param size: Size of the vector.
        @param args: Non-zero entries, as a dictionary, list of tupes,
               or two sorted lists containing indices and values.

        >>> print SparseVector(4, {1: 1.0, 3: 5.5})
        (4,[1,3],[1.0,5.5])
        >>> print SparseVector(4, [(1, 1.0), (3, 5.5)])
        (4,[1,3],[1.0,5.5])
        >>> print SparseVector(4, [1, 3], [1.0, 5.5])
        (4,[1,3],[1.0,5.5])
        """
        self.size = int(size)
        assert 1 <= len(args) <= 2, "must pass either 2 or 3 arguments"
        if len(args) == 1:
            pairs = args[0]
            if type(pairs) == dict:
                pairs = pairs.items()
            pairs = sorted(pairs)
            self.indices = array([p[0] for p in pairs], dtype=int32)
            self.values = array([p[1] for p in pairs], dtype=float64)
        else:
            assert len(args[0]) == len(args[1]), "index and value arrays not same length"
            self.indices = array(args[0], dtype=int32)
            self.values = array(args[1], dtype=float64)
            for i in xrange(len(self.indices) - 1):
                if self.indices[i] >= self.indices[i + 1]:
                    raise TypeError("indices array must be sorted")

    def dot(self, other):
        """
        Dot product with a SparseVector or 1- or 2-dimensional Numpy array.

        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.dot(a)
        25.0
        >>> a.dot(array([1., 2., 3., 4.]))
        22.0
        >>> b = SparseVector(4, [2, 4], [1.0, 2.0])
        >>> a.dot(b)
        0.0
        >>> a.dot(array([[1, 1], [2, 2], [3, 3], [4, 4]]))
        array([ 22.,  22.])
        """
        if type(other) == ndarray:
            if other.ndim == 1:
                result = 0.0
                for i in xrange(len(self.indices)):
                    result += self.values[i] * other[self.indices[i]]
                return result
            elif other.ndim == 2:
                results = [self.dot(other[:, i]) for i in xrange(other.shape[1])]
                return array(results)
            else:
                raise Exception("Cannot call dot with %d-dimensional array" % other.ndim)
        else:
            result = 0.0
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

    def squared_distance(self, other):
        """
        Squared distance from a SparseVector or 1-dimensional NumPy array.

        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.squared_distance(a)
        0.0
        >>> a.squared_distance(array([1., 2., 3., 4.]))
        11.0
        >>> b = SparseVector(4, [2, 4], [1.0, 2.0])
        >>> a.squared_distance(b)
        30.0
        >>> b.squared_distance(a)
        30.0
        """
        if type(other) == ndarray:
            if other.ndim == 1:
                result = 0.0
                j = 0   # index into our own array
                for i in xrange(other.shape[0]):
                    if j < len(self.indices) and self.indices[j] == i:
                        diff = self.values[j] - other[i]
                        result += diff * diff
                        j += 1
                    else:
                        result += other[i] * other[i]
                return result
            else:
                raise Exception("Cannot call squared_distance with %d-dimensional array" %
                                other.ndim)
        else:
            result = 0.0
            i, j = 0, 0
            while i < len(self.indices) and j < len(other.indices):
                if self.indices[i] == other.indices[j]:
                    diff = self.values[i] - other.values[j]
                    result += diff * diff
                    i += 1
                    j += 1
                elif self.indices[i] < other.indices[j]:
                    result += self.values[i] * self.values[i]
                    i += 1
                else:
                    result += other.values[j] * other.values[j]
                    j += 1
            while i < len(self.indices):
                result += self.values[i] * self.values[i]
                i += 1
            while j < len(other.indices):
                result += other.values[j] * other.values[j]
                j += 1
            return result

    def __str__(self):
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        vals = "[" + ",".join([str(v) for v in self.values]) + "]"
        return "(" + ",".join((str(self.size), inds, vals)) + ")"

    def __repr__(self):
        inds = self.indices
        vals = self.values
        entries = ", ".join(["{0}: {1}".format(inds[i], vals[i]) for i in xrange(len(inds))])
        return "SparseVector({0}, {{{1}}})".format(self.size, entries)

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
    Factory methods for working with vectors. Note that dense vectors
    are simply represented as NumPy array objects, so there is no need
    to covert them for use in MLlib. For sparse vectors, the factory
    methods in this class create an MLlib-compatible type, or users
    can pass in SciPy's C{scipy.sparse} column vectors.
    """

    @staticmethod
    def sparse(size, *args):
        """
        Create a sparse vector, using either a dictionary, a list of
        (index, value) pairs, or two separate arrays of indices and
        values (sorted by index).

        @param size: Size of the vector.
        @param args: Non-zero entries, as a dictionary, list of tupes,
                     or two sorted lists containing indices and values.

        >>> print Vectors.sparse(4, {1: 1.0, 3: 5.5})
        (4,[1,3],[1.0,5.5])
        >>> print Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        (4,[1,3],[1.0,5.5])
        >>> print Vectors.sparse(4, [1, 3], [1.0, 5.5])
        (4,[1,3],[1.0,5.5])
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
        return array(elements, dtype=float64)

    @staticmethod
    def stringify(vector):
        """
        Converts a vector into a string, which can be recognized by
        Vectors.parse().

        >>> Vectors.stringify(Vectors.sparse(2, [1], [1.0]))
        '(2,[1],[1.0])'
        >>> Vectors.stringify(Vectors.dense([0.0, 1.0]))
        '[0.0,1.0]'
        """
        if type(vector) == SparseVector:
            return str(vector)
        else:
            return "[" + ",".join([str(v) for v in vector]) + "]"


def _test():
    import doctest
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
