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

from numpy import array


class SparseVector(object):
    """
    A simple sparse vector class for passing data to MLlib. Users may
    alternatively pass use SciPy's {scipy.sparse} data types.
    """

    def __init__(self, size, *args):
        """
        Create a sparse vector, using either an array of (index, value) pairs
        or two separate arrays of indices and values.

        >>> print SparseVector(4, [(1, 1.0), (3, 5.5)])
        [1: 1.0, 3: 5.5]
        >>> print SparseVector(4, [1, 3], [1.0, 5.5])
        [1: 1.0, 3: 5.5]
        """
        self.size = size
        assert 1 <= len(args) <= 2, "must pass either 1 or 2 arguments"
        if len(args) == 1:
            pairs = args[0]
            self.indices = array([p[0] for p in pairs], dtype='int32')
            self.values = array([p[1] for p in pairs], dtype='float64')
        else:
            assert len(args[0]) == len(args[1]), "index and value arrays not same length"
            self.indices = array(args[0], dtype='int32')
            self.values = array(args[1], dtype='float64')

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
        or two separate arrays of indices and values.

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
