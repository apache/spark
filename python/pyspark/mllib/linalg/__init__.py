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
uses the NumPy `array` type, so you can simply pass NumPy arrays
around. For sparse vectors, users can construct a :class:`SparseVector`
object from MLlib or pass SciPy `scipy.sparse` column vectors if
SciPy is available in their environment.
"""

import sys
import array
import struct
from typing import (
    Any,
    Callable,
    cast,
    Dict,
    Generic,
    Iterable,
    List,
    Optional,
    overload,
    Sequence,
    Tuple,
    Type,
    TypeVar,
    TYPE_CHECKING,
    Union,
)

import numpy as np

from pyspark import since
from pyspark.ml import linalg as newlinalg
from pyspark.sql.types import (
    UserDefinedType,
    StructField,
    StructType,
    ArrayType,
    DoubleType,
    IntegerType,
    ByteType,
    BooleanType,
)

if TYPE_CHECKING:
    from pyspark.mllib._typing import VectorLike, NormType
    from scipy.sparse import spmatrix
    from numpy.typing import ArrayLike


QT = TypeVar("QT")
RT = TypeVar("RT")


__all__ = [
    "Vector",
    "DenseVector",
    "SparseVector",
    "Vectors",
    "Matrix",
    "DenseMatrix",
    "SparseMatrix",
    "Matrices",
    "QRDecomposition",
]


# Check whether we have SciPy. MLlib works without it too, but if we have it, some methods,
# such as _dot and _serialize_double_vector, start to support scipy.sparse matrices.

try:
    import scipy.sparse

    _have_scipy = True
except BaseException:
    # No SciPy in environment, but that's okay
    _have_scipy = False


def _convert_to_vector(d: Union["VectorLike", "spmatrix", range]) -> "Vector":
    if isinstance(d, Vector):
        return d
    elif type(d) in (array.array, np.array, np.ndarray, list, tuple, range):
        return DenseVector(d)
    elif _have_scipy and scipy.sparse.issparse(d):
        assert cast("spmatrix", d).shape[1] == 1, "Expected column vector"
        # Make sure the converted csc_matrix has sorted indices.
        csc = cast("spmatrix", d).tocsc()
        if not csc.has_sorted_indices:
            csc.sort_indices()
        return SparseVector(cast("spmatrix", d).shape[0], csc.indices, csc.data)
    else:
        raise TypeError("Cannot convert type %s into Vector" % type(d))


def _vector_size(v: Union["VectorLike", "spmatrix", range]) -> int:
    """
    Returns the size of the vector.

    Examples
    --------
    >>> _vector_size([1., 2., 3.])
    3
    >>> _vector_size((1., 2., 3.))
    3
    >>> _vector_size(array.array('d', [1., 2., 3.]))
    3
    >>> _vector_size(np.zeros(3))
    3
    >>> _vector_size(np.zeros((3, 1)))
    3
    >>> _vector_size(np.zeros((1, 3)))
    Traceback (most recent call last):
        ...
    ValueError: Cannot treat an ndarray of shape (1, 3) as a vector
    """
    if isinstance(v, Vector):
        return len(v)
    elif type(v) in (array.array, list, tuple, range):
        return len(v)
    elif type(v) == np.ndarray:
        if v.ndim == 1 or (v.ndim == 2 and v.shape[1] == 1):
            return len(v)
        else:
            raise ValueError("Cannot treat an ndarray of shape %s as a vector" % str(v.shape))
    elif _have_scipy and scipy.sparse.issparse(v):
        assert cast("spmatrix", v).shape[1] == 1, "Expected column vector"
        return cast("spmatrix", v).shape[0]
    else:
        raise TypeError("Cannot treat type %s as a vector" % type(v))


def _format_float(f: float, digits: int = 4) -> str:
    s = str(round(f, digits))
    if "." in s:
        s = s[: s.index(".") + 1 + digits]
    return s


def _format_float_list(xs: Iterable[float]) -> List[str]:
    return [_format_float(x) for x in xs]


def _double_to_long_bits(value: float) -> int:
    if np.isnan(value):
        value = float("nan")
    # pack double into 64 bits, then unpack as long int
    return struct.unpack("Q", struct.pack("d", value))[0]


class VectorUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for Vector.
    """

    @classmethod
    def sqlType(cls) -> StructType:
        return StructType(
            [
                StructField("type", ByteType(), False),
                StructField("size", IntegerType(), True),
                StructField("indices", ArrayType(IntegerType(), False), True),
                StructField("values", ArrayType(DoubleType(), False), True),
            ]
        )

    @classmethod
    def module(cls) -> str:
        return "pyspark.mllib.linalg"

    @classmethod
    def scalaUDT(cls) -> str:
        return "org.apache.spark.mllib.linalg.VectorUDT"

    def serialize(
        self, obj: "Vector"
    ) -> Tuple[int, Optional[int], Optional[List[int]], List[float]]:
        if isinstance(obj, SparseVector):
            indices = [int(i) for i in obj.indices]
            values = [float(v) for v in obj.values]
            return (0, obj.size, indices, values)
        elif isinstance(obj, DenseVector):
            values = [float(v) for v in obj]  # type: ignore[attr-defined]
            return (1, None, None, values)
        else:
            raise TypeError("cannot serialize %r of type %r" % (obj, type(obj)))

    def deserialize(
        self, datum: Tuple[int, Optional[int], Optional[List[int]], List[float]]
    ) -> "Vector":
        assert (
            len(datum) == 4
        ), "VectorUDT.deserialize given row with length %d but requires 4" % len(datum)
        tpe = datum[0]
        if tpe == 0:
            return SparseVector(cast(int, datum[1]), cast(List[int], datum[2]), datum[3])
        elif tpe == 1:
            return DenseVector(datum[3])
        else:
            raise ValueError("do not recognize type %r" % tpe)

    def simpleString(self) -> str:
        return "vector"


class MatrixUDT(UserDefinedType):
    """
    SQL user-defined type (UDT) for Matrix.
    """

    @classmethod
    def sqlType(cls) -> StructType:
        return StructType(
            [
                StructField("type", ByteType(), False),
                StructField("numRows", IntegerType(), False),
                StructField("numCols", IntegerType(), False),
                StructField("colPtrs", ArrayType(IntegerType(), False), True),
                StructField("rowIndices", ArrayType(IntegerType(), False), True),
                StructField("values", ArrayType(DoubleType(), False), True),
                StructField("isTransposed", BooleanType(), False),
            ]
        )

    @classmethod
    def module(cls) -> str:
        return "pyspark.mllib.linalg"

    @classmethod
    def scalaUDT(cls) -> str:
        return "org.apache.spark.mllib.linalg.MatrixUDT"

    def serialize(
        self, obj: "Matrix"
    ) -> Tuple[int, int, int, Optional[List[int]], Optional[List[int]], List[float], bool]:
        if isinstance(obj, SparseMatrix):
            colPtrs = [int(i) for i in obj.colPtrs]
            rowIndices = [int(i) for i in obj.rowIndices]
            values = [float(v) for v in obj.values]
            return (
                0,
                obj.numRows,
                obj.numCols,
                colPtrs,
                rowIndices,
                values,
                bool(obj.isTransposed),
            )
        elif isinstance(obj, DenseMatrix):
            values = [float(v) for v in obj.values]
            return (1, obj.numRows, obj.numCols, None, None, values, bool(obj.isTransposed))
        else:
            raise TypeError("cannot serialize type %r" % (type(obj)))

    def deserialize(
        self,
        datum: Tuple[int, int, int, Optional[List[int]], Optional[List[int]], List[float], bool],
    ) -> "Matrix":
        assert (
            len(datum) == 7
        ), "MatrixUDT.deserialize given row with length %d but requires 7" % len(datum)
        tpe = datum[0]
        if tpe == 0:
            return SparseMatrix(
                datum[1],
                datum[2],
                cast(List[int], datum[3]),
                cast(List[int], datum[4]),
                datum[5],
                datum[6],
            )
        elif tpe == 1:
            return DenseMatrix(datum[1], datum[2], datum[5], datum[6])
        else:
            raise ValueError("do not recognize type %r" % tpe)

    def simpleString(self) -> str:
        return "matrix"


class Vector:
    __UDT__ = VectorUDT()

    """
    Abstract class for DenseVector and SparseVector
    """

    def toArray(self) -> np.ndarray:
        """
        Convert the vector into an numpy.ndarray

        Returns
        -------
        :py:class:`numpy.ndarray`
        """
        raise NotImplementedError

    def asML(self) -> newlinalg.Vector:
        """
        Convert this vector to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        Returns
        -------
        :py:class:`pyspark.ml.linalg.Vector`
        """
        raise NotImplementedError

    def __len__(self) -> int:
        raise NotImplementedError


class DenseVector(Vector):
    """
    A dense vector represented by a value array. We use numpy array for
    storage and arithmetics will be delegated to the underlying numpy
    array.

    Examples
    --------
    >>> v = Vectors.dense([1.0, 2.0])
    >>> u = Vectors.dense([3.0, 4.0])
    >>> v + u
    DenseVector([4.0, 6.0])
    >>> 2 - v
    DenseVector([1.0, 0.0])
    >>> v / 2
    DenseVector([0.5, 1.0])
    >>> v * u
    DenseVector([3.0, 8.0])
    >>> u / v
    DenseVector([3.0, 2.0])
    >>> u % 2
    DenseVector([1.0, 0.0])
    >>> -v
    DenseVector([-1.0, -2.0])
    """

    def __init__(self, ar: Union[bytes, np.ndarray, Iterable[float]]):
        ar_: np.ndarray
        if isinstance(ar, bytes):
            ar_ = np.frombuffer(ar, dtype=np.float64)
        elif not isinstance(ar, np.ndarray):
            ar_ = np.array(ar, dtype=np.float64)
        else:
            ar_ = ar.astype(np.float64) if ar.dtype != np.float64 else ar
        self.array = ar_

    @staticmethod
    def parse(s: str) -> "DenseVector":
        """
        Parse string representation back into the DenseVector.

        Examples
        --------
        >>> DenseVector.parse(' [ 0.0,1.0,2.0,  3.0]')
        DenseVector([0.0, 1.0, 2.0, 3.0])
        """
        start = s.find("[")
        if start == -1:
            raise ValueError("Array should start with '['.")
        end = s.find("]")
        if end == -1:
            raise ValueError("Array should end with ']'.")
        s = s[start + 1 : end]

        try:
            values = [float(val) for val in s.split(",") if val]
        except ValueError:
            raise ValueError("Unable to parse values from %s" % s)
        return DenseVector(values)

    def __reduce__(self) -> Tuple[Type["DenseVector"], Tuple[bytes]]:
        return DenseVector, (self.array.tobytes(),)

    def numNonzeros(self) -> int:
        """
        Number of nonzero elements. This scans all active values and count non zeros
        """
        return np.count_nonzero(self.array)

    def norm(self, p: "NormType") -> np.float64:
        """
        Calculates the norm of a DenseVector.

        Examples
        --------
        >>> a = DenseVector([0, -1, 2, -3])
        >>> a.norm(2)
        3.7...
        >>> a.norm(1)
        6.0
        """
        return np.linalg.norm(self.array, p)

    def dot(self, other: Iterable[float]) -> np.float64:
        """
        Compute the dot product of two Vectors. We support
        (Numpy array, list, SparseVector, or SciPy sparse)
        and a target NumPy array that is either 1- or 2-dimensional.
        Equivalent to calling numpy.dot of the two vectors.

        Examples
        --------
        >>> dense = DenseVector(array.array('d', [1., 2.]))
        >>> dense.dot(dense)
        5.0
        >>> dense.dot(SparseVector(2, [0, 1], [2., 1.]))
        4.0
        >>> dense.dot(range(1, 3))
        5.0
        >>> dense.dot(np.array(range(1, 3)))
        5.0
        >>> dense.dot([1.,])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> dense.dot(np.reshape([1., 2., 3., 4.], (2, 2), order='F'))
        array([  5.,  11.])
        >>> dense.dot(np.reshape([1., 2., 3.], (3, 1), order='F'))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        if type(other) == np.ndarray:
            if other.ndim > 1:
                assert len(self) == other.shape[0], "dimension mismatch"
            return np.dot(self.array, other)
        elif _have_scipy and scipy.sparse.issparse(other):
            assert len(self) == cast("spmatrix", other).shape[0], "dimension mismatch"
            return cast("spmatrix", other).transpose().dot(self.toArray())
        else:
            assert len(self) == _vector_size(other), "dimension mismatch"
            if isinstance(other, SparseVector):
                return other.dot(self)
            elif isinstance(other, Vector):
                return np.dot(self.toArray(), other.toArray())
            else:
                return np.dot(self.toArray(), cast("ArrayLike", other))

    def squared_distance(self, other: Iterable[float]) -> np.float64:
        """
        Squared distance of two Vectors.

        Examples
        --------
        >>> dense1 = DenseVector(array.array('d', [1., 2.]))
        >>> dense1.squared_distance(dense1)
        0.0
        >>> dense2 = np.array([2., 1.])
        >>> dense1.squared_distance(dense2)
        2.0
        >>> dense3 = [2., 1.]
        >>> dense1.squared_distance(dense3)
        2.0
        >>> sparse1 = SparseVector(2, [0, 1], [2., 1.])
        >>> dense1.squared_distance(sparse1)
        2.0
        >>> dense1.squared_distance([1.,])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> dense1.squared_distance(SparseVector(1, [0,], [1.,]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        assert len(self) == _vector_size(other), "dimension mismatch"
        if isinstance(other, SparseVector):
            return other.squared_distance(self)
        elif _have_scipy and scipy.sparse.issparse(other):
            return _convert_to_vector(other).squared_distance(self)  # type: ignore[attr-defined]

        if isinstance(other, Vector):
            other = other.toArray()
        elif not isinstance(other, np.ndarray):
            other = np.array(other)
        diff: np.ndarray = self.toArray() - other
        return np.dot(diff, diff)

    def toArray(self) -> np.ndarray:
        """
        Returns an numpy.ndarray
        """
        return self.array

    def asML(self) -> newlinalg.DenseVector:
        """
        Convert this vector to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        .. versionadded:: 2.0.0

        Returns
        -------
        :py:class:`pyspark.ml.linalg.DenseVector`
        """
        return newlinalg.DenseVector(self.array)

    @property
    def values(self) -> np.ndarray:
        """
        Returns a list of values
        """
        return self.array

    @overload
    def __getitem__(self, item: int) -> np.float64:
        ...

    @overload
    def __getitem__(self, item: slice) -> np.ndarray:
        ...

    def __getitem__(self, item: Union[int, slice]) -> Union[np.float64, np.ndarray]:
        return self.array[item]

    def __len__(self) -> int:
        return len(self.array)

    def __str__(self) -> str:
        return "[" + ",".join([str(v) for v in self.array]) + "]"

    def __repr__(self) -> str:
        return "DenseVector([%s])" % (", ".join(_format_float(i) for i in self.array))

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, DenseVector):
            return np.array_equal(self.array, other.array)
        elif isinstance(other, SparseVector):
            if len(self) != other.size:
                return False
            return Vectors._equals(list(range(len(self))), self.array, other.indices, other.values)
        return False

    def __ne__(self, other: Any) -> bool:
        return not self == other

    def __hash__(self) -> int:
        size = len(self)
        result = 31 + size
        nnz = 0
        i = 0
        while i < size and nnz < 128:
            if self.array[i] != 0:
                result = 31 * result + i
                bits = _double_to_long_bits(self.array[i])
                result = 31 * result + (bits ^ (bits >> 32))
                nnz += 1
            i += 1
        return result

    def __getattr__(self, item: str) -> Any:
        return getattr(self.array, item)

    def __neg__(self) -> "DenseVector":
        return DenseVector(-self.array)

    def _delegate(op: str) -> Callable[["DenseVector", Any], "DenseVector"]:  # type: ignore[misc]
        def func(self: "DenseVector", other: Any) -> "DenseVector":
            if isinstance(other, DenseVector):
                other = other.array
            return DenseVector(getattr(self.array, op)(other))

        return func

    __add__ = _delegate("__add__")
    __sub__ = _delegate("__sub__")
    __mul__ = _delegate("__mul__")
    __div__ = _delegate("__div__")
    __truediv__ = _delegate("__truediv__")
    __mod__ = _delegate("__mod__")
    __radd__ = _delegate("__radd__")
    __rsub__ = _delegate("__rsub__")
    __rmul__ = _delegate("__rmul__")
    __rdiv__ = _delegate("__rdiv__")
    __rtruediv__ = _delegate("__rtruediv__")
    __rmod__ = _delegate("__rmod__")


class SparseVector(Vector):
    """
    A simple sparse vector class for passing data to MLlib. Users may
    alternatively pass SciPy's {scipy.sparse} data types.
    """

    @overload
    def __init__(self, size: int, __indices: bytes, __values: bytes):
        ...

    @overload
    def __init__(self, size: int, *args: Tuple[int, float]):
        ...

    @overload
    def __init__(self, size: int, __indices: Iterable[int], __values: Iterable[float]):
        ...

    @overload
    def __init__(self, size: int, __pairs: Iterable[Tuple[int, float]]):
        ...

    @overload
    def __init__(self, size: int, __map: Dict[int, float]):
        ...

    def __init__(
        self,
        size: int,
        *args: Union[
            bytes, Tuple[int, float], Iterable[float], Iterable[Tuple[int, float]], Dict[int, float]
        ],
    ):
        """
        Create a sparse vector, using either a dictionary, a list of
        (index, value) pairs, or two separate arrays of indices and
        values (sorted by index).

        Parameters
        ----------
        size : int
            Size of the vector.
        args
            Active entries, as a dictionary {index: value, ...},
            a list of tuples [(index, value), ...], or a list of strictly
            increasing indices and a list of corresponding values [index, ...],
            [value, ...]. Inactive entries are treated as zeros.

        Examples
        --------
        >>> SparseVector(4, {1: 1.0, 3: 5.5})
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> SparseVector(4, [(1, 1.0), (3, 5.5)])
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> SparseVector(4, [1, 3], [1.0, 5.5])
        SparseVector(4, {1: 1.0, 3: 5.5})
        """
        self.size = int(size)
        """ Size of the vector. """
        assert 1 <= len(args) <= 2, "must pass either 2 or 3 arguments"
        if len(args) == 1:
            pairs = args[0]
            if type(pairs) == dict:
                pairs = pairs.items()
            pairs = cast(Iterable[Tuple[int, float]], sorted(pairs))
            self.indices = np.array([p[0] for p in pairs], dtype=np.int32)
            """ A list of indices corresponding to active entries. """
            self.values = np.array([p[1] for p in pairs], dtype=np.float64)
            """ A list of values corresponding to active entries. """
        else:
            if isinstance(args[0], bytes):
                assert isinstance(args[1], bytes), "values should be string too"
                if args[0]:
                    self.indices = np.frombuffer(args[0], np.int32)
                    self.values = np.frombuffer(args[1], np.float64)
                else:
                    # np.frombuffer() doesn't work well with empty string in older version
                    self.indices = np.array([], dtype=np.int32)
                    self.values = np.array([], dtype=np.float64)
            else:
                self.indices = np.array(args[0], dtype=np.int32)
                self.values = np.array(args[1], dtype=np.float64)
            assert len(self.indices) == len(self.values), "index and value arrays not same length"
            for i in range(len(self.indices) - 1):
                if self.indices[i] >= self.indices[i + 1]:
                    raise TypeError(
                        "Indices %s and %s are not strictly increasing"
                        % (self.indices[i], self.indices[i + 1])
                    )

    def numNonzeros(self) -> int:
        """
        Number of nonzero elements. This scans all active values and count non zeros.
        """
        return np.count_nonzero(self.values)

    def norm(self, p: "NormType") -> np.float64:
        """
        Calculates the norm of a SparseVector.

        Examples
        --------
        >>> a = SparseVector(4, [0, 1], [3., -4.])
        >>> a.norm(1)
        7.0
        >>> a.norm(2)
        5.0
        """
        return np.linalg.norm(self.values, p)

    def __reduce__(self) -> Tuple[Type["SparseVector"], Tuple[int, bytes, bytes]]:
        return (
            SparseVector,
            (
                self.size,
                self.indices.tobytes(),
                self.values.tobytes(),
            ),
        )

    @staticmethod
    def parse(s: str) -> "SparseVector":
        """
        Parse string representation back into the SparseVector.

        Examples
        --------
        >>> SparseVector.parse(' (4, [0,1 ],[ 4.0,5.0] )')
        SparseVector(4, {0: 4.0, 1: 5.0})
        """
        start = s.find("(")
        if start == -1:
            raise ValueError("Tuple should start with '('")
        end = s.find(")")
        if end == -1:
            raise ValueError("Tuple should end with ')'")
        s = s[start + 1 : end].strip()

        size = s[: s.find(",")]
        try:
            size = int(size)  # type: ignore[assignment]
        except ValueError:
            raise ValueError("Cannot parse size %s." % size)

        ind_start = s.find("[")
        if ind_start == -1:
            raise ValueError("Indices array should start with '['.")
        ind_end = s.find("]")
        if ind_end == -1:
            raise ValueError("Indices array should end with ']'")
        new_s = s[ind_start + 1 : ind_end]
        ind_list = new_s.split(",")
        try:
            indices = [int(ind) for ind in ind_list if ind]
        except ValueError:
            raise ValueError("Unable to parse indices from %s." % new_s)
        s = s[ind_end + 1 :].strip()

        val_start = s.find("[")
        if val_start == -1:
            raise ValueError("Values array should start with '['.")
        val_end = s.find("]")
        if val_end == -1:
            raise ValueError("Values array should end with ']'.")
        val_list = s[val_start + 1 : val_end].split(",")
        try:
            values = [float(val) for val in val_list if val]
        except ValueError:
            raise ValueError("Unable to parse values from %s." % s)
        return SparseVector(cast(int, size), indices, values)

    def dot(self, other: Iterable[float]) -> np.float64:
        """
        Dot product with a SparseVector or 1- or 2-dimensional Numpy array.

        Examples
        --------
        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.dot(a)
        25.0
        >>> a.dot(array.array('d', [1., 2., 3., 4.]))
        22.0
        >>> b = SparseVector(4, [2], [1.0])
        >>> a.dot(b)
        0.0
        >>> a.dot(np.array([[1, 1], [2, 2], [3, 3], [4, 4]]))
        array([ 22.,  22.])
        >>> a.dot([1., 2., 3.])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> a.dot(np.array([1., 2.]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> a.dot(DenseVector([1., 2.]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> a.dot(np.zeros((3, 2)))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """

        if isinstance(other, np.ndarray):
            if other.ndim not in [2, 1]:
                raise ValueError("Cannot call dot with %d-dimensional array" % other.ndim)
            assert len(self) == other.shape[0], "dimension mismatch"
            return np.dot(self.values, other[self.indices])

        assert len(self) == _vector_size(other), "dimension mismatch"

        if isinstance(other, DenseVector):
            return np.dot(other.array[self.indices], self.values)

        elif isinstance(other, SparseVector):
            # Find out common indices.
            self_cmind = np.isin(self.indices, other.indices, assume_unique=True)
            self_values = self.values[self_cmind]
            if self_values.size == 0:
                return np.float64(0.0)
            else:
                other_cmind = np.isin(other.indices, self.indices, assume_unique=True)
                return np.dot(self_values, other.values[other_cmind])

        else:
            return self.dot(_convert_to_vector(other))  # type: ignore[arg-type]

    def squared_distance(self, other: Iterable[float]) -> np.float64:
        """
        Squared distance from a SparseVector or 1-dimensional NumPy array.

        Examples
        --------
        >>> a = SparseVector(4, [1, 3], [3.0, 4.0])
        >>> a.squared_distance(a)
        0.0
        >>> a.squared_distance(array.array('d', [1., 2., 3., 4.]))
        11.0
        >>> a.squared_distance(np.array([1., 2., 3., 4.]))
        11.0
        >>> b = SparseVector(4, [2], [1.0])
        >>> a.squared_distance(b)
        26.0
        >>> b.squared_distance(a)
        26.0
        >>> b.squared_distance([1., 2.])
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        >>> b.squared_distance(SparseVector(3, [1,], [1.0,]))
        Traceback (most recent call last):
            ...
        AssertionError: dimension mismatch
        """
        assert len(self) == _vector_size(other), "dimension mismatch"

        if isinstance(other, np.ndarray) or isinstance(other, DenseVector):
            if isinstance(other, np.ndarray) and other.ndim != 1:
                raise ValueError(
                    "Cannot call squared_distance with %d-dimensional array" % other.ndim
                )
            if isinstance(other, DenseVector):
                other = other.array
            sparse_ind = np.zeros(other.size, dtype=bool)
            sparse_ind[self.indices] = True
            dist = other[sparse_ind] - self.values
            result = np.dot(dist, dist)

            other_ind = other[~sparse_ind]
            result += np.dot(other_ind, other_ind)
            return result

        elif isinstance(other, SparseVector):
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
        else:
            return self.squared_distance(_convert_to_vector(other))  # type: ignore[arg-type]

    def toArray(self) -> np.ndarray:
        """
        Returns a copy of this SparseVector as a 1-dimensional NumPy array.
        """
        arr = np.zeros((self.size,), dtype=np.float64)
        arr[self.indices] = self.values
        return arr

    def asML(self) -> newlinalg.SparseVector:
        """
        Convert this vector to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        .. versionadded:: 2.0.0

        Returns
        -------
        :py:class:`pyspark.ml.linalg.SparseVector`
        """
        return newlinalg.SparseVector(self.size, self.indices, self.values)

    def __len__(self) -> int:
        return self.size

    def __str__(self) -> str:
        inds = "[" + ",".join([str(i) for i in self.indices]) + "]"
        vals = "[" + ",".join([str(v) for v in self.values]) + "]"
        return "(" + ",".join((str(self.size), inds, vals)) + ")"

    def __repr__(self) -> str:
        inds = self.indices
        vals = self.values
        entries = ", ".join(
            ["{0}: {1}".format(inds[i], _format_float(vals[i])) for i in range(len(inds))]
        )
        return "SparseVector({0}, {{{1}}})".format(self.size, entries)

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, SparseVector):
            return (
                other.size == self.size
                and np.array_equal(other.indices, self.indices)
                and np.array_equal(other.values, self.values)
            )
        elif isinstance(other, DenseVector):
            if self.size != len(other):
                return False
            return Vectors._equals(self.indices, self.values, list(range(len(other))), other.array)
        return False

    def __getitem__(self, index: int) -> np.float64:
        inds = self.indices
        vals = self.values
        if not isinstance(index, int):
            raise TypeError("Indices must be of type integer, got type %s" % type(index))

        if index >= self.size or index < -self.size:
            raise IndexError("Index %d out of bounds." % index)
        if index < 0:
            index += self.size

        if (inds.size == 0) or (index > inds.item(-1)):
            return np.float64(0.0)

        insert_index = np.searchsorted(inds, index)
        row_ind = inds[insert_index]
        if row_ind == index:
            return vals[insert_index]
        return np.float64(0.0)

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def __hash__(self) -> int:
        result = 31 + self.size
        nnz = 0
        i = 0
        while i < len(self.values) and nnz < 128:
            if self.values[i] != 0:
                result = 31 * result + int(self.indices[i])
                bits = _double_to_long_bits(self.values[i])
                result = 31 * result + (bits ^ (bits >> 32))
                nnz += 1
            i += 1
        return result


class Vectors:

    """
    Factory methods for working with vectors.

    Notes
    -----
    Dense vectors are simply represented as NumPy array objects,
    so there is no need to convert them for use in MLlib. For sparse vectors,
    the factory methods in this class create an MLlib-compatible type, or users
    can pass in SciPy's `scipy.sparse` column vectors.
    """

    @staticmethod
    @overload
    def sparse(size: int, __indices: bytes, __values: bytes) -> SparseVector:
        ...

    @staticmethod
    @overload
    def sparse(size: int, *args: Tuple[int, float]) -> SparseVector:
        ...

    @staticmethod
    @overload
    def sparse(size: int, __indices: Iterable[int], __values: Iterable[float]) -> SparseVector:
        ...

    @staticmethod
    @overload
    def sparse(size: int, __pairs: Iterable[Tuple[int, float]]) -> SparseVector:
        ...

    @staticmethod
    @overload
    def sparse(size: int, __map: Dict[int, float]) -> SparseVector:
        ...

    @staticmethod
    def sparse(
        size: int,
        *args: Union[
            bytes, Tuple[int, float], Iterable[float], Iterable[Tuple[int, float]], Dict[int, float]
        ],
    ) -> SparseVector:
        """
        Create a sparse vector, using either a dictionary, a list of
        (index, value) pairs, or two separate arrays of indices and
        values (sorted by index).

        Parameters
        ----------
        size : int
            Size of the vector.
        args
            Non-zero entries, as a dictionary, list of tuples,
            or two sorted lists containing indices and values.

        Examples
        --------
        >>> Vectors.sparse(4, {1: 1.0, 3: 5.5})
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> Vectors.sparse(4, [(1, 1.0), (3, 5.5)])
        SparseVector(4, {1: 1.0, 3: 5.5})
        >>> Vectors.sparse(4, [1, 3], [1.0, 5.5])
        SparseVector(4, {1: 1.0, 3: 5.5})
        """
        return SparseVector(size, *args)  # type: ignore[arg-type]

    @overload
    @staticmethod
    def dense(*elements: float) -> DenseVector:
        ...

    @overload
    @staticmethod
    def dense(__arr: bytes) -> DenseVector:
        ...

    @overload
    @staticmethod
    def dense(__arr: Iterable[float]) -> DenseVector:
        ...

    @staticmethod
    def dense(*elements: Union[float, bytes, np.ndarray, Iterable[float]]) -> DenseVector:
        """
        Create a dense vector of 64-bit floats from a Python list or numbers.

        Examples
        --------
        >>> Vectors.dense([1, 2, 3])
        DenseVector([1.0, 2.0, 3.0])
        >>> Vectors.dense(1.0, 2.0)
        DenseVector([1.0, 2.0])
        """
        if len(elements) == 1 and not isinstance(elements[0], (float, int)):
            # it's list, numpy.array or other iterable object.
            elements = elements[0]  # type: ignore[assignment]
        return DenseVector(cast(Iterable[float], elements))

    @staticmethod
    def fromML(vec: newlinalg.DenseVector) -> DenseVector:
        """
        Convert a vector from the new mllib-local representation.
        This does NOT copy the data; it copies references.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        vec : :py:class:`pyspark.ml.linalg.Vector`

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Vector`
        """
        if isinstance(vec, newlinalg.DenseVector):
            return DenseVector(vec.array)
        elif isinstance(vec, newlinalg.SparseVector):
            return SparseVector(vec.size, vec.indices, vec.values)
        else:
            raise TypeError("Unsupported vector type %s" % type(vec))

    @staticmethod
    def stringify(vector: Vector) -> str:
        """
        Converts a vector into a string, which can be recognized by
        Vectors.parse().

        Examples
        --------
        >>> Vectors.stringify(Vectors.sparse(2, [1], [1.0]))
        '(2,[1],[1.0])'
        >>> Vectors.stringify(Vectors.dense([0.0, 1.0]))
        '[0.0,1.0]'
        """
        return str(vector)

    @staticmethod
    def squared_distance(v1: Vector, v2: Vector) -> np.float64:
        """
        Squared distance between two vectors.
        a and b can be of type SparseVector, DenseVector, np.ndarray
        or array.array.

        Examples
        --------
        >>> a = Vectors.sparse(4, [(0, 1), (3, 4)])
        >>> b = Vectors.dense([2, 5, 4, 1])
        >>> a.squared_distance(b)
        51.0
        """
        v1, v2 = _convert_to_vector(v1), _convert_to_vector(v2)
        return v1.squared_distance(v2)  # type: ignore[attr-defined]

    @staticmethod
    def norm(vector: Vector, p: "NormType") -> np.float64:
        """
        Find norm of the given vector.
        """
        return _convert_to_vector(vector).norm(p)  # type: ignore[attr-defined]

    @staticmethod
    def parse(s: str) -> Vector:
        """Parse a string representation back into the Vector.

        Examples
        --------
        >>> Vectors.parse('[2,1,2 ]')
        DenseVector([2.0, 1.0, 2.0])
        >>> Vectors.parse(' ( 100,  [0],  [2])')
        SparseVector(100, {0: 2.0})
        """
        if s.find("(") == -1 and s.find("[") != -1:
            return DenseVector.parse(s)
        elif s.find("(") != -1:
            return SparseVector.parse(s)
        else:
            raise ValueError("Cannot find tokens '[' or '(' from the input string.")

    @staticmethod
    def zeros(size: int) -> DenseVector:
        return DenseVector(np.zeros(size))

    @staticmethod
    def _equals(
        v1_indices: Union[Sequence[int], np.ndarray],
        v1_values: Union[Sequence[float], np.ndarray],
        v2_indices: Union[Sequence[int], np.ndarray],
        v2_values: Union[Sequence[float], np.ndarray],
    ) -> bool:
        """
        Check equality between sparse/dense vectors,
        v1_indices and v2_indices assume to be strictly increasing.
        """
        v1_size = len(v1_values)
        v2_size = len(v2_values)
        k1 = 0
        k2 = 0
        all_equal = True
        while all_equal:
            while k1 < v1_size and v1_values[k1] == 0:
                k1 += 1
            while k2 < v2_size and v2_values[k2] == 0:
                k2 += 1

            if k1 >= v1_size or k2 >= v2_size:
                return k1 >= v1_size and k2 >= v2_size

            all_equal = v1_indices[k1] == v2_indices[k2] and v1_values[k1] == v2_values[k2]
            k1 += 1
            k2 += 1
        return all_equal


class Matrix:
    __UDT__ = MatrixUDT()

    """
    Represents a local matrix.
    """

    def __init__(self, numRows: int, numCols: int, isTransposed: bool = False) -> None:
        self.numRows = numRows
        self.numCols = numCols
        self.isTransposed = isTransposed

    def toArray(self) -> np.ndarray:
        """
        Returns its elements in a NumPy ndarray.
        """
        raise NotImplementedError

    def asML(self) -> newlinalg.Matrix:
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.
        """
        raise NotImplementedError

    @staticmethod
    def _convert_to_array(array_like: Union[bytes, Iterable[float]], dtype: Any) -> np.ndarray:
        """
        Convert Matrix attributes which are array-like or buffer to array.
        """
        if isinstance(array_like, bytes):
            return np.frombuffer(array_like, dtype=dtype)
        return np.asarray(array_like, dtype=dtype)


class DenseMatrix(Matrix):
    """
    Column-major dense matrix.
    """

    def __init__(
        self,
        numRows: int,
        numCols: int,
        values: Union[bytes, Iterable[float]],
        isTransposed: bool = False,
    ):
        Matrix.__init__(self, numRows, numCols, isTransposed)
        values = self._convert_to_array(values, np.float64)
        assert len(values) == numRows * numCols
        self.values = values

    def __reduce__(self) -> Tuple[Type["DenseMatrix"], Tuple[int, int, bytes, int]]:
        return DenseMatrix, (
            self.numRows,
            self.numCols,
            self.values.tobytes(),
            int(self.isTransposed),
        )

    def __str__(self) -> str:
        """
        Pretty printing of a DenseMatrix

        Examples
        --------
        >>> dm = DenseMatrix(2, 2, range(4))
        >>> print(dm)
        DenseMatrix([[ 0.,  2.],
                     [ 1.,  3.]])
        >>> dm = DenseMatrix(2, 2, range(4), isTransposed=True)
        >>> print(dm)
        DenseMatrix([[ 0.,  1.],
                     [ 2.,  3.]])
        """
        # Inspired by __repr__ in scipy matrices.
        array_lines = repr(self.toArray()).splitlines()

        # We need to adjust six spaces which is the difference in number
        # of letters between "DenseMatrix" and "array"
        x = "\n".join([(" " * 6 + line) for line in array_lines[1:]])
        return array_lines[0].replace("array", "DenseMatrix") + "\n" + x

    def __repr__(self) -> str:
        """
        Representation of a DenseMatrix

        Examples
        --------
        >>> dm = DenseMatrix(2, 2, range(4))
        >>> dm
        DenseMatrix(2, 2, [0.0, 1.0, 2.0, 3.0], False)
        """
        # If the number of values are less than seventeen then return as it is.
        # Else return first eight values and last eight values.
        if len(self.values) < 17:
            entries = _format_float_list(self.values)
        else:
            entries = (
                _format_float_list(self.values[:8]) + ["..."] + _format_float_list(self.values[-8:])
            )

        return "DenseMatrix({0}, {1}, [{2}], {3})".format(
            self.numRows, self.numCols, ", ".join(entries), self.isTransposed
        )

    def toArray(self) -> np.ndarray:
        """
        Return an numpy.ndarray

        Examples
        --------
        >>> m = DenseMatrix(2, 2, range(4))
        >>> m.toArray()
        array([[ 0.,  2.],
               [ 1.,  3.]])
        """
        if self.isTransposed:
            return np.asfortranarray(self.values.reshape((self.numRows, self.numCols)))
        else:
            return self.values.reshape((self.numRows, self.numCols), order="F")

    def toSparse(self) -> "SparseMatrix":
        """Convert to SparseMatrix"""
        if self.isTransposed:
            values = np.ravel(self.toArray(), order="F")
        else:
            values = self.values
        indices = np.nonzero(values)[0]
        colCounts = np.bincount(indices // self.numRows)
        colPtrs = np.cumsum(np.hstack((0, colCounts, np.zeros(self.numCols - colCounts.size))))
        values = values[indices]
        rowIndices = indices % self.numRows

        return SparseMatrix(self.numRows, self.numCols, colPtrs, rowIndices, values)

    def asML(self) -> newlinalg.DenseMatrix:
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        .. versionadded:: 2.0.0

        Returns
        -------
        :py:class:`pyspark.ml.linalg.DenseMatrix`
        """
        return newlinalg.DenseMatrix(self.numRows, self.numCols, self.values, self.isTransposed)

    def __getitem__(self, indices: Tuple[int, int]) -> np.float64:
        i, j = indices
        if i < 0 or i >= self.numRows:
            raise IndexError("Row index %d is out of range [0, %d)" % (i, self.numRows))
        if j >= self.numCols or j < 0:
            raise IndexError("Column index %d is out of range [0, %d)" % (j, self.numCols))

        if self.isTransposed:
            return self.values[i * self.numCols + j]
        else:
            return self.values[i + j * self.numRows]

    def __eq__(self, other: Any) -> bool:
        if self.numRows != other.numRows or self.numCols != other.numCols:
            return False
        if isinstance(other, SparseMatrix):
            return np.all(self.toArray() == other.toArray()).tolist()

        self_values = np.ravel(self.toArray(), order="F")
        other_values = np.ravel(other.toArray(), order="F")
        return np.all(self_values == other_values).tolist()


class SparseMatrix(Matrix):
    """Sparse Matrix stored in CSC format."""

    def __init__(
        self,
        numRows: int,
        numCols: int,
        colPtrs: Union[bytes, Iterable[int]],
        rowIndices: Union[bytes, Iterable[int]],
        values: Union[bytes, Iterable[float]],
        isTransposed: bool = False,
    ) -> None:
        Matrix.__init__(self, numRows, numCols, isTransposed)
        self.colPtrs = self._convert_to_array(colPtrs, np.int32)
        self.rowIndices = self._convert_to_array(rowIndices, np.int32)
        self.values = self._convert_to_array(values, np.float64)

        if self.isTransposed:
            if self.colPtrs.size != numRows + 1:
                raise ValueError(
                    "Expected colPtrs of size %d, got %d." % (numRows + 1, self.colPtrs.size)
                )
        else:
            if self.colPtrs.size != numCols + 1:
                raise ValueError(
                    "Expected colPtrs of size %d, got %d." % (numCols + 1, self.colPtrs.size)
                )
        if self.rowIndices.size != self.values.size:
            raise ValueError(
                "Expected rowIndices of length %d, got %d."
                % (self.rowIndices.size, self.values.size)
            )

    def __str__(self) -> str:
        """
        Pretty printing of a SparseMatrix

        Examples
        --------
        >>> sm1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        >>> print(sm1)
        2 X 2 CSCMatrix
        (0,0) 2.0
        (1,0) 3.0
        (1,1) 4.0
        >>> sm1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4], True)
        >>> print(sm1)
        2 X 2 CSRMatrix
        (0,0) 2.0
        (0,1) 3.0
        (1,1) 4.0
        """
        spstr = "{0} X {1} ".format(self.numRows, self.numCols)
        if self.isTransposed:
            spstr += "CSRMatrix\n"
        else:
            spstr += "CSCMatrix\n"

        cur_col = 0
        smlist = []

        # Display first 16 values.
        if len(self.values) <= 16:
            zipindval = zip(self.rowIndices, self.values)
        else:
            zipindval = zip(self.rowIndices[:16], self.values[:16])
        for i, (rowInd, value) in enumerate(zipindval):
            if self.colPtrs[cur_col + 1] <= i:
                cur_col += 1
            if self.isTransposed:
                smlist.append("({0},{1}) {2}".format(cur_col, rowInd, _format_float(value)))
            else:
                smlist.append("({0},{1}) {2}".format(rowInd, cur_col, _format_float(value)))
        spstr += "\n".join(smlist)

        if len(self.values) > 16:
            spstr += "\n.." * 2
        return spstr

    def __repr__(self) -> str:
        """
        Representation of a SparseMatrix

        Examples
        --------
        >>> sm1 = SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2, 3, 4])
        >>> sm1
        SparseMatrix(2, 2, [0, 2, 3], [0, 1, 1], [2.0, 3.0, 4.0], False)
        """
        rowIndices = list(self.rowIndices)
        colPtrs = list(self.colPtrs)

        if len(self.values) <= 16:
            values = _format_float_list(self.values)

        else:
            values = (
                _format_float_list(self.values[:8]) + ["..."] + _format_float_list(self.values[-8:])
            )
            rowIndices = rowIndices[:8] + ["..."] + rowIndices[-8:]

        if len(self.colPtrs) > 16:
            colPtrs = colPtrs[:8] + ["..."] + colPtrs[-8:]

        return "SparseMatrix({0}, {1}, [{2}], [{3}], [{4}], {5})".format(
            self.numRows,
            self.numCols,
            ", ".join([str(ptr) for ptr in colPtrs]),
            ", ".join([str(ind) for ind in rowIndices]),
            ", ".join(values),
            self.isTransposed,
        )

    def __reduce__(self) -> Tuple[Type["SparseMatrix"], Tuple[int, int, bytes, bytes, bytes, int]]:
        return SparseMatrix, (
            self.numRows,
            self.numCols,
            self.colPtrs.tobytes(),
            self.rowIndices.tobytes(),
            self.values.tobytes(),
            int(self.isTransposed),
        )

    def __getitem__(self, indices: Tuple[int, int]) -> np.float64:
        i, j = indices
        if i < 0 or i >= self.numRows:
            raise IndexError("Row index %d is out of range [0, %d)" % (i, self.numRows))
        if j < 0 or j >= self.numCols:
            raise IndexError("Column index %d is out of range [0, %d)" % (j, self.numCols))

        # If a CSR matrix is given, then the row index should be searched
        # for in ColPtrs, and the column index should be searched for in the
        # corresponding slice obtained from rowIndices.
        if self.isTransposed:
            j, i = i, j

        colStart = self.colPtrs[j]
        colEnd = self.colPtrs[j + 1]
        nz = self.rowIndices[colStart:colEnd]
        ind = np.searchsorted(nz, i) + colStart
        if ind < colEnd and self.rowIndices[ind] == i:
            return self.values[ind]
        else:
            return np.float64(0.0)

    def toArray(self) -> np.ndarray:
        """
        Return an numpy.ndarray
        """
        A = np.zeros((self.numRows, self.numCols), dtype=np.float64, order="F")
        for k in range(self.colPtrs.size - 1):
            startptr = self.colPtrs[k]
            endptr = self.colPtrs[k + 1]
            if self.isTransposed:
                A[k, self.rowIndices[startptr:endptr]] = self.values[startptr:endptr]
            else:
                A[self.rowIndices[startptr:endptr], k] = self.values[startptr:endptr]
        return A

    def toDense(self) -> "DenseMatrix":
        densevals = np.ravel(self.toArray(), order="F")
        return DenseMatrix(self.numRows, self.numCols, densevals)

    def asML(self) -> newlinalg.SparseMatrix:
        """
        Convert this matrix to the new mllib-local representation.
        This does NOT copy the data; it copies references.

        .. versionadded:: 2.0.0

        Returns
        -------
        :py:class:`pyspark.ml.linalg.SparseMatrix`
        """
        return newlinalg.SparseMatrix(
            self.numRows,
            self.numCols,
            self.colPtrs,
            self.rowIndices,
            self.values,
            self.isTransposed,
        )

    # TODO: More efficient implementation:
    def __eq__(self, other: Any) -> bool:
        assert isinstance(other, Matrix)
        return np.all(self.toArray() == other.toArray()).tolist()


class Matrices:
    @staticmethod
    def dense(numRows: int, numCols: int, values: Union[bytes, Iterable[float]]) -> DenseMatrix:
        """
        Create a DenseMatrix
        """
        return DenseMatrix(numRows, numCols, values)

    @staticmethod
    def sparse(
        numRows: int,
        numCols: int,
        colPtrs: Union[bytes, Iterable[int]],
        rowIndices: Union[bytes, Iterable[int]],
        values: Union[bytes, Iterable[float]],
    ) -> SparseMatrix:
        """
        Create a SparseMatrix
        """
        return SparseMatrix(numRows, numCols, colPtrs, rowIndices, values)

    @staticmethod
    def fromML(mat: newlinalg.Matrix) -> Matrix:
        """
        Convert a matrix from the new mllib-local representation.
        This does NOT copy the data; it copies references.

        .. versionadded:: 2.0.0

        Parameters
        ----------
        mat : :py:class:`pyspark.ml.linalg.Matrix`

        Returns
        -------
        :py:class:`pyspark.mllib.linalg.Matrix`
        """
        if isinstance(mat, newlinalg.DenseMatrix):
            return DenseMatrix(mat.numRows, mat.numCols, mat.values, mat.isTransposed)
        elif isinstance(mat, newlinalg.SparseMatrix):
            return SparseMatrix(
                mat.numRows, mat.numCols, mat.colPtrs, mat.rowIndices, mat.values, mat.isTransposed
            )
        else:
            raise TypeError("Unsupported matrix type %s" % type(mat))


class QRDecomposition(Generic[QT, RT]):
    """
    Represents QR factors.
    """

    def __init__(self, Q: QT, R: RT) -> None:
        self._Q = Q
        self._R = R

    @property
    @since("2.0.0")
    def Q(self) -> QT:
        """
        An orthogonal matrix Q in a QR decomposition.
        May be null if not computed.
        """
        return self._Q

    @property
    @since("2.0.0")
    def R(self) -> RT:
        """
        An upper triangular matrix R in a QR decomposition.
        """
        return self._R


def _test() -> None:
    import doctest
    import numpy

    try:
        # Numpy 1.14+ changed it's string format.
        numpy.set_printoptions(legacy="1.13")
    except TypeError:
        pass
    (failure_count, test_count) = doctest.testmod(optionflags=doctest.ELLIPSIS)
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
