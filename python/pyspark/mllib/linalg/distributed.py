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
MLlib utilities for distributed linear algebra.
"""

import sys

if sys.version >= '3':
    long = int

from pyspark import RDD
from pyspark.mllib.common import callMLlibFunc, JavaModelWrapper
from pyspark.mllib.linalg import _convert_to_vector


__all__ = ['DistributedMatrix', 'RowMatrix', 'IndexedRow',
           'IndexedRowMatrix', 'MatrixEntry', 'CoordinateMatrix']


class DistributedMatrix(object):
    """
    Represents a distributively stored matrix backed by one or
    more RDDs.

    """
    def numRows(self):
        """Get or compute the number of rows."""
        raise NotImplementedError

    def numCols(self):
        """Get or compute the number of cols."""
        raise NotImplementedError


class RowMatrix(DistributedMatrix):
    """
    .. note:: Experimental

    Represents a row-oriented distributed Matrix with no meaningful
    row indices.

    :param rows: An RDD of vectors.
    :param numRows: Number of rows in the matrix. A non-positive
                    value means unknown, at which point the number
                    of rows will be determined by the number of
                    records in the `rows` RDD.
    :param numCols: Number of columns in the matrix. A non-positive
                    value means unknown, at which point the number
                    of columns will be determined by the size of
                    the first row.
    """
    def __init__(self, rows, numRows=0, numCols=0):
        """Create a wrapper over a Java RowMatrix."""
        if not isinstance(rows, RDD):
            raise TypeError("rows should be an RDD of vectors, got %s" % type(rows))
        rows = rows.map(_convert_to_vector)

        javaRowMatrix = callMLlibFunc("createRowMatrix", rows, long(numRows), int(numCols))
        self._jrm = JavaModelWrapper(javaRowMatrix)
        self._rows = rows

    @property
    def rows(self):
        """Rows of the RowMatrix stored as an RDD of vectors."""
        return self._rows

    @staticmethod
    def _from_java(javaRowMatrix):
        """Create a PySpark RowMatrix from a Java RowMatrix object."""
        rows = JavaModelWrapper(javaRowMatrix).call("rows")
        return RowMatrix(rows)

    def numRows(self):
        """
        Get or compute the number of rows.

        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6],
        ...                        [7, 8, 9], [10, 11, 12]])

        >>> rm = RowMatrix(rows)
        >>> print(rm.numRows())
        4

        >>> rm = RowMatrix(rows, 7, 6)
        >>> print(rm.numRows())
        7
        """
        return self._jrm.call("numRows")

    def numCols(self):
        """
        Get or compute the number of cols.

        >>> rows = sc.parallelize([[1, 2, 3], [4, 5, 6],
        ...                        [7, 8, 9], [10, 11, 12]])

        >>> rm = RowMatrix(rows)
        >>> print(rm.numCols())
        3

        >>> rm = RowMatrix(rows, 7, 6)
        >>> print(rm.numCols())
        6
        """
        return self._jrm.call("numCols")


class IndexedRow(object):
    """
    .. note:: Experimental

    Represents a row of an IndexedRowMatrix.

    Just a wrapper over a (long, vector) tuple.

    :param index: The index for the given row.
    :param vector: The row in the matrix at the given index.
    """
    def __init__(self, index, vector):
        self.index = long(index)
        self.vector = _convert_to_vector(vector)

    def __repr__(self):
        return "IndexedRow(%s, %s)" % (self.index, self.vector)


def _convert_to_indexed_row(row):
    if isinstance(row, IndexedRow):
        return row
    elif isinstance(row, tuple) and len(row) == 2:
        return IndexedRow(*row)
    else:
        raise TypeError("Cannot convert type %s into IndexedRow" % type(row))


class IndexedRowMatrix(DistributedMatrix):
    """
    .. note:: Experimental

    Represents a row-oriented distributed Matrix with indexed rows.

    :param rows: An RDD of IndexedRows or (long, vector) tuples.
    :param numRows: Number of rows in the matrix. A non-positive
                    value means unknown, at which point the number
                    of rows will be determined by the max row
                    index plus one.
    :param numCols: Number of columns in the matrix. A non-positive
                    value means unknown, at which point the number
                    of columns will be determined by the size of
                    the first row.
    """
    def __init__(self, rows, numRows=0, numCols=0):
        """Create a wrapper over a Java IndexedRowMatrix."""
        if not isinstance(rows, RDD):
            raise TypeError("rows should be an RDD of IndexedRows or (long, vector) tuples, "
                            "got %s" % type(rows))
        rows = rows.map(_convert_to_indexed_row)

        # We use DataFrames for serialization of IndexedRows from
        # Python, so first convert the RDD to a DataFrame on this side.
        # This will convert each IndexedRow to a Row containing the
        # 'index' and 'vector' values, which can both be easily
        # serialized.  We will convert back to IndexedRows on the
        # Scala side.
        javaIndexedRowMatrix = callMLlibFunc("createIndexedRowMatrix", rows.toDF(),
                                             long(numRows), int(numCols))
        self._jirm = JavaModelWrapper(javaIndexedRowMatrix)
        self._rows = rows

    @property
    def rows(self):
        """
        Rows of the IndexedRowMatrix stored as an RDD of IndexedRows.
        """
        return self._rows

    @staticmethod
    def _from_java(javaIndexedRowMatrix):
        """Create a PySpark IndexedRowMatrix from a Java IndexedRowMatrix object."""
        # We use DataFrames for serialization of IndexedRows from
        # Java, so we first convert the RDD of rows to a DataFrame
        # on the Scala/Java side. Then we map each Row in the
        # DataFrame back to an IndexedRow on this side.
        rowsDF = callMLlibFunc("getIndexedRows", javaIndexedRowMatrix)
        rows = rowsDF.map(lambda row: IndexedRow(row[0], row[1]))
        return IndexedRowMatrix(rows)

    def numRows(self):
        """
        Get or compute the number of rows.

        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(1, [4, 5, 6]),
        ...                        IndexedRow(2, [7, 8, 9]),
        ...                        IndexedRow(3, [10, 11, 12])])

        >>> rm = IndexedRowMatrix(rows)
        >>> print(rm.numRows())
        4

        >>> rm = IndexedRowMatrix(rows, 7, 6)
        >>> print(rm.numRows())
        7
        """
        return self._jirm.call("numRows")

    def numCols(self):
        """
        Get or compute the number of cols.

        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(1, [4, 5, 6]),
        ...                        IndexedRow(2, [7, 8, 9]),
        ...                        IndexedRow(3, [10, 11, 12])])

        >>> rm = IndexedRowMatrix(rows)
        >>> print(rm.numCols())
        3

        >>> rm = IndexedRowMatrix(rows, 7, 6)
        >>> print(rm.numCols())
        6
        """
        return self._jirm.call("numCols")

    def toRowMatrix(self):
        """
        Convert this matrix to a RowMatrix.

        >>> rows = sc.parallelize([IndexedRow(0, [1, 2, 3]),
        ...                        IndexedRow(6, [4, 5, 6])])
        >>> rm = IndexedRowMatrix(rows).toRowMatrix()
        >>> rm.rows.collect()
        [DenseVector([1.0, 2.0, 3.0]), DenseVector([4.0, 5.0, 6.0])]
        """
        javaRowMatrix = self._jirm.call("toRowMatrix")
        return RowMatrix._from_java(javaRowMatrix)

    def toCoordinateMatrix(self):
        """
        Convert this matrix to a CoordinateMatrix.

        >>> rows = sc.parallelize([IndexedRow(0, [1, 0]),
        ...                        IndexedRow(6, [0, 5])])
        >>> cm = IndexedRowMatrix(rows).toCoordinateMatrix()
        >>> cm.entries.take(3)
        [MatrixEntry(0, 0, 1.0), MatrixEntry(0, 1, 0.0), MatrixEntry(6, 0, 0.0)]
        """
        javaCoordinateMatrix = self._jirm.call("toCoordinateMatrix")
        return CoordinateMatrix._from_java(javaCoordinateMatrix)


class MatrixEntry(object):
    """
    .. note:: Experimental

    Represents an entry of a CoordinateMatrix.

    Just a wrapper over a (long, long, float) tuple.

    :param i: The row index of the matrix.
    :param j: The column index of the matrix.
    :param value: The (i, j)th entry of the matrix, as a float.
    """
    def __init__(self, i, j, value):
        self.i = long(i)
        self.j = long(j)
        self.value = float(value)

    def __repr__(self):
        return "MatrixEntry(%s, %s, %s)" % (self.i, self.j, self.value)


def _convert_to_matrix_entry(entry):
    if isinstance(entry, MatrixEntry):
        return entry
    elif isinstance(entry, tuple) and len(entry) == 3:
        return MatrixEntry(*entry)
    else:
        raise TypeError("Cannot convert type %s into MatrixEntry" % type(entry))


class CoordinateMatrix(DistributedMatrix):
    """
    .. note:: Experimental

    Represents a matrix in coordinate format.

    :param entries: An RDD of MatrixEntry inputs or
                    (long, long, float) tuples.
    :param numRows: Number of rows in the matrix. A non-positive
                    value means unknown, at which point the number
                    of rows will be determined by the max row
                    index plus one.
    :param numCols: Number of columns in the matrix. A non-positive
                    value means unknown, at which point the number
                    of columns will be determined by the max row
                    index plus one.
    """
    def __init__(self, entries, numRows=0, numCols=0):
        """Create a wrapper over a Java CoordinateMatrix."""
        if not isinstance(entries, RDD):
            raise TypeError("entries should be an RDD of MatrixEntry entries or "
                            "(long, long, float) tuples, got %s" % type(entries))
        entries = entries.map(_convert_to_matrix_entry)

        # We use DataFrames for serialization of MatrixEntry entries
        # from Python, so first convert the RDD to a DataFrame on this
        # side. This will convert each MatrixEntry to a Row containing
        # the 'i', 'j', and 'value' values, which can each be easily
        # serialized. We will convert back to MatrixEntry inputs on
        # the Scala side.
        javaCoordinateMatrix = callMLlibFunc("createCoordinateMatrix", entries.toDF(),
                                             long(numRows), long(numCols))
        self._jcm = JavaModelWrapper(javaCoordinateMatrix)
        self._entries = entries

    @property
    def entries(self):
        """
        Entries of the CoordinateMatrix stored as an RDD of MatrixEntries.
        """
        return self._entries

    @staticmethod
    def _from_java(javaCoordinateMatrix):
        """Create a PySpark CoordinateMatrix from a Java CoordinateMatrix object."""
        # We use DataFrames for serialization of MatrixEntry entries
        # from Java, so we first convert the RDD of entries to a
        # DataFrame on the Scala/Java side. Then we map each Row in
        # the DataFrame back to a MatrixEntry on this side.
        entriesDF = callMLlibFunc("getMatrixEntries", javaCoordinateMatrix)
        entries = entriesDF.map(lambda row: MatrixEntry(row[0], row[1], row[2]))
        return CoordinateMatrix(entries)

    def numRows(self):
        """
        Get or compute the number of rows.

        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(1, 0, 2),
        ...                           MatrixEntry(2, 1, 3.7)])

        >>> cm = CoordinateMatrix(entries)
        >>> print(cm.numRows())
        3

        >>> cm = CoordinateMatrix(entries, 7, 6)
        >>> print(cm.numRows())
        7
        """
        return self._jcm.call("numRows")

    def numCols(self):
        """
        Get or compute the number of cols.

        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2),
        ...                           MatrixEntry(1, 0, 2),
        ...                           MatrixEntry(2, 1, 3.7)])

        >>> cm = CoordinateMatrix(entries)
        >>> print(cm.numCols())
        2

        >>> cm = CoordinateMatrix(entries, 7, 6)
        >>> print(cm.numCols())
        6
        """
        return self._jcm.call("numCols")

    def toRowMatrix(self):
        """
        Convert this matrix to a RowMatrix.

        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2), MatrixEntry(6, 4, 2.1)])

        >>> # This CoordinateMatrix will have 7 effective rows, due to
        >>> # the highest row index being 6, but the ensuing RowMatrix
        >>> # will only have 2 rows since there are only entries on 2
        >>> # unique rows.
        >>> rm = CoordinateMatrix(entries).toRowMatrix()
        >>> print(rm.numRows())
        2

        >>> # This CoordinateMatrix will have 5 columns, due to the
        >>> # highest column index being 4, and the ensuing RowMatrix
        >>> # will have 5 columns as well.
        >>> rm = CoordinateMatrix(entries).toRowMatrix()
        >>> print(rm.numCols())
        5
        """
        javaRowMatrix = self._jcm.call("toRowMatrix")
        return RowMatrix._from_java(javaRowMatrix)

    def toIndexedRowMatrix(self):
        """
        Convert this matrix to an IndexedRowMatrix.

        >>> entries = sc.parallelize([MatrixEntry(0, 0, 1.2), MatrixEntry(6, 4, 2.1)])

        >>> # This CoordinateMatrix will have 7 effective rows, due to
        >>> # the highest row index being 6, and the ensuing
        >>> # IndexedRowMatrix will have 7 rows as well.
        >>> irm = CoordinateMatrix(entries).toIndexedRowMatrix()
        >>> print(irm.numRows())
        7

        >>> # This CoordinateMatrix will have 5 columns, due to the
        >>> # highest column index being 4, and the ensuing
        >>> # IndexedRowMatrix will have 5 columns as well.
        >>> irm = CoordinateMatrix(entries).toIndexedRowMatrix()
        >>> print(irm.numCols())
        5
        """
        javaIndexedRowMatrix = self._jcm.call("toIndexedRowMatrix")
        return IndexedRowMatrix._from_java(javaIndexedRowMatrix)


def _test():
    import doctest
    from pyspark import SparkContext
    from pyspark.sql import SQLContext
    import pyspark.mllib.linalg.distributed
    globs = pyspark.mllib.linalg.distributed.__dict__.copy()
    globs['sc'] = SparkContext('local[2]', 'PythonTest', batchSize=2)
    globs['sqlContext'] = SQLContext(globs['sc'])
    (failure_count, test_count) = doctest.testmod(globs=globs, optionflags=doctest.ELLIPSIS)
    globs['sc'].stop()
    if failure_count:
        exit(-1)

if __name__ == "__main__":
    _test()
