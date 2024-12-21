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
A collections of partitioning functions
"""

from typing import Sequence, TYPE_CHECKING

from pyspark.errors import PySparkValueError
from pyspark.sql.column import Column
from pyspark.sql.utils import _try_remote_hash_functions

if TYPE_CHECKING:
    from pyspark.sql._typing import ColumnOrName


__all__: Sequence[str] = []


@_try_remote_hash_functions
def crc32(col: "ColumnOrName") -> Column:
    """
    Calculates the cyclic redundancy check value (CRC32) of a binary column and
    returns the value as a bigint.

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or column name
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    .. versionadded:: 1.5.0

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([('ABC',)], ['a'])
    >>> df.select('*', sf.crc32('a')).show(truncate=False)
    +---+----------+
    |a  |crc32(a)  |
    +---+----------+
    |ABC|2743272264|
    +---+----------+
    """
    from pyspark.sql.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("crc32", col)


@_try_remote_hash_functions
def hash(*cols: "ColumnOrName") -> Column:
    """Calculates the hash code of given columns, and returns the result as an int column.

    .. versionadded:: 2.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or column name
        one or more columns to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hash value as int column.

    See Also
    --------
    :meth:`pyspark.sql.functions.xxhash64`

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([('ABC', 'DEF')], ['c1', 'c2'])
    >>> df.select('*', sf.hash('c1')).show()
    +---+---+----------+
    | c1| c2|  hash(c1)|
    +---+---+----------+
    |ABC|DEF|-757602832|
    +---+---+----------+

    >>> df.select('*', sf.hash('c1', df.c2)).show()
    +---+---+------------+
    | c1| c2|hash(c1, c2)|
    +---+---+------------+
    |ABC|DEF|   599895104|
    +---+---+------------+

    >>> df.select('*', sf.hash('*')).show()
    +---+---+------------+
    | c1| c2|hash(c1, c2)|
    +---+---+------------+
    |ABC|DEF|   599895104|
    +---+---+------------+
    """
    from pyspark.sql.functions.builtin import _invoke_function_over_seq_of_columns

    return _invoke_function_over_seq_of_columns("hash", cols)


@_try_remote_hash_functions
def md5(col: "ColumnOrName") -> Column:
    """Calculates the MD5 digest and returns the value as a 32 character hex string.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or column name
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([('ABC',)], ['a'])
    >>> df.select('*', sf.md5('a')).show(truncate=False)
    +---+--------------------------------+
    |a  |md5(a)                          |
    +---+--------------------------------+
    |ABC|902fbdd2b1df0c4f70b4a5d23525e932|
    +---+--------------------------------+
    """
    from pyspark.sql.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("md5", col)


@_try_remote_hash_functions
def sha(col: "ColumnOrName") -> Column:
    """
    Returns a sha1 hash value as a hex string of the `col`.

    .. versionadded:: 3.5.0

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or column name

    See Also
    --------
    :meth:`pyspark.sql.functions.sha1`
    :meth:`pyspark.sql.functions.sha2`

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> spark.range(1).select(sf.sha(sf.lit("Spark"))).show()
    +--------------------+
    |          sha(Spark)|
    +--------------------+
    |85f5955f4b27a9a4c...|
    +--------------------+
    """
    from pyspark.sql.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("sha", col)


@_try_remote_hash_functions
def sha1(col: "ColumnOrName") -> Column:
    """Returns the hex string result of SHA-1.

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or column name
        target column to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    See Also
    --------
    :meth:`pyspark.sql.functions.sha`
    :meth:`pyspark.sql.functions.sha2`

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([('ABC',)], ['a'])
    >>> df.select('*', sf.sha1('a')).show(truncate=False)
    +---+----------------------------------------+
    |a  |sha1(a)                                 |
    +---+----------------------------------------+
    |ABC|3c01bdbb26f358bab27f267924aa2c9a03fcfdb8|
    +---+----------------------------------------+
    """
    from pyspark.sql.functions.builtin import _invoke_function_over_columns

    return _invoke_function_over_columns("sha1", col)


@_try_remote_hash_functions
def sha2(col: "ColumnOrName", numBits: int) -> Column:
    """Returns the hex string result of SHA-2 family of hash functions (SHA-224, SHA-256, SHA-384,
    and SHA-512). The numBits indicates the desired bit length of the result, which must have a
    value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    .. versionadded:: 1.5.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    col : :class:`~pyspark.sql.Column` or column name
        target column to compute on.
    numBits : int
        the desired bit length of the result, which must have a
        value of 224, 256, 384, 512, or 0 (which is equivalent to 256).

    Returns
    -------
    :class:`~pyspark.sql.Column`
        the column for computed results.

    See Also
    --------
    :meth:`pyspark.sql.functions.sha`
    :meth:`pyspark.sql.functions.sha1`

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([['Alice'], ['Bob']], ['name'])
    >>> df.select('*', sf.sha2('name', 256)).show(truncate=False)
    +-----+----------------------------------------------------------------+
    |name |sha2(name, 256)                                                 |
    +-----+----------------------------------------------------------------+
    |Alice|3bc51062973c458d5a6f2d8d64a023246354ad7e064b1e4e009ec8a0699a3043|
    |Bob  |cd9fb1e148ccd8442e5aa74904cc73bf6fb54d1d54d333bd596aa9bb4bb4e961|
    +-----+----------------------------------------------------------------+
    """
    from pyspark.sql.classic.column import _to_java_column
    from pyspark.sql.functions.builtin import _invoke_function

    if numBits not in [0, 224, 256, 384, 512]:
        raise PySparkValueError(
            errorClass="VALUE_NOT_ALLOWED",
            messageParameters={
                "arg_name": "numBits",
                "allowed_values": "[0, 224, 256, 384, 512]",
            },
        )
    return _invoke_function("sha2", _to_java_column(col), numBits)


@_try_remote_hash_functions
def xxhash64(*cols: "ColumnOrName") -> Column:
    """Calculates the hash code of given columns using the 64-bit variant of the xxHash algorithm,
    and returns the result as a long column. The hash computation uses an initial seed of 42.

    .. versionadded:: 3.0.0

    .. versionchanged:: 3.4.0
        Supports Spark Connect.

    Parameters
    ----------
    cols : :class:`~pyspark.sql.Column` or column name
        one or more columns to compute on.

    Returns
    -------
    :class:`~pyspark.sql.Column`
        hash value as long column.

    See Also
    --------
    :meth:`pyspark.sql.functions.hash`

    Examples
    --------
    >>> import pyspark.sql.functions as sf
    >>> df = spark.createDataFrame([('ABC', 'DEF')], ['c1', 'c2'])
    >>> df.select('*', sf.xxhash64('c1')).show()
    +---+---+-------------------+
    | c1| c2|       xxhash64(c1)|
    +---+---+-------------------+
    |ABC|DEF|4105715581806190027|
    +---+---+-------------------+

    >>> df.select('*', sf.xxhash64('c1', df.c2)).show()
    +---+---+-------------------+
    | c1| c2|   xxhash64(c1, c2)|
    +---+---+-------------------+
    |ABC|DEF|3233247871021311208|
    +---+---+-------------------+

    >>> df.select('*', sf.xxhash64('*')).show()
    +---+---+-------------------+
    | c1| c2|   xxhash64(c1, c2)|
    +---+---+-------------------+
    |ABC|DEF|3233247871021311208|
    +---+---+-------------------+
    """
    from pyspark.sql.functions.builtin import _invoke_function_over_seq_of_columns

    return _invoke_function_over_seq_of_columns("xxhash64", cols)


def _test() -> None:
    import sys
    import doctest
    from pyspark.sql import SparkSession
    import pyspark.sql.functions._hash_funcs

    globs = pyspark.sql.functions._hash_funcs.__dict__.copy()
    spark = (
        SparkSession.builder.master("local[4]")
        .appName("sql.functions._hash_funcs tests")
        .getOrCreate()
    )
    globs["spark"] = spark
    (failure_count, test_count) = doctest.testmod(
        pyspark.sql.functions._hash_funcs,
        globs=globs,
        optionflags=doctest.ELLIPSIS | doctest.NORMALIZE_WHITESPACE,
    )
    spark.stop()
    if failure_count:
        sys.exit(-1)


if __name__ == "__main__":
    _test()
