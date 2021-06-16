====================================
Type Support in Pandas APIs on Spark
====================================

.. currentmodule:: pyspark.pandas

In this chapter, we will briefly show you how data types change when converting pandas-on-Spark DataFrame from/to PySpark DataFrame or pandas DataFrame.


Type casting between PySpark and pandas APIs on Spark
-----------------------------------------------------

When converting a pandas-on-Spark DataFrame from/to PySpark DataFrame, the data types are automatically casted to the appropriate type.

The example below shows how data types are casted from PySpark DataFrame to pandas-on-Spark DataFrame.

.. code-block:: python

    # 1. Create a PySpark DataFrame
    >>> sdf = spark.createDataFrame([
    ...     (1, Decimal(1.0), 1., 1., 1, 1, 1, datetime(2020, 10, 27), "1", True, datetime(2020, 10, 27)),
    ... ], 'tinyint tinyint, decimal decimal, float float, double double, integer integer, long long, short short, timestamp timestamp, string string, boolean boolean, date date')

    # 2. Check the PySpark data types
    >>> sdf
    DataFrame[tinyint: tinyint, decimal: decimal(10,0), float: float, double: double, integer: int, long: bigint, short: smallint, timestamp: timestamp, string: string, boolean: boolean, date: date]

    # 3. Convert PySpark DataFrame to pandas-on-Spark DataFrame
    >>> kdf = sdf.to_koalas()

    # 4. Check the pandas-on-Spark data types
    >>> kdf.dtypes
    tinyint                int8
    decimal              object
    float               float32
    double              float64
    integer               int32
    long                  int64
    short                 int16
    timestamp    datetime64[ns]
    string               object
    boolean                bool
    date                 object
    dtype: object


The example below shows how data types are casted from pandas-on-Spark DataFrame to PySpark DataFrame.

.. code-block:: python

    # 1. Create a pandas-on-Spark DataFrame
    >>> kdf = ks.DataFrame({"int8": [1], "bool": [True], "float32": [1.0], "float64": [1.0], "int32": [1], "int64": [1], "int16": [1], "datetime": [datetime.datetime(2020, 10, 27)], "object_string": ["1"], "object_decimal": [decimal.Decimal("1.1")], "object_date": [datetime.date(2020, 10, 27)]})

    # 2. Type casting by using `astype`
    >>> kdf['int8'] = kdf['int8'].astype('int8')
    >>> kdf['int16'] = kdf['int16'].astype('int16')
    >>> kdf['int32'] = kdf['int32'].astype('int32')
    >>> kdf['float32'] = kdf['float32'].astype('float32')

    # 3. Check the pandas-on-Spark data types
    >>> kdf.dtypes
    int8                        int8
    bool                        bool
    float32                  float32
    float64                  float64
    int32                      int32
    int64                      int64
    int16                      int16
    datetime          datetime64[ns]
    object_string             object
    object_decimal            object
    object_date               object
    dtype: object

    # 4. Convert pandas-on-Spark DataFrame to PySpark DataFrame
    >>> sdf = kdf.to_spark()

    # 5. Check the PySpark data types
    >>> sdf
    DataFrame[int8: tinyint, bool: boolean, float32: float, float64: double, int32: int, int64: bigint, int16: smallint, datetime: timestamp, object_string: string, object_decimal: decimal(2,1), object_date: date]


Type casting between pandas and pandas APIs on Spark
----------------------------------------------------

When converting pandas-on-Spark DataFrame to pandas DataFrame, and the data types are basically same as pandas.

.. code-block:: python

    # Convert pandas-on-Spark DataFrame to pandas DataFrame
    >>> pdf = kdf.to_pandas()

    # Check the pandas data types
    >>> pdf.dtypes
    int8                        int8
    bool                        bool
    float32                  float32
    float64                  float64
    int32                      int32
    int64                      int64
    int16                      int16
    datetime          datetime64[ns]
    object_string             object
    object_decimal            object
    object_date               object
    dtype: object


However, there are several data types only provided by pandas.

.. code-block:: python

    # pd.Catrgorical type is not supported in pandas APIs on Spark yet.
    >>> ks.Series([pd.Categorical([1, 2, 3])])
    Traceback (most recent call last):
    ...
    pyarrow.lib.ArrowInvalid: Could not convert [1, 2, 3]
    Categories (3, int64): [1, 2, 3] with type Categorical: did not recognize Python value type when inferring an Arrow data type


These kind of pandas specific data types below are not currently supported in pandas APIs on Spark but planned to be supported.

* pd.Timedelta
* pd.Categorical
* pd.CategoricalDtype


The pandas specific data types below are not planned to be supported in pandas APIs on Spark yet.

* pd.SparseDtype
* pd.DatetimeTZDtype
* pd.UInt*Dtype
* pd.BooleanDtype
* pd.StringDtype


Internal type mapping
---------------------

The table below shows which NumPy data types are matched to which PySpark data types internally in pandas APIs on Spark.

============= =======================
NumPy         PySpark
============= =======================
np.character  BinaryType
np.bytes\_    BinaryType
np.string\_   BinaryType
np.int8       ByteType
np.byte       ByteType
np.int16      ShortType
np.int32      IntegerType
np.int64      LongType
np.int        LongType
np.float32    FloatType
np.float      DoubleType
np.float64    DoubleType
np.str        StringType
np.unicode\_  StringType
np.bool       BooleanType
np.datetime64 TimestampType
np.ndarray    ArrayType(StringType())
============= =======================


The table below shows which Python data types are matched to which PySpark data types internally in pandas APIs on Spark.

================= ===================
Python            PySpark
================= ===================
bytes             BinaryType
int               LongType
float             DoubleType
str               StringType
bool              BooleanType
datetime.datetime TimestampType
datetime.date     DateType
decimal.Decimal   DecimalType(38, 18)
================= ===================

For decimal type, pandas APIs on Spark use Spark's system default precision and scale.

You can check this mapping by using `as_spark_type` function.

.. code-block:: python

    >>> import typing
    >>> import numpy as np
    >>> from pyspark.pandas.typedef import as_spark_type

    >>> as_spark_type(int)
    LongType

    >>> as_spark_type(np.int32)
    IntegerType

    >>> as_spark_type(typing.List[float])
    ArrayType(DoubleType,true)


You can also check the underlying PySpark data type of `Series` or schema of `DataFrame` by using Spark accessor.

.. code-block:: python

    >>> ks.Series([0.3, 0.1, 0.8]).spark.data_type
    DoubleType

    >>> ks.Series(["welcome", "to", "pandas-on-Spark"]).spark.data_type
    StringType

    >>> ks.Series([[False, True, False]]).spark.data_type
    ArrayType(BooleanType,true)

    >>> ks.DataFrame({"d": [0.3, 0.1, 0.8], "s": ["welcome", "to", "pandas-on-Spark"], "b": [False, True, False]}).spark.print_schema()
    root
     |-- d: double (nullable = false)
     |-- s: string (nullable = false)
     |-- b: boolean (nullable = false)

.. note::

    Pandas APIs on Spark currently do not support multiple types of data in single column.

    .. code-block:: python
    
        >>> ks.Series([1, "A"])
        Traceback (most recent call last):
        ...
        TypeError: an integer is required (got type str)
