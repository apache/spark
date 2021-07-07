==============================
Transform and apply a function
==============================

.. NOTE: the images are stored at https://github.com/koalas/issues/1443. Feel free to edit and/or add.

.. currentmodule:: pyspark.pandas

There are many APIs that allow users to apply a function against pandas-on-Spark DataFrame such as
:func:`DataFrame.transform`, :func:`DataFrame.apply`, :func:`DataFrame.pandas_on_spark.transform_batch`,
:func:`DataFrame.pandas_on_spark.apply_batch`, :func:`Series.pandas_on_spark.transform_batch`, etc. Each has a distinct
purpose and works differently internally. This section describes the differences among
them where users are confused often.

``transform`` and ``apply``
---------------------------

The main difference between :func:`DataFrame.transform` and :func:`DataFrame.apply` is that the former requires
to return the same length of the input and the latter does not require this. See the example below:

.. code-block:: python

   >>> psdf = ps.DataFrame({'a': [1,2,3], 'b':[4,5,6]})
   >>> def pandas_plus(pser):
   ...     return pser + 1  # should always return the same length as input.
   ...
   >>> psdf.transform(pandas_plus)

.. code-block:: python

   >>> psdf = ps.DataFrame({'a': [1,2,3], 'b':[5,6,7]})
   >>> def pandas_plus(pser):
   ...     return pser[pser % 2 == 1]  # allows an arbitrary length
   ...
   >>> psdf.apply(pandas_plus)

In this case, each function takes a pandas Series, and pandas API on Spark computes the functions in a distributed manner as below.

.. image:: https://user-images.githubusercontent.com/6477701/80076790-a1cf0680-8587-11ea-8b08-8dc694071ba0.png
  :alt: transform and apply
  :align: center
  :width: 550

In case of 'column' axis, the function takes each row as a pandas Series.

.. code-block:: python

   >>> psdf = ps.DataFrame({'a': [1,2,3], 'b':[4,5,6]})
   >>> def pandas_plus(pser):
   ...     return sum(pser)  # allows an arbitrary length
   ...
   >>> psdf.apply(pandas_plus, axis='columns')

The example above calculates the summation of each row as a pandas Series. See below:

.. image:: https://user-images.githubusercontent.com/6477701/80076898-c2975c00-8587-11ea-9b2c-69c9729e9294.png
  :alt: apply axis
  :align: center
  :width: 600

In the examples above, the type hints were not used for simplicity but it is encouraged to use to avoid performance penalty.
Please refer the API documentations.


``pandas_on_spark.transform_batch`` and ``pandas_on_spark.apply_batch``
-----------------------------------------------------------------------

In :func:`DataFrame.pandas_on_spark.transform_batch`, :func:`DataFrame.pandas_on_spark.apply_batch`, :func:`Series.pandas_on_spark.transform_batch`, etc., the ``batch``
postfix means each chunk in pandas-on-Spark DataFrame or Series. The APIs slice the pandas-on-Spark DataFrame or Series, and
then applies the given function with pandas DataFrame or Series as input and output. See the examples below:

.. code-block:: python

   >>> psdf = ps.DataFrame({'a': [1,2,3], 'b':[4,5,6]})
   >>> def pandas_plus(pdf):
   ...     return pdf + 1  # should always return the same length as input.
   ...
   >>> psdf.pandas_on_spark.transform_batch(pandas_plus)

.. code-block:: python

   >>> psdf = ps.DataFrame({'a': [1,2,3], 'b':[4,5,6]})
   >>> def pandas_plus(pdf):
   ...     return pdf[pdf.a > 1]  # allow arbitrary length
   ...
   >>> psdf.pandas_on_spark.apply_batch(pandas_plus)

The functions in both examples take a pandas DataFrame as a chunk of pandas-on-Spark DataFrame, and output a pandas DataFrame.
Pandas API on Spark combines the pandas DataFrames as a pandas-on-Spark DataFrame.

Note that :func:`DataFrame.pandas_on_spark.transform_batch` has the length restriction - the length of input and output should be
the same whereas :func:`DataFrame.pandas_on_spark.apply_batch` does not.  However, it is important to know that
the output belongs to the same DataFrame when :func:`DataFrame.pandas_on_spark.transform_batch` returns a Series, and
you can avoid a shuffle by the operations between different DataFrames. In case of :func:`DataFrame.pandas_on_spark.apply_batch`, its output is always
treated that it belongs to a new different DataFrame. See also
`Operations on different DataFrames <options.rst#operations-on-different-dataframes>`_ for more details.

.. image:: https://user-images.githubusercontent.com/6477701/80076779-9f6cac80-8587-11ea-8c92-07d7b992733b.png
  :alt: pandas_on_spark.transform_batch and pandas_on_spark.apply_batch in Frame
  :align: center
  :width: 650

In case of :func:`Series.pandas_on_spark.transform_batch`, it is also similar with :func:`DataFrame.pandas_on_spark.transform_batch`; however, it takes
a pandas Series as a chunk of pandas-on-Spark Series.

.. code-block:: python

   >>> psdf = ps.DataFrame({'a': [1,2,3], 'b':[4,5,6]})
   >>> def pandas_plus(pser):
   ...     return pser + 1  # should always return the same length as input.
   ...
   >>> psdf.a.pandas_on_spark.transform_batch(pandas_plus)

Under the hood, each batch of pandas-on-Spark Series is split to multiple pandas Series, and each function computes on that as below:

.. image:: https://user-images.githubusercontent.com/6477701/80076795-a3003380-8587-11ea-8b73-186e4047f8c0.png
  :alt: pandas_on_spark.transform_batch in Series
  :width: 350
  :align: center

There are more details such as the type inference and preventing its performance penalty. Please refer the API documentations.
