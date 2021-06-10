======
Window
======
.. currentmodule:: pyspark.pandas.window

Rolling objects are returned by ``.rolling`` calls: :func:`pandas_on_spark.DataFrame.rolling`, :func:`pandas_on_spark.Series.rolling`, etc.
Expanding objects are returned by ``.expanding`` calls: :func:`pandas_on_spark.DataFrame.expanding`, :func:`pandas_on_spark.Series.expanding`, etc.

Standard moving window functions
--------------------------------

.. autosummary::
   :toctree: api/

   Rolling.count
   Rolling.sum
   Rolling.min
   Rolling.max
   Rolling.mean

Standard expanding window functions
-----------------------------------

.. autosummary::
   :toctree: api/

   Expanding.count
   Expanding.sum
   Expanding.min
   Expanding.max
   Expanding.mean
