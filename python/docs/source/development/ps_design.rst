=================
Design Principles
=================

.. currentmodule:: pyspark.pandas

This section outlines design principles guiding the pandas APIs on Spark.

Be Pythonic
-----------

Pandas APIs on Spark target Python data scientists. We want to stick to the convention that users are already familiar with as much as possible. Here are some examples:

- Function names and parameters use snake_case, rather than CamelCase. This is different from PySpark's design. For example, pandas APIs on Spark have `to_pandas()`, whereas PySpark has `toPandas()` for converting a DataFrame into a pandas DataFrame. In limited cases, to maintain compatibility with Spark, we also provide Spark's variant as an alias.

- Pandas APIs on Spark respect to the largest extent the conventions of the Python numerical ecosystem, and allows the use of NumPy types, etc. that can be supported by Spark.

- pandas-on-Spark docs' style and infrastructure simply follow rest of the PyData projects'.

Unify small data (pandas) API and big data (Spark) API, but pandas first
------------------------------------------------------------------------

The pandas-on-Spark DataFrame is meant to provide the best of pandas and Spark under a single API, with easy and clear conversions between each API when necessary. When Spark and pandas have similar APIs with subtle differences, the principle is to honor the contract of the pandas API first.

There are different classes of functions:

 1. Functions that are found in both Spark and pandas under the same name (`count`, `dtypes`, `head`). The return value is the same as the return type in pandas (and not Spark's).
    
 2. Functions that are found in Spark but that have a clear equivalent in pandas, e.g. `alias` and `rename`. These functions will be implemented as the alias of the pandas function, but should be marked that they are aliases of the same functions. They are provided so that existing users of PySpark can get the benefits of pandas APIs on Spark without having to adapt their code.
 
 3. Functions that are only found in pandas. When these functions are appropriate for distributed datasets, they should become available in pandas APIs on Spark.
 
 4. Functions that are only found in Spark that are essential to controlling the distributed nature of the computations, e.g. `cache`. These functions should be available in pandas APIs on Spark.

We are still debating whether data transformation functions only available in Spark should be added to pandas APIs on Spark, e.g. `select`. We would love to hear your feedback on that.

Return pandas-on-Spark data structure for big data, and pandas data structure for small data
--------------------------------------------------------------------------------------------

Often developers face the question whether a particular function should return a pandas-on-Spark DataFrame/Series, or a pandas DataFrame/Series. The principle is: if the returned object can be large, use a pandas-on-Spark DataFrame/Series. If the data is bound to be small, use a pandas DataFrame/Series. For example, `DataFrame.dtypes` return a pandas Series, because the number of columns in a DataFrame is bounded and small, whereas `DataFrame.head()` or `Series.unique()` returns a pandas-on-Spark DataFrame/Series, because the resulting object can be large.

Provide discoverable APIs for common data science tasks
-------------------------------------------------------

At the risk of overgeneralization, there are two API design approaches: the first focuses on providing APIs for common tasks; the second starts with abstractions, and enable users to accomplish their tasks by composing primitives. While the world is not black and white, pandas takes more of the former approach, while Spark has taken more of the later.

One example is value count (count by some key column), one of the most common operations in data science. pandas `DataFrame.value_count` returns the result in sorted order, which in 90% of the cases is what users prefer when exploring data, whereas Spark's does not sort, which is more desirable when building data pipelines, as users can accomplish the pandas behavior by adding an explicit `orderBy`.

Similar to pandas, pandas APIs on Spark should also lean more towards the former, providing discoverable APIs for common data science tasks. In most cases, this principle is well taken care of by simply implementing pandas' APIs. However, there will be circumstances in which pandas' APIs don't address a specific need, e.g. plotting for big data.

Provide well documented APIs, with examples
-------------------------------------------

All functions and parameters should be documented. Most functions should be documented with examples, because those are the easiest to understand than a blob of text explaining what the function does.

A recommended way to add documentation is to start with the docstring of the corresponding function in PySpark or pandas, and adapt it for pandas APIs on Spark. If you are adding a new function, also add it to the API reference doc index page in `docs/source/reference` directory. The examples in docstring also improve our test coverage.

Guardrails to prevent users from shooting themselves in the foot
----------------------------------------------------------------

Certain operations in pandas are prohibitively expensive as data scales, and we don't want to give users the illusion that they can rely on such operations in pandas APIs on Spark. That is to say, methods implemented in pandas APIs on Spark should be safe to perform by default on large datasets. As a result, the following capabilities are not implemented in pandas APIs on Spark:

1. Capabilities that are fundamentally not parallelizable: e.g. imperatively looping over each element
2. Capabilities that require materializing the entire working set in a single node's memory. This is why we do not implement `pandas.DataFrame.to_xarray <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_xarray.html>`_. Another example is the `_repr_html_` call caps the total number of records shown to a maximum of 1000, to prevent users from blowing up their driver node simply by typing the name of the DataFrame in a notebook.

A few exceptions, however, exist. One common pattern with "big data science" is that while the initial dataset is large, the working set becomes smaller as the analysis goes deeper. For example, data scientists often perform aggregation on datasets and want to then convert the aggregated dataset to some local data structure. To help data scientists, we offer the following:

- :func:`DataFrame.to_pandas`: returns a pandas DataFrame, koalas only
- :func:`DataFrame.to_numpy`: returns a numpy array, works with both pandas and pandas APIs on Spark

Note that it is clear from the names that these functions return some local data structure that would require materializing data in a single node's memory. For these functions, we also explicitly document them with a warning note that the resulting data structure must be small.

Be a lean API layer and move fast
---------------------------------

Pandas APIs on Spark are designed as an API overlay layer on top of Spark. The project should be lightweight, and most functions should be implemented as wrappers
around Spark or pandas - the pandas-on-Spark library is designed to be used only in the Spark's driver side in general.
Pandas APIs on Spark do not accept heavyweight implementations, e.g. execution engine changes.

This approach enables us to move fast. For the considerable future, we aim to be making monthly releases. If we find a critical bug, we will be making a new release as soon as the bug fix is available.

High test coverage
------------------

Pandas APIs on Spark should be well tested. The project tracks its test coverage with over 90% across the entire codebase, and close to 100% for critical parts. Pull requests will not be accepted unless they have close to 100% statement coverage from the codecov report.
