..  Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

..    http://www.apache.org/licenses/LICENSE-2.0

..  Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.

=======================
Contributing to PySpark
=======================

There are many types of contribution, for example, helping other users, testing releases, reviewing changes,
documentation contribution, bug reporting, JIRA maintenance, code changes, etc.
These are documented at `the general guidelines <https://spark.apache.org/contributing.html>`_.
This page focuses on PySpark and includes additional details specifically for PySpark.


Contributing by Testing Releases
--------------------------------

Before the official release, PySpark release candidates are shared in the `dev@spark.apache.org <https://mail-archives.apache.org/mod_mbox/spark-dev/>`_ mailing list to vote on.
This release candidates can be easily installed via pip. For example, in case of Spark 3.0.0 RC1, you can install as below:

.. code-block:: bash

    pip install https://dist.apache.org/repos/dist/dev/spark/v3.0.0-rc1-bin/pyspark-3.0.0.tar.gz

The link for release files such as ``https://dist.apache.org/repos/dist/dev/spark/v3.0.0-rc1-bin`` can be found in the vote thread.

Testing and verifying users' existing workloads against release candidates is one of the vital contributions to PySpark.
It prevents breaking users' existing workloads before the official release.
When there is an issue such as a regression, correctness problem or performance degradation worth enough to drop the release candidate,
usually the release candidate is dropped and the community focuses on fixing it to include in the next release candidate.


Contributing Documentation Changes
----------------------------------

The release documentation is located under Spark's `docs <https://github.com/apache/spark/tree/master/docs>`_ directory.
`README.md <https://github.com/apache/spark/blob/master/docs/README.md>`_ describes the required dependencies and steps
to generate the documentations. Usually, PySpark documentation is tested with the command below
under the `docs <https://github.com/apache/spark/tree/master/docs>`_ directory:

.. code-block:: bash

    SKIP_SCALADOC=1 SKIP_RDOC=1 SKIP_SQLDOC=1 bundle exec jekyll serve --watch

PySpark uses Sphinx to generate its release PySpark documentation. Therefore, if you want to build only PySpark documentation alone,
you can build under `python/docs <https://github.com/apache/spark/tree/master/python>`_ directory by:

.. code-block:: bash

    make html

It generates the corresponding HTMLs under ``python/docs/build/html``.

Lastly, please make sure that the new APIs are documented by manually adding methods and/or classes at the corresponding RST files
under ``python/docs/source/reference``. Otherwise, they would not be documented in PySpark documentation.


Preparing to Contribute Code Changes
------------------------------------

Before starting to work on codes in PySpark, it is recommended to read `the general guidelines <https://spark.apache.org/contributing.html>`_.
Additionally, there are a couple of additional notes to keep in mind when contributing to codes in PySpark:

* Be Pythonic
    See `The Zen of Python <https://www.python.org/dev/peps/pep-0020/>`_.

* Match APIs with Scala and Java sides
    Apache Spark is an unified engine that provides a consistent API layer. In general, the APIs are consistently supported across other languages.

* PySpark-specific APIs can be accepted
    As long as they are Pythonic and do not conflict with other existent APIs, it is fine to raise a API request, for example, decorator usage of UDFs.

* Adjust the corresponding type hints if you extend or modify public API
    See `Contributing and Maintaining Type Hints`_ for details.

If you are fixing pandas API on Spark (``pyspark.pandas``) package, please consider the design principles below:

* Return pandas-on-Spark data structure for big data, and pandas data structure for small data
    Often developers face the question whether a particular function should return a pandas-on-Spark DataFrame/Series, or a pandas DataFrame/Series. The principle is: if the returned object can be large, use a pandas-on-Spark DataFrame/Series. If the data is bound to be small, use a pandas DataFrame/Series. For example, ``DataFrame.dtypes`` return a pandas Series, because the number of columns in a DataFrame is bounded and small, whereas ``DataFrame.head()`` or ``Series.unique()`` returns a pandas-on-Spark DataFrame/Series, because the resulting object can be large.

* Provide discoverable APIs for common data science tasks
    At the risk of overgeneralization, there are two API design approaches: the first focuses on providing APIs for common tasks; the second starts with abstractions, and enables users to accomplish their tasks by composing primitives. While the world is not black and white, pandas takes more of the former approach, while Spark has taken more of the latter.

    One example is value count (count by some key column), one of the most common operations in data science. pandas ``DataFrame.value_count`` returns the result in sorted order, which in 90% of the cases is what users prefer when exploring data, whereas Spark's does not sort, which is more desirable when building data pipelines, as users can accomplish the pandas behavior by adding an explicit ``orderBy``.

    Similar to pandas, pandas API on Spark should also lean more towards the former, providing discoverable APIs for common data science tasks. In most cases, this principle is well taken care of by simply implementing pandas' APIs. However, there will be circumstances in which pandas' APIs don't address a specific need, e.g. plotting for big data.

* Guardrails to prevent users from shooting themselves in the foot
    Certain operations in pandas are prohibitively expensive as data scales, and we don't want to give users the illusion that they can rely on such operations in pandas API on Spark. That is to say, methods implemented in pandas API on Spark should be safe to perform by default on large datasets. As a result, the following capabilities are not implemented in pandas API on Spark:

    * Capabilities that are fundamentally not parallelizable: e.g. imperatively looping over each element
    * Capabilities that require materializing the entire working set in a single node's memory. This is why we do not implement `pandas.DataFrame.to_xarray <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.to_xarray.html>`_. Another example is the ``_repr_html_`` call caps the total number of records shown to a maximum of 1000, to prevent users from blowing up their driver node simply by typing the name of the DataFrame in a notebook.

    A few exceptions, however, exist. One common pattern with "big data science" is that while the initial dataset is large, the working set becomes smaller as the analysis goes deeper. For example, data scientists often perform aggregation on datasets and want to then convert the aggregated dataset to some local data structure. To help data scientists, we offer the following:

    * ``DataFrame.to_pandas``: returns a pandas DataFrame (pandas-on-Spark only)
    * ``DataFrame.to_numpy``: returns a numpy array, works with both pandas and pandas API on Spark

    Note that it is clear from the names that these functions return some local data structure that would require materializing data in a single node's memory. For these functions, we also explicitly document them with a warning note that the resulting data structure must be small.


Environment Setup
-----------------

Prerequisite
~~~~~~~~~~~~

PySpark development requires to build Spark that needs a proper JDK installed, etc. See `Building Spark <https://spark.apache.org/docs/latest/building-spark.html>`_ for more details.

Conda
~~~~~

If you are using Conda, the development environment can be set as follows.

.. code-block:: bash

    # Python 3.6+ is required
    conda create --name pyspark-dev-env python=3.9
    conda activate pyspark-dev-env
    pip install -r dev/requirements.txt

Once it is set up, make sure you switch to `pyspark-dev-env` before starting the development:

.. code-block:: bash

    conda activate pyspark-dev-env

Now, you can start developing and `running the tests <testing.rst>`_.

pip
~~~

With Python 3.6+, pip can be used as below to install and set up the development environment.

.. code-block:: bash

    pip install -r dev/requirements.txt

Now, you can start developing and `running the tests <testing.rst>`_.


Contributing and Maintaining Type Hints
----------------------------------------

PySpark type hints are provided using stub files, placed in the same directory as the annotated module, with exception to:

* ``# type: ignore`` in modules which don't have their own stubs (tests, examples and non-public API). 
* pandas API on Spark (``pyspark.pandas`` package) where the type hints are inlined.

As a rule of thumb, only public API is annotated.

Annotations should, when possible:

* Reflect expectations of the underlying JVM API, to help avoid type related failures outside Python interpreter.
* In case of conflict between too broad (``Any``) and too narrow argument annotations, prefer the latter as one, as long as it is covering most of the typical use cases.
* Indicate nonsensical combinations of arguments using ``@overload``  annotations. For example, to indicate that ``*Col`` and ``*Cols`` arguments are mutually exclusive:

  .. code-block:: python

    @overload
    def __init__(
        self,
        *,
        threshold: float = ...,
        inputCol: Optional[str] = ...,
        outputCol: Optional[str] = ...
    ) -> None: ...
    @overload
    def __init__(
        self,
        *,
        thresholds: Optional[List[float]] = ...,
        inputCols: Optional[List[str]] = ...,
        outputCols: Optional[List[str]] = ...
    ) -> None: ...

* Be compatible with the current stable MyPy release.


Complex supporting type definitions, should be placed in dedicated ``_typing.pyi`` stubs. See for example `pyspark.sql._typing.pyi <https://github.com/apache/spark/blob/master/python/pyspark/sql/_typing.pyi>`_.

Annotations can be validated using ``dev/lint-python`` script or by invoking mypy directly:

.. code-block:: bash

    mypy --config python/mypy.ini python/pyspark


Code and Docstring Guide
------------------------

Code Conventions
~~~~~~~~~~~~~~~~

Please follow the style of the existing codebase as is, which is virtually PEP 8 with one exception: lines can be up
to 100 characters in length, not 79.

Note that:

* the method and variable names in PySpark are the similar case is ``threading`` library in Python itself where the APIs were inspired by Java. PySpark also follows `camelCase` for exposed APIs that match with Scala and Java. 

* In contrast, ``functions.py`` uses `snake_case` in order to make APIs SQL (and Python) friendly.

* In addition, pandas-on-Spark (``pyspark.pandas``) also uses `snake_case` because this package is free from API consistency with other languages.

PySpark leverages linters such as `pycodestyle <https://pycodestyle.pycqa.org/en/latest/>`_ and `flake8 <https://flake8.pycqa.org/en/latest/>`_, which ``dev/lint-python`` runs. Therefore, make sure to run that script to double check.


Docstring Conventions
~~~~~~~~~~~~~~~~~~~~~

PySpark follows `NumPy documentation style <https://numpydoc.readthedocs.io/en/latest/format.html>`_.


Doctest Conventions
~~~~~~~~~~~~~~~~~~~

In general, doctests should be grouped logically by separating a newline.

For instance, the first block is for the statements for preparation, the second block is for using the function with a specific argument,
and third block is for another argument. As a example, please refer `DataFrame.rsub <https://pandas.pydata.org/pandas-docs/stable/reference/api/pandas.DataFrame.rsub.html#pandas.DataFrame.rsub>`_ in pandas.

These blocks should be consistently separated in PySpark doctests, and more doctests should be added if the coverage of the doctests or the number of examples to show is not enough.
