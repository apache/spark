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

    SKIP_SCALADOC=1 SKIP_RDOC=1 SKIP_SQLDOC=1 jekyll serve --watch

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
There are a couple of additional notes to keep in mind when contributing to codes in PySpark:

* Be Pythonic.
* APIs are matched with Scala and Java sides in general.
* PySpark specific APIs can still be considered as long as they are Pythonic and do not conflict with other existent APIs, for example, decorator usage of UDFs.
* If you extend or modify public API, please adjust corresponding type hints. See `Contributing and Maintaining Type Hints`_ for details.

Contributing and Maintaining Type Hints
----------------------------------------

PySpark type hints are provided using stub files, placed in the same directory as the annotated module, with exception to ``# type: ignore`` in modules which don't have their own stubs (tests, examples and non-public API).
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
----------------------------------

Please follow the style of the existing codebase as is, which is virtually PEP 8 with one exception: lines can be up
to 100 characters in length, not 79.
For the docstring style, PySpark follows `NumPy documentation style <https://numpydoc.readthedocs.io/en/latest/format.html>`_.

Note that the method and variable names in PySpark are the similar case is ``threading`` library in Python itself where
the APIs were inspired by Java. PySpark also follows `camelCase` for exposed APIs that match with Scala and Java.
There is an exception ``functions.py`` that uses `snake_case`. It was in order to make APIs SQL (and Python) friendly.

PySpark leverages linters such as `pycodestyle <https://pycodestyle.pycqa.org/en/latest/>`_ and `flake8 <https://flake8.pycqa.org/en/latest/>`_, which ``dev/lint-python`` runs. Therefore, make sure to run that script to double check.
