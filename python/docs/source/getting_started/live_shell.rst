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


==============================
Try PySpark in Your Browser
==============================

.. warning::
   This page is **experimental**. It runs a JVM-free Spark engine entirely in
   your browser through `Pyodide <https://pyodide.org>`_ (CPython on
   WebAssembly) and `sail-wasm <https://github.com/HyukjinKwon/sail-wasm>`_, a
   WebAssembly build of `Sail <https://github.com/lakehq/sail>`_ (a Rust,
   DataFusion-based engine that implements the Spark Connect protocol). It is
   meant for learning and quickstart exploration, not for production or full
   semantic fidelity. Nothing is sent to a server; all computation happens
   locally in this tab.

The cells below are runnable. Press **Run** on the first cell to start the
in-browser engine (a one-time download of a few seconds), then run the others.
A single ``SparkSession`` named ``spark`` is shared across every cell. When a
cell ends with a DataFrame, it is executed in the browser and its result is
shown.

.. note::
   This proof of concept supports **lazy DataFrame transformations** (such as
   ``range``, ``filter``, ``select``, and ``selectExpr``). Operations that
   eagerly contact a server in Spark Connect -- for example ``spark.sql(...)``
   and ``spark.createDataFrame(...)`` -- are not wired up in the browser yet.

A range of numbers
------------------

.. code-block:: python
   :class: pyspark-live

   spark.range(10)

Filter and project
------------------

.. code-block:: python
   :class: pyspark-live

   spark.range(10).filter("id > 3").selectExpr("id", "id * id AS squared")

A derived column
----------------

.. code-block:: python
   :class: pyspark-live

   spark.range(1, 11).selectExpr(
       "id",
       "CASE WHEN id % 2 = 0 THEN 'even' ELSE 'odd' END AS parity",
   )

How it works
------------

These cells use the **stock** PySpark Spark Connect client. The client builds a
Spark Connect plan for the DataFrame entirely on the client side (no server, no
gRPC). The serialized plan is handed to ``sail-wasm``, which decodes, resolves,
and executes it with DataFusion and returns the result as Arrow IPC bytes; the
page then renders those with pandas.

None of this code ships in the PySpark package -- it lives under
``python/docs/source/_static/pyspark-live/`` -- and this page is only built when
the documentation is generated with ``PYSPARK_DOCS_LIVE`` set.
