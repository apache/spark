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


====================
From/to other DBMSes
====================
.. currentmodule:: pyspark.pandas


The APIs interacting with other DBMSes in pandas API on Spark are slightly different from the ones in pandas
because pandas API on Spark leverages JDBC APIs in PySpark to read and write from/to other DBMSes.

The APIs to read/write from/to external DBMSes are as follows:

.. autosummary::

    read_sql_table
    read_sql_query
    read_sql

..
    TODO: we should implement and document `DataFrame.to_sql`.

pandas-on-Spark needs a canonical JDBC URL for ``con``, and is able to take extra keyword arguments for `the options in PySpark JDBC APIs <https://spark.apache.org/docs/latest/sql-data-sources-jdbc.html>`_:

.. code-block:: python

    ps.read_sql(..., dbtable="...", driver="", keytab="", ...)


Reading and writing DataFrames
------------------------------

In the example below, you will read and write a table in SQLite.

Firstly, create the ``example`` database as below via Python's SQLite library. This will be read to pandas-on-Spark later:

.. code-block:: python

    import sqlite3

    con = sqlite3.connect('example.db')
    cur = con.cursor()
    # Create table
    cur.execute(
        '''CREATE TABLE stocks
           (date text, trans text, symbol text, qty real, price real)''')
    # Insert a row of data
    cur.execute("INSERT INTO stocks VALUES ('2006-01-05','BUY','RHAT',100,35.14)")
    # Save (commit) the changes
    con.commit()
    con.close()

Pandas API on Spark requires a JDBC driver to read so it requires the driver for your particular database to be on the Spark's classpath. For SQLite JDBC driver, you can download it, for example, as below:

.. code-block:: bash

    curl -O https://repo1.maven.org/maven2/org/xerial/sqlite-jdbc/3.34.0/sqlite-jdbc-3.34.0.jar

After that, you should add it into your Spark session first. Once you add them, pandas API on Spark will automatically detect the Spark session and leverage it.

.. code-block:: python

    import os

    from pyspark.sql import SparkSession

    (SparkSession.builder
        .master("local")
        .appName("SQLite JDBC")
        .config(
            "spark.jars",
            "{}/sqlite-jdbc-3.34.0.jar".format(os.getcwd()))
        .config(
            "spark.driver.extraClassPath",
            "{}/sqlite-jdbc-3.34.0.jar".format(os.getcwd()))
        .getOrCreate())

Now, you're ready to read the table:

.. code-block:: python

    import pyspark.pandas as ps

    df = ps.read_sql("stocks", con="jdbc:sqlite:{}/example.db".format(os.getcwd()))
    df

.. code-block:: text

             date trans symbol    qty  price
    0  2006-01-05   BUY   RHAT  100.0  35.14

You can also write it back to the ``stocks`` table as below:

..
     TODO: switch to use DataFrame.to_sql in the example

.. code-block:: python

    df.price += 1
    df.spark.to_spark_io(
        format="jdbc", mode="append",
        dbtable="stocks", url="jdbc:sqlite:{}/example.db".format(os.getcwd()))
    ps.read_sql("stocks", con="jdbc:sqlite:{}/example.db".format(os.getcwd()))

.. code-block:: text

             date trans symbol    qty  price
    0  2006-01-05   BUY   RHAT  100.0  35.14
    1  2006-01-05   BUY   RHAT  100.0  36.14
