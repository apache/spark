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

===========
Spark Connect - Execution Info and Debug
===========


Execution Info
--------------

The ``executionInfo`` property of the DataFrame allows users to access execution
metrics about a previously executed operation. In Spark Connect mode, the
plan metrics of the execution are always submitted as the last elements of the
response allowing users an easy way to present this information.

.. code-block:: python
    df = spark.range(100)
    df.collect()
    ei = df.executionInfo

    # Access the execution metrics:
    metrics = ei.metrics
    print(metrics.toText())

Debugging DataFrame Data Flows
-------------------------------
Sometimes it is useful to understand the data flow of a DataFrame operation. Whereas
metrics allow to track row counts between different operators, the execution plan
does not always resemble the semantic execution.

The ``debug`` method allows users to inject predefiend observation points into the
query execution. After execution the user can access the observations and access
the associated metrics.

By default, calling ``debug()`` will inject a single observation that counts the number
of rows flowing out of the DataFrame.


.. code-block:: python
    df = spark.range(100).debug()
    filtered = df.where(df.id < 10).debug()
    filtered.collect()
    ei = df.executionInfo
    for op in ei.observations:
        print(op.debugString())
