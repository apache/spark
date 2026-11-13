---
layout: global
title: Spark Declarative Pipelines Quick Start
displayTitle: Spark Declarative Pipelines Quick Start
license: |
  Licensed to the Apache Software Foundation (ASF) under one or more
  contributor license agreements.  See the NOTICE file distributed with
  this work for additional information regarding copyright ownership.
  The ASF licenses this file to You under the Apache License, Version 2.0
  (the "License"); you may not use this file except in compliance with
  the License.  You may obtain a copy of the License at
 
     http://www.apache.org/licenses/LICENSE-2.0
 
  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
---

* Table of contents
{:toc}

This page takes you from an empty machine to a running [Spark Declarative Pipelines (SDP)](declarative-pipelines-programming-guide.html)
pipeline on your laptop. You install PySpark, generate a pipeline project, run it, query its
output tables, and then add a streaming table and a materialized view that depends on it.

## Prerequisites

- Java 21 or 25. `java` must be on your `PATH`, or `JAVA_HOME` must point to the Java installation.
- Python 3.11 or later.

## Install

Create a virtual environment and install PySpark with the `pipelines` extra. The extra installs the
Spark Connect and YAML dependencies that the `spark-pipelines` command needs.

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install "pyspark[pipelines]"
```

The quotes around `pyspark[pipelines]` are required in shells such as zsh, which otherwise treat
the brackets as a glob pattern.

## Create a pipeline project

```bash
spark-pipelines init --name hello_pipeline
cd hello_pipeline
```

This creates the following project:

```
hello_pipeline/
|-- spark-pipeline.yml
|-- pipeline-storage/
`-- transformations/
    |-- example_python_materialized_view.py
    `-- example_sql_materialized_view.sql
```

`spark-pipeline.yml` is the pipeline spec. It names the pipeline, points `libraries` at the source
files under `transformations/`, sets `storage` to the directory that holds streaming checkpoints,
and sets `spark.sql.catalogImplementation: hive` so that table metadata is kept in a local Hive
metastore and survives across runs. See [Pipeline Projects](declarative-pipelines-programming-guide.html#pipeline-projects)
for all fields.

The two example files define one materialized view each. The Python file:

```python
from pyspark import pipelines as dp
from pyspark.sql import DataFrame

@dp.materialized_view
def example_python_materialized_view() -> DataFrame:
    return spark.range(10)
```

The SQL file reads from the Python one:

```sql
CREATE MATERIALIZED VIEW example_sql_materialized_view AS
SELECT id FROM example_python_materialized_view
WHERE id % 2 = 0
```

## Run the pipeline

```bash
spark-pipelines run
```

SDP analyzes the dependencies between the datasets, runs
`example_python_materialized_view` first, and then `example_sql_materialized_view`. The output
ends with:

```
... Flow spark_catalog.default.example_python_materialized_view has COMPLETED.
...
... Flow spark_catalog.default.example_sql_materialized_view has COMPLETED.
... Run is COMPLETED.
```

The run writes table data to `spark-warehouse/` and table metadata to `metastore_db/`, both in the
current directory.

## Query the output tables

From the project directory, query the tables with the `spark-sql` command installed with PySpark:

```bash
spark-sql -e "SELECT * FROM example_sql_materialized_view ORDER BY id"
```

```
0
2
4
6
8
```

You can also start the PySpark shell with `pyspark` from the same directory and run
`spark.table("example_sql_materialized_view").show()`.

Run `spark-pipelines`, `spark-sql` and `pyspark` from the project directory: the local Hive
metastore lives in `metastore_db/` under the current directory.

## Add a streaming table and a materialized view

A streaming table processes only the data that arrived since the previous run. Create a directory
of JSON order events with a first batch of two orders:

```bash
mkdir -p data/orders
cat > data/orders/batch-1.json <<'EOF'
{"order_id": 1, "customer": "alice", "amount": 20.0}
{"order_id": 2, "customer": "bob", "amount": 35.5}
EOF
```

Define a streaming table that ingests the files, in `transformations/orders.py`:

```bash
cat > transformations/orders.py <<'EOF'
from pathlib import Path

from pyspark import pipelines as dp
from pyspark.sql import DataFrame

ORDERS_DIR = (Path(__file__).parent.parent / "data" / "orders").resolve().as_uri()


@dp.table
def orders() -> DataFrame:
    return (
        spark.readStream
        .schema("order_id INT, customer STRING, amount DOUBLE")
        .json(ORDERS_DIR)
    )
EOF
```

`@dp.table` defines a streaming table because the function returns a streaming DataFrame
(`spark.readStream`). Streaming file sources need an explicit schema.

Define a materialized view that aggregates the streaming table, in
`transformations/revenue_by_customer.sql`:

```bash
cat > transformations/revenue_by_customer.sql <<'EOF'
CREATE MATERIALIZED VIEW revenue_by_customer AS
SELECT customer, count(*) AS num_orders, sum(amount) AS revenue
FROM orders
GROUP BY customer
EOF
```

Run the pipeline and query the result:

```bash
spark-pipelines run
spark-sql -e "SELECT * FROM revenue_by_customer ORDER BY customer"
```

```
alice	1	20.0
bob	1	35.5
```

Add a second batch and run again:

```bash
cat > data/orders/batch-2.json <<'EOF'
{"order_id": 3, "customer": "alice", "amount": 12.5}
EOF
spark-pipelines run
spark-sql -e "SELECT * FROM revenue_by_customer ORDER BY customer"
```

```
alice	2	32.5
bob	1	35.5
```

The second run read only `batch-2.json` into `orders`, and recomputed `revenue_by_customer`.

## Check a pipeline without running it

`spark-pipelines dry-run` resolves the whole pipeline and reports errors, such as a reference to a
table or column that does not exist, without reading or writing data:

```bash
spark-pipelines dry-run
```

## Start over

To recompute every dataset from scratch, discarding the existing data and streaming checkpoints:

```bash
spark-pipelines run --full-refresh-all
```

To remove all pipeline state, delete `spark-warehouse/`, `metastore_db/` and `pipeline-storage/`.

## Next steps

- The [hello world example]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python/pipelines/hello_world)
  in the Spark source tree is a complete project with Python and SQL datasets.
- The [Spark Declarative Pipelines Programming Guide](declarative-pipelines-programming-guide.html)
  covers flows, temporary views, sinks, change data capture and the `spark-pipelines` options.
