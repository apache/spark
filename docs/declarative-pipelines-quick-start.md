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
pipeline on your laptop. You install PySpark, start a Spark Connect server, generate a pipeline
project, run it, query its output tables, and then add a streaming table and a materialized view
that depends on it.

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

## Start a Spark Connect server

Pipelines run on a Spark Connect server: `spark-pipelines` sends the pipeline definition to the
server, and the server runs the flows and holds the output tables and their metadata. Start one
before you create the project.

The `pyspark` package ships the server scripts in its `sbin/` directory, which `pip` does not add
to the `PATH`, so locate the package first:

```bash
SPARK_HOME=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
"$SPARK_HOME"/sbin/start-connect-server.sh --conf spark.sql.catalogImplementation=hive
```

The script prints the path of the server log and returns. The server keeps running in the
background and listens on `localhost:15002`.

`spark.sql.catalogImplementation=hive` keeps table metadata in a local Hive metastore, so tables
survive a restart of the server. Without it, the server uses its default in-memory catalog, which
keeps table metadata only while the server runs: after a restart the tables are gone, and the next
run fails with `LOCATION_ALREADY_EXISTS` because their data is still in `spark-warehouse/`.

The server writes two directories into the directory you started it from: `spark-warehouse/` holds
the table data and `metastore_db/` holds the metastore (together with a `derby.log` file). It runs
until you stop it with `sbin/stop-connect-server.sh` from the same directory, as shown in
[Stop the server](#stop-the-server) at the end of this page. See
[Starting a Spark Connect server](spark-connect-setup.html#starting-a-spark-connect-server) for the
server's options.

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

`spark-pipeline.yml` is the pipeline spec:

```yaml
name: hello_pipeline
storage: file:///path/to/hello_pipeline/pipeline-storage
libraries:
  - glob:
      include: transformations/**
```

`name` names the pipeline, `storage` is the absolute URI of the directory that holds streaming
checkpoints, and `libraries` points at the source files under `transformations/`. See
[Pipeline Projects](declarative-pipelines-programming-guide.html#pipeline-projects) for all fields.

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

Run the pipeline from the project directory, where `spark-pipelines` finds `spark-pipeline.yml`.
`--remote` points it at the server:

```bash
spark-pipelines run --remote sc://localhost:15002
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

The run writes each table as Parquet files under the server's `spark-warehouse/`, and the table
metadata to the server's catalog.

## Query the output tables

Query the tables through the same server. Start the PySpark shell with `--remote`:

```bash
pyspark --remote sc://localhost:15002
```

```python
>>> spark.sql("SELECT * FROM example_sql_materialized_view ORDER BY id").show()
+---+
| id|
+---+
|  0|
|  2|
|  4|
|  6|
|  8|
+---+
```

Keep this shell open in a second terminal; the next sections run their queries in it. `spark-sql`
and a plain `pyspark` without `--remote` start their own Spark and do not see the server's tables.

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

ORDERS_DIR = f"file://{(Path(__file__).parent.parent / 'data' / 'orders').resolve()}"


@dp.table
def orders() -> DataFrame:
    return (
        spark.readStream
        .schema("order_id INT, customer STRING, amount DOUBLE")
        .json(ORDERS_DIR)
    )
EOF
```

`@dp.table` defines a streaming table, so its function must return a streaming DataFrame
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

Run the pipeline and query the result in the PySpark shell:

```bash
spark-pipelines run --remote sc://localhost:15002
```

```python
>>> spark.sql("SELECT * FROM revenue_by_customer ORDER BY customer").show()
+--------+----------+-------+
|customer|num_orders|revenue|
+--------+----------+-------+
|   alice|         1|   20.0|
|     bob|         1|   35.5|
+--------+----------+-------+
```

Add a second batch and run again:

```bash
cat > data/orders/batch-2.json <<'EOF'
{"order_id": 3, "customer": "alice", "amount": 12.5}
EOF
spark-pipelines run --remote sc://localhost:15002
```

```python
>>> spark.sql("SELECT * FROM revenue_by_customer ORDER BY customer").show()
+--------+----------+-------+
|customer|num_orders|revenue|
+--------+----------+-------+
|   alice|         2|   32.5|
|     bob|         1|   35.5|
+--------+----------+-------+
```

The second run read only `batch-2.json` into `orders`, and recomputed `revenue_by_customer`.

## Check a pipeline without running it

`spark-pipelines dry-run` resolves the whole pipeline and reports errors, such as a reference to a
table or column that does not exist, without reading or writing data:

```bash
spark-pipelines dry-run --remote sc://localhost:15002
```

## Start over

To recompute every dataset from scratch, discarding the existing data and streaming checkpoints:

```bash
spark-pipelines run --full-refresh-all --remote sc://localhost:15002
```

To remove all pipeline state, stop the server, delete its `spark-warehouse/` and `metastore_db/`
and the project's `pipeline-storage/`, and start the server again. With the directories this page
uses:

```bash
"$SPARK_HOME"/sbin/stop-connect-server.sh
rm -r ../spark-warehouse ../metastore_db pipeline-storage
"$SPARK_HOME"/sbin/start-connect-server.sh --conf spark.sql.catalogImplementation=hive
```

Restart the PySpark shell as well: a shell connected to the old server cannot reuse its session.

## Stop the server

```bash
"$SPARK_HOME"/sbin/stop-connect-server.sh
```

The tables stay in `spark-warehouse/` and `metastore_db/`. Start the server again with the command
from [Start a Spark Connect server](#start-a-spark-connect-server) to query them again.

## Next steps

- The [hello world example]({{site.SPARK_GITHUB_URL}}/tree/master/examples/src/main/python/pipelines/hello_world)
  in the Spark source tree is a complete project with Python and SQL datasets.
- The [Spark Declarative Pipelines Programming Guide](declarative-pipelines-programming-guide.html)
  covers flows, temporary views, sinks, change data capture and the `spark-pipelines` options.
