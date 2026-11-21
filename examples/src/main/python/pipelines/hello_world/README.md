# Spark Declarative Pipelines: hello world

A minimal [Spark Declarative Pipelines](https://spark.apache.org/docs/latest/declarative-pipelines-programming-guide.html)
project that ingests order events and reports revenue per country.

| Dataset              | Type              | Language | Defined in                             |
|----------------------|-------------------|----------|----------------------------------------|
| `customers`          | Materialized view | Python   | `transformations/orders.py`            |
| `orders`             | Streaming table   | Python   | `transformations/orders.py`            |
| `revenue_by_country` | Materialized view | SQL      | `transformations/revenue_by_country.sql` |

`orders` incrementally ingests the JSON files in `data/orders/`. `revenue_by_country` joins `orders`
with `customers`, so SDP runs it after both.

```
hello_world/
|-- spark-pipeline.yml            # pipeline spec
|-- transformations/
|   |-- orders.py                 # customers (materialized view), orders (streaming table)
|   `-- revenue_by_country.sql    # revenue_by_country (materialized view)
|-- data/orders/orders-1.json     # first batch of orders
`-- more-data/orders-2.json       # second batch, added in step 5
```

## Requirements

- Java 21 or 25, with `java` on the `PATH` or `JAVA_HOME` set.
- Python 3.11 or later.
- PySpark with the `pipelines` extra:

  ```bash
  pip install "pyspark[pipelines]"
  ```

  From a Spark distribution, use `bin/spark-pipelines`, `bin/pyspark` and
  `sbin/start-connect-server.sh` in place of the commands below.

## Run the example

1. Copy the project out of the Spark source tree, so the run's output stays out of it, and
   add the `storage` field, which must be an absolute URI, to the spec:

   ```bash
   cp -r examples/src/main/python/pipelines/hello_world /tmp/hello_world
   cd /tmp/hello_world
   echo "storage: \"file://$PWD/pipeline-storage\"" >> spark-pipeline.yml
   ```

2. Start a Spark Connect server. Pipelines run on the server, which holds the output tables and
   their metadata. The `pyspark` package ships the server scripts in its `sbin/` directory, which
   is not on the `PATH`:

   ```bash
   SPARK_HOME=$(python -c "import pyspark, os; print(os.path.dirname(pyspark.__file__))")
   "$SPARK_HOME"/sbin/start-connect-server.sh --conf spark.sql.catalogImplementation=hive
   ```

   The server runs in the background on `localhost:15002` and writes `spark-warehouse/` (table
   data) and `metastore_db/` (table metadata) into the directory it was started from. With
   `spark.sql.catalogImplementation=hive` the tables survive a restart of the server; the default
   in-memory catalog keeps them only while the server runs.

3. Run the pipeline against the server:

   ```bash
   spark-pipelines run --remote sc://localhost:15002
   ```

   The run ends with `Run is COMPLETED.` The tables are Parquet files under the server's
   `spark-warehouse/`, their metadata is in the server's catalog, and the streaming checkpoints
   are in `pipeline-storage/`.

4. Query the output through the same server, in the PySpark shell:

   ```bash
   pyspark --remote sc://localhost:15002
   ```

   ```python
   >>> spark.sql("SELECT * FROM revenue_by_country ORDER BY country").show()
   +-------+----------+-------+
   |country|num_orders|revenue|
   +-------+----------+-------+
   |     DE|         1|   35.5|
   |     US|         2|   32.5|
   +-------+----------+-------+
   ```

   Keep this shell open in a second terminal for the next step. `spark-sql` and a plain `pyspark`
   without `--remote` start their own Spark and do not see the server's tables.

5. Add a second batch of orders and run again:

   ```bash
   cp more-data/orders-2.json data/orders/
   spark-pipelines run --remote sc://localhost:15002
   ```

   ```python
   >>> spark.sql("SELECT * FROM revenue_by_country ORDER BY country").show()
   +-------+----------+-------+
   |country|num_orders|revenue|
   +-------+----------+-------+
   |     DE|         2|   40.0|
   |     US|         3|   82.5|
   +-------+----------+-------+
   ```

   The `orders` streaming table reads only the new file; `revenue_by_country` is recomputed.

6. Recompute everything from scratch:

   ```bash
   spark-pipelines run --full-refresh-all --remote sc://localhost:15002
   ```

7. Stop the server when you are done:

   ```bash
   "$SPARK_HOME"/sbin/stop-connect-server.sh
   ```

To start over, stop the server, delete `spark-warehouse/`, `metastore_db/` and `pipeline-storage/`,
and start the server again.
