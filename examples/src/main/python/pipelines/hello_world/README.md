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
`-- more-data/orders-2.json       # second batch, added in step 4
```

## Requirements

- Java 21 or 25, with `java` on the `PATH` or `JAVA_HOME` set.
- Python 3.11 or later.
- PySpark with the `pipelines` extra:

  ```bash
  pip install "pyspark[pipelines]"
  ```

  From a Spark distribution, use `bin/spark-pipelines` and `bin/spark-sql` in place of the
  commands below.

## Run the example

1. Copy the project out of the Spark source tree, so the run's output stays out of it:

   ```bash
   cp -r examples/src/main/python/pipelines/hello_world /tmp/hello_world
   cd /tmp/hello_world
   ```

2. Run the pipeline:

   ```bash
   spark-pipelines run
   ```

   The run ends with `Run is COMPLETED.` Table data is written to `spark-warehouse/`, table
   metadata to the local Hive metastore in `metastore_db/`, and streaming checkpoints to
   `pipeline-storage/`.

3. Query the output:

   ```bash
   spark-sql -e "SELECT * FROM revenue_by_country ORDER BY country"
   ```

   ```
   DE	1	35.5
   US	2	32.5
   ```

4. Add a second batch of orders and run again:

   ```bash
   cp more-data/orders-2.json data/orders/
   spark-pipelines run
   spark-sql -e "SELECT * FROM revenue_by_country ORDER BY country"
   ```

   ```
   DE	2	40.0
   US	3	82.5
   ```

   The `orders` streaming table reads only the new file; `revenue_by_country` is recomputed.

5. Recompute everything from scratch:

   ```bash
   spark-pipelines run --full-refresh-all
   ```

Run `spark-pipelines` and `spark-sql` from the project directory: both keep the Hive metastore in
`metastore_db/` under the current directory. To start over, delete `spark-warehouse/`,
`metastore_db/` and `pipeline-storage/`.
