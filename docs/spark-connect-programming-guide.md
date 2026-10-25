---
layout: global
title: Spark Connect Programming Guide
displayTitle: Spark Connect Programming Guide
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

Spark Connect is a client-server interface to Spark. A client application builds DataFrame queries
as unresolved logical plans and sends them to a Spark Connect server over gRPC; the server analyzes,
optimizes and runs them on the cluster and returns the results as Arrow record batches. Clients
exist for Python, Scala, Go, Rust and Swift, and for JDBC.

For the architecture, see the [Spark Connect Overview](spark-connect-overview.html). For running
and securing the server, see [Setting up Spark Connect](spark-connect-setup.html).

## Spark Connect and Spark Classic

In Spark Classic, the driver runs in the application's process. In Spark Connect, the driver runs
in the server, and the application holds only the client.

| Component | Spark Classic | Spark Connect |
|-----------|---------------|---------------|
| Application code and the DataFrame API | application process | application process (the client) |
| `SparkSession` state: temporary views, configuration, artifacts | driver, in the application process | server |
| Analyzer, catalog, optimizer, scheduler | driver, in the application process | server |
| Python UDFs | application's Python environment, on the executors | server's Python environment, on the executors |
| Cluster manager, workers, executors | cluster | cluster |
| Results | driver, in the application process | Arrow batches over gRPC |

As a consequence, `SparkContext` and RDDs are not available, `explain()` prints the plan built by
the server, and analysis errors such as an unknown column are reported when the plan is sent to the
server, not when the DataFrame is defined. See
[Eager vs Lazy: Spark Connect vs Spark Classic](spark-connect-gotchas.html) for details.

## Installing the client

### Python packages

All PySpark packages require Python 3.11 or later. PyPI has three PySpark packages, and `pyspark`
has a `connect` extra:

| Package | Contents | Java required |
|---------|----------|---------------|
| `pyspark` | Full PySpark with the Spark JARs, without the Spark Connect dependencies | yes |
| `pyspark[connect]` | Full PySpark with the Spark Connect dependencies; `remote()` accepts `sc://` and `local[*]` | yes |
| `pyspark-connect` | Full PySpark with the Spark Connect dependencies, and `spark.api.mode` defaulting to `connect` | yes |
| `pyspark-client` | The Spark Connect client only, with no JARs and no Py4J; `remote()` accepts `sc://` only | no |

`pyspark-client` is the package for applications that connect to a remote server:

```bash
pip install pyspark-client=={{site.SPARK_VERSION_SHORT}}
```

Its dependencies are PyArrow, pandas, NumPy, gRPC, PyYAML and zstandard. It installs no
`pyspark/jars` directory. If a full Spark installation is present on the same machine, unset
`SPARK_HOME` and `PYTHONPATH` so that they do not shadow the client.

### Scala dependency

Add `spark-connect-client-jvm`, not `spark-sql`:

```scala
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "{{site.SPARK_VERSION_SHORT}}"
```

When `spark-sql` is also on the classpath, `SparkSession.builder()` builds a Spark Classic session
and ignores `remote()`, unless `spark.api.mode` is set to `connect`.

On JDK 17 and later, start the JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`. Arrow, which
the client uses for results, requires it; without it, the first `createDataFrame` or `collect`
fails with `sun.misc.Unsafe or java.nio.DirectByteBuffer.<init>(long, int) not available`.
`spark-submit` and `spark-shell --remote` add the option automatically.

### Rust crate

The Rust client, [`apache/spark-connect-rust`](https://github.com/apache/spark-connect-rust), is
published on crates.io as `apache-spark-connect`. Building it requires `protoc`, the Protocol
Buffers compiler.

```toml
[dependencies]
apache-spark-connect = "4.2"
```

### Other clients

The Apache Software Foundation also hosts clients for
[Go](https://github.com/apache/spark-connect-go) and
[Swift](https://github.com/apache/spark-connect-swift). Each client outside the Spark repository
has its own release schedule. The JDBC driver `spark-connect-client-jdbc` (since Spark 4.1) accepts
URLs of the form `jdbc:sc://host:15002`.

## Starting a server

The examples in this guide connect to a server at `sc://localhost:15002`.

### From PySpark

With `pyspark[connect]` or `pyspark-connect` and Java installed, `remote("local[*]")` starts a
local Spark Connect server in a JVM launched by PySpark and connects to it:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("local[*]").getOrCreate()
```

The server listens on port 15002 and runs until the Python process exits or `spark.stop()` is
called. If port 15002 is in use, the server fails to start with `java.net.BindException`. To keep
one server running across runs, see
[Faster local iteration with a persistent Connect server](spark-connect-setup.html#faster-local-iteration-with-a-persistent-connect-server).

### From a Spark distribution

[Download Spark](https://spark.apache.org/downloads.html) (Java 17 or later is required) and run:

```bash
./sbin/start-connect-server.sh
./sbin/stop-connect-server.sh
```

`start-connect-server.sh` accepts the same options as `spark-submit`, including `--master`,
`--deploy-mode`, `--packages`, `--jars` and `--conf`. On YARN, the server can run in cluster deploy
mode since Spark 4.2. On Kubernetes, it runs in client deploy mode only.

```bash
./sbin/start-connect-server.sh --master spark://my-cluster:7077
```

By default, the server listens on all network interfaces. See
[Starting a Spark Connect server](spark-connect-setup.html#starting-a-spark-connect-server) for the
binding address, the port and the log location.

### In a container

The [`apache/spark`](https://hub.docker.com/r/apache/spark) image includes Spark, Java and Python.
Python UDFs are Arrow-optimized by default since Spark 4.2, so add PyArrow and pandas:

```dockerfile
FROM apache/spark:{{site.SPARK_VERSION_SHORT}}-scala{{site.SCALA_BINARY_VERSION}}-java21-python3-ubuntu
USER root
RUN python3 -m pip install --no-cache-dir pyarrow pandas
USER spark
```

Pass `--wait` as the first argument to keep the server in the foreground:

```yaml
services:
  spark-connect:
    build: .
    entrypoint:
      - /opt/spark/sbin/start-connect-server.sh
      - --wait
    ports:
      - "127.0.0.1:15002:15002"   # Spark Connect
      - "127.0.0.1:4040:4040"     # Spark UI
```

Clients that use Python UDFs must use the same Python minor version as the image; see
[Python UDFs](#python-udfs).

A server holds many sessions. Sessions are isolated from each other but share the server's memory
and executors; applications that need stronger isolation need separate servers.

## Connecting to a server

### Python

```python
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
print(spark.version)            # the server's Spark version

orders = spark.createDataFrame(
    [("metro", "pizza", 24.0), ("metro", "sushi", 40.0), ("urban", "pizza", 18.0)],
    "region string, item string, total double",
)
orders.groupBy("region").agg(F.round(F.avg("total"), 2).alias("avg_ticket")).orderBy("region").show()
# +------+----------+
# |region|avg_ticket|
# +------+----------+
# | metro|      32.0|
# | urban|      18.0|
# +------+----------+
```

When the builder does not call `remote()`, the connection string is read from the `SPARK_REMOTE`
environment variable:

```bash
export SPARK_REMOTE="sc://localhost:15002"
python my_app.py
```

If `SPARK_REMOTE` is not set either, the result of `SparkSession.builder.getOrCreate()` depends on
the package:

| Package | `getOrCreate()` without `remote()` or `SPARK_REMOTE` |
|---------|------------------------------------------------------|
| `pyspark`, `pyspark[connect]` | a Spark Classic session |
| `pyspark-connect` | a session on a local Spark Connect server |
| `pyspark-client` | raises `CONNECT_URL_NOT_SET` |

### Scala

```scala
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().remote("sc://localhost:15002").getOrCreate()
println(spark.version)          // the server's Spark version

val orders = spark.createDataFrame(Seq(
  ("metro", "pizza", 24.0), ("metro", "sushi", 40.0), ("urban", "pizza", 18.0)
)).toDF("region", "item", "total")
orders.groupBy("region").agg(round(avg("total"), 2).as("avg_ticket")).show()
```

The Scala client also reads `SPARK_REMOTE` and the `spark.remote` system property.
`spark-shell --remote sc://localhost:15002` starts an interactive shell connected to the server.

### Rust

```rust
use spark_connect::{SparkSession, col, lit, functions as f};

fn main() -> Result<(), Box<dyn std::error::Error>> {
    let spark = SparkSession::builder().remote("sc://localhost:15002").get_or_create()?;
    println!("Spark {}", spark.version()?);          // the server's Spark version

    let orders = spark.sql(
        "SELECT * FROM VALUES ('metro', 'pizza', 24.0D), ('metro', 'sushi', 40.0D),
                              ('urban', 'pizza', 18.0D) AS t(region, item, total)",
    )?;
    let avg_ticket = f::round_scale(f::avg(col("total")), lit(2)).alias("avg_ticket");
    orders
        .group_by([col("region")])
        .agg(vec![avg_ticket.expression().clone()])
        .show(10)?;

    let batches = orders.collect_record_batches()?;
    println!("{} rows in {} batches", batches.iter().map(|b| b.num_rows()).sum::<usize>(), batches.len());
    Ok(())
}
```

`collect_record_batches()` returns the result as Arrow record batches. The Rust client takes the
connection string from the builder only.

### Go

```go
spark, err := sql.NewSessionBuilder().Remote("sc://localhost:15002").Build(ctx)
df, err := spark.Sql(ctx, "SELECT 'apple' AS word, 123 AS count")
df.Show(ctx, 100, false)
```

### Connection strings

A connection string has the form `sc://host:port/;param1=value;param2=value`. The path must be
empty, and parameter values cannot contain `/`. See
[Connection strings](spark-connect-setup.html#connection-strings) for the parameters.

## Sessions

Each session has its own temporary views, runtime configuration and artifacts.

| Call | Result |
|------|--------|
| `builder.getOrCreate()` | In PySpark, the active session if one exists (the thread's active session, otherwise the process default), even if the builder names a different server; otherwise a new session. The Scala client caches sessions by builder configuration. |
| `builder.create()` | Always a new session. Requires an `sc://` connection string; `local[*]` raises `UNSUPPORTED_LOCAL_CONNECTION_STRING`. |
| `spark.newSession()` | A new session over the same connection (since Spark 4.3). |
| `spark.stop()` | Releases the session on the server. Other sessions and the server keep running, except with `local[*]`, where the server stops too. |

```python
a = SparkSession.builder.remote("sc://server-a:15002").getOrCreate()
b = SparkSession.builder.remote("sc://server-b:15002").getOrCreate()   # returns a
c = SparkSession.builder.remote("sc://server-b:15002").create()        # a new session on server-b
```

A session that is not stopped remains on the server until the server's session timeout expires.

## Running an application

A Spark Connect application runs as an ordinary program and needs no `spark-submit`. The machine
that runs it needs the client and the server's address; it does not need a Spark distribution or
access to the cluster manager, and Python and Rust clients do not need a JVM.

```bash
export SPARK_REMOTE="sc://spark-connect.example.com:15002"

python my_app.py
java --add-opens=java.base/java.nio=ALL-UNNAMED -jar my-app.jar
./target/release/my-app
```

An unhandled Spark error ends the program with a non-zero exit code.

`spark-submit` options map to Spark Connect as follows:

| `spark-submit` option | Spark Connect |
|-----------------------|---------------|
| `--py-files`, `--jars`, `--files`, `--archives` | `spark.addArtifact()` in the application; see [Artifacts](#artifacts) |
| `--master`, `--deploy-mode`, `--packages`, `--driver-memory` | options of `start-connect-server.sh` |
| `--conf` for runtime SQL configuration | `builder.config()` or `spark.conf.set()` |
| `--conf` for static or core configuration | `--conf` of `start-connect-server.sh` |

### spark-submit

`spark-submit` can run a client application with `--remote`. It does not accept `--master` or
`--deploy-mode` together with `--remote`.

```bash
spark-submit --remote sc://spark-connect.example.com:15002 --class Main my-app.jar
spark-submit --remote sc://spark-connect.example.com:15002 --conf spark.api.mode=connect my_app.py
```

For a Python file, `--conf spark.api.mode=connect` is required; without it the program starts as a
Spark Classic application and fails with `MASTER_URL_NOT_SET`. Setting `SPARK_REMOTE` instead of
`--remote` has the same effect. For a JAR, do not set `spark.api.mode=connect`: for JVM applications
it starts a local server instead of connecting to the `--remote` server.

### spark.api.mode

`spark.api.mode=connect` runs a Spark Classic application on Spark Connect without code changes.
Spark starts a Spark Connect server with the application's master and runs the application as its
client. In PySpark, this requires `pyspark[connect]`.

```python
spark = (SparkSession.builder
         .master("spark://my-cluster:7077")
         .config("spark.api.mode", "connect")
         .getOrCreate())
```

See [Switching between Spark Connect and Spark Classic](spark-connect-setup.html#switching-between-spark-connect-and-spark-classic).

### spark-pipelines

```bash
spark-pipelines run --spec spark-pipeline.yml --remote sc://localhost:15002
```

`spark-pipelines` is included in the `pyspark` package and requires the `pipelines` extra
(`pip install pyspark[pipelines]`). It does not read `SPARK_REMOTE`. With `pyspark-client`, run
`python -m pyspark.pipelines.cli run --spec spark-pipeline.yml`, which connects to the server in
`SPARK_REMOTE` and has no `--remote` option.

## Configuration

A client can set runtime configuration for its own session, including runtime SQL configuration and
catalog definitions (`spark.sql.catalog.<name>`):

```python
spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

Static SQL configuration (such as `spark.sql.extensions` and `spark.sql.catalogImplementation`),
and core configuration (such as `spark.jars` and `spark.jars.packages`) are fixed when the server
starts. Set them on `start-connect-server.sh`:

```bash
./sbin/start-connect-server.sh \
  --master spark://my-cluster:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} \
  --conf spark.sql.catalogImplementation=hive
```

Log levels are set in the server's `conf/log4j2.properties`.

Setting a static or core key from the client has no effect on the server:

| Client call | Result |
|-------------|--------|
| PySpark `builder.config(key, value)` | `UserWarning: Failed to set <key> (CANNOT_MODIFY_CONFIG)` or `(CANNOT_MODIFY_STATIC_CONFIG)`; the session is created without the setting |
| PySpark `builder.enableHiveSupport()` | the same warning for `spark.sql.catalogImplementation`; the session keeps the server's catalog |
| Scala `builder.config(key, value)` | `getOrCreate()` raises `CANNOT_MODIFY_CONFIG` or `CANNOT_MODIFY_STATIC_CONFIG` |
| Scala `builder.enableHiveSupport()` | dropped by the client with a log warning; the session keeps the server's catalog |
| `spark.conf.set(key, value)` | raises `CANNOT_MODIFY_CONFIG` or `CANNOT_MODIFY_STATIC_CONFIG` |

Servers up to the 4.3 line word the PySpark warning as
`Failed to set <key> to <value> due to [<condition>] ...`.

A catalog's class is loaded by the server the first time the catalog is used, and the catalog's JAR
must be on the server. A missing JAR fails at that point with `CANNOT_LOAD_CATALOG`. Applications
that depend on a catalog can check it at startup, for example with
`spark.sql("SHOW NAMESPACES IN <catalog>")`.

## Artifacts

`spark.addArtifact()` uploads local files to the session and distributes them to the executors,
like `--py-files`, `--archives` and `--files` for `spark-submit`. Add artifacts before the
operation that uses them runs.

```python
spark.addArtifact("lib/geo.py", pyfile=True)        # importable by Python UDFs
spark.addArtifact("model.tar.gz", archive=True)     # unpacked on the executors
spark.addArtifact("lookup.csv", file=True)          # SparkFiles.get("lookup.csv") in UDFs
```

```python
from pyspark.sql import functions as F

@F.udf("double")
def km_north(lat):
    from geo import haversine_km
    return haversine_km(0.0, 0.0, lat, 0.0)

spark.addArtifact("lib/geo.py", pyfile=True)
spark.range(1).select(km_north(F.lit(1.0))).show()
```

Without the `addArtifact` call, the UDF fails on the executor with
`ModuleNotFoundError: No module named 'geo'`. `addArtifact` is also available in Spark Classic since
Spark 4.0.

In Scala, `addArtifact` accepts local `.jar` and `.class` files and `ivy://` URIs. Operations that
reference classes from the application's build (UDFs, `map`, `filter`) also require a
`ClassFinder` that uploads the compiled classes. `spark-shell --remote` registers one
automatically.

```scala
spark.addArtifact("/path/to/my-udfs.jar")
spark.addArtifact("ivy://com.example:my-library:1.0.0")

import org.apache.spark.sql.connect.client.REPLClassDirMonitor
spark.registerClassFinder(new REPLClassDirMonitor("/path/to/build/classes"))
```

`addArtifact` does not resolve the transitive dependencies of a local JAR. JARs for catalogs and SQL
extensions are loaded when the server starts and must be installed on the server.

## Python UDFs

Python UDFs are pickled by the client and run by the server's Python on the executors. The client
and the server must use the same Python minor version; otherwise the UDF fails with:

```
[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.12 than that in
driver: 3.11, PySpark cannot run with different minor versions.
```

In this message, "driver" is the client. Built-in functions and `toPandas()` do not run Python on
the executors and are not affected.

Packages that a UDF imports must be installed in the server's Python environment, or uploaded with
`addArtifact` if they are pure Python. Since Spark 4.2, Python UDFs are Arrow-optimized by default,
so the server's Python needs PyArrow; without it, a Python UDF fails on the executor with
`ModuleNotFoundError: No module named 'pyarrow'`. Install PyArrow on the server, or declare the UDF
with `useArrow=False`.

Scala UDFs run in the server's JVM. Their classes must be uploaded as described in
[Artifacts](#artifacts).

## Reading and writing data

The server resolves paths. `spark.read.text("README.md")` reads `README.md` relative to the
server's working directory, and a path on the client machine is readable only through a file
system or object store that the server can also access. The same applies to checkpoint locations
and output paths.

`spark.createDataFrame(rows)` uploads local rows to the server. `spark.read.json(df)`, `csv(df)`
and `xml(df)` (since Spark 4.2) parse a DataFrame with a single string column:

```python
lines = spark.createDataFrame([('{"city": "Toronto", "n": 3}',)], "value string")
spark.read.json(lines).show()
# +-------+---+
# |   city|  n|
# +-------+---+
# |Toronto|  3|
# +-------+---+
```

Output of `print()` in `foreach`, `foreachPartition` or a streaming `ForeachWriter` goes to the
executor logs.

## Migrating from Spark Classic

DataFrame and SQL code runs unchanged. Code that uses the driver directly must be replaced:

| Spark Classic | Spark Connect |
|---------------|---------------|
| `spark.sparkContext.setLogLevel("WARN")` | `conf/log4j2.properties` on the server |
| `sc.addPyFile("lib/geo.py")`, `--py-files` | `spark.addArtifact("lib/geo.py", pyfile=True)` |
| `sc.addFile(path)`, `--files` | `spark.addArtifact(path, file=True)` |
| `sc.parallelize(data)` | `spark.createDataFrame(data, schema)`, `spark.range(n)` |
| `df.rdd.map(f)`, `df.rdd.mapPartitions(f)` | a UDF, `df.mapInArrow(f, schema)` or `df.mapInPandas(f, schema)` |
| `df.rdd.zipWithIndex()` | `df.zipWithIndex()` (since Spark 4.2) |
| `df.rdd.isEmpty()` | `df.isEmpty()` |
| `spark.read.json(rdd_of_strings)` | `spark.read.json(df)` on a string column (since Spark 4.2) |
| `sc.accumulator(0)` | `df.observe(Observation(...), ...)` |
| `sc.broadcast(lookup)` | a broadcast join, `df.join(F.broadcast(lookup_df), ...)` |
| `spark-submit job.py` | `python job.py` with `SPARK_REMOTE` set |

Most of these replacements also work in Spark Classic. For example, an accumulator that counts rows
becomes an `Observation`, which collects the metric while the query runs:

```python
# Spark Classic only
bad_rows = spark.sparkContext.accumulator(0)
orders.foreach(lambda row: bad_rows.add(1) if row.total is None else None)
print(bad_rows.value)
```

```python
# Spark Classic and Spark Connect
from pyspark.sql import Observation

obs = Observation("quality")
checked = orders.observe(obs, F.count(F.when(F.col("total").isNull(), 1)).alias("bad_rows"))
checked.write.mode("overwrite").saveAsTable("orders_checked")
print(obs.get["bad_rows"])
```

In PySpark, `spark.sparkContext`, `spark._jvm` and `df._jdf` raise `JVM_ATTRIBUTE_NOT_SUPPORTED`.
`df.rdd` raises `JVM_ATTRIBUTE_NOT_SUPPORTED` with `pyspark-client` and `NOT_IMPLEMENTED` with the
other packages. In Scala, these raise `UNSUPPORTED_CONNECT_FEATURE`.

With the `pyspark` package, an application that creates its session with
`SparkSession.builder.getOrCreate()` runs on Spark Connect when `SPARK_REMOTE` is set and on Spark
Classic otherwise. Do not set the master in the builder: builder options override the
`spark-submit` command line. Without `SPARK_REMOTE` or `spark-submit`, the master defaults to
`local[*]`.

```python
def build_session(app_name):
    return SparkSession.builder.appName(app_name).getOrCreate()
```

Spark Connect analyzes a DataFrame when it is executed, not when it is defined. This changes when
errors are reported and how temporary views and UDF closures are resolved. See
[Eager vs Lazy: Spark Connect vs Spark Classic](spark-connect-gotchas.html), including the
[session and configuration gotchas](spark-connect-gotchas.html#session-and-configuration-gotchas).

## Client and server versions

An older client can connect to a newer server; the server keeps its RPC definitions backward
compatible. A newer client on an older server can fail when it uses an RPC or a setting that the
server does not have. Keep the client version at or below the server version, and check `spark.version` in applications that can connect to servers of different
versions.

## Troubleshooting

| Symptom | Cause |
|---------|-------|
| Unexpected errors from a server that worked before | A different server is listening on the address, possibly a different Spark version. Check `spark.version`. |
| `getOrCreate()` returns a session on the wrong server | PySpark returns the existing session; see [Sessions](#sessions). |
| `INVALID_CONNECT_URL` | The connection string has a non-empty path or a parameter value containing `/`. |
| `JVM_ATTRIBUTE_NOT_SUPPORTED`, `NOT_IMPLEMENTED` | The code uses `sparkContext`, `rdd` or another driver object; see [Migrating from Spark Classic](#migrating-from-spark-classic). |
| `CANNOT_MODIFY_CONFIG`, `CANNOT_MODIFY_STATIC_CONFIG` | The setting must be passed to the server at startup; see [Configuration](#configuration). |
| `UNSUPPORTED_LOCAL_CONNECTION_STRING` | `create()` was called with `local[*]`. |
| `PYTHON_VERSION_MISMATCH`, `ModuleNotFoundError` in a UDF | The server's Python environment; see [Python UDFs](#python-udfs). |
| A table is missing after `enableHiveSupport()` | Hive support must be enabled on the server; see [Configuration](#configuration). |
| An analysis error is raised at an action, not where the DataFrame is defined | Analysis is deferred. Call `df.schema` or `df.explain()` to analyze earlier. |

The server's Spark UI (port 4040 by default) shows the jobs, stages and SQL queries of all
sessions, and its **Connect** tab lists sessions and requests. The server's logs are in `logs/`
under the Spark directory, or in the container's output. `spark.interruptAll()` cancels all
running operations of the session.
