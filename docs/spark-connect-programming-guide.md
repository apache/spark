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

Spark Connect separates your application from Spark. Your code becomes a small client, Spark runs
as a server somewhere else, and the two talk over the network: the client sends the query plan it
built with the DataFrame API over gRPC, the server analyzes, optimizes and runs it on the cluster,
and the results come back as Arrow batches. The application can be a Python script on a laptop,
a web service, a notebook kernel, a Scala program, or a program in any language that has a Spark
Connect client. It does not need the Spark JARs, and a client written in a language other than
Java or Scala does not need a JVM either.

For the architecture, see the [Spark Connect Overview](spark-connect-overview.html); for operating
the server, including authentication and running behind a proxy, see
[Setting up Spark Connect](spark-connect-setup.html).

## What is Spark Connect?

In a classic Spark application the driver runs inside your program. The driver holds the
`SparkSession`, resolves the tables and columns a query names against the catalog, optimizes and
plans the query, and schedules its tasks on the executors. In PySpark, your Python process starts a
JVM next to it and talks to it through Py4J, and that JVM is the driver. Spark Connect moves the
driver into a long-running server and leaves the DataFrame API in your program;
[How Spark Connect works](spark-connect-overview.html#how-spark-connect-works) describes the
protocol between them.

| Piece | Spark Classic | Spark Connect |
|-------|---------------|---------------|
| Your code and the DataFrame API | your process | your process (the client) |
| `SparkSession` state: temporary views, settings, uploaded files | the driver, in your process | the server |
| Analyzer, catalog, optimizer, scheduling | the driver, in your process | the server |
| Python UDFs | your Python, on the executors | the server's Python, on the executors |
| Cluster manager, workers, executors | the cluster | the cluster, unchanged |
| Results | the driver, in your process | Arrow batches over gRPC |

The cluster does not change, and executors run tasks exactly as before. What changes is that your
program no longer contains a driver: `explain()` shows a plan the server made, a typo in a column
name fails when the plan is sent rather than on the line with the typo, and `sparkContext` is gone
because it belonged to the driver.

## Getting a Spark Connect server

### In your own process

With the Connect dependencies installed, PySpark can start a local Spark Connect server for the
Python process, so the code runs on the Spark Connect path without a separate server to manage. It
is the quickest way to try things and to run tests:

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder.remote("local[*]").getOrCreate()
```

This needs `pyspark[connect]` (or `pyspark-connect`) and Java installed, because PySpark launches a
JVM for the server. The server listens on port 15002 and lives as long as the Python process;
`spark.stop()` shuts it down too. If something else already holds port 15002, the start fails
with `java.net.BindException`. To keep one local server running across runs instead, see
[Faster local iteration with a persistent Connect server](spark-connect-setup.html#faster-local-iteration-with-a-persistent-connect-server).

### From a Spark download

[Download Spark](https://spark.apache.org/downloads.html) (Java 17 or newer is required) and start
the server that ships with it:

```bash
./sbin/start-connect-server.sh        # listens on port 15002
./sbin/stop-connect-server.sh         # when you are done
```

By default the server runs Spark locally on that machine. To have it run jobs on a cluster, point
it at the cluster's master:

```bash
./sbin/start-connect-server.sh --master spark://my-cluster:7077
```

The server listens on every network interface by default, so anything that can reach the machine
can reach the server;
[Starting a Spark Connect server](spark-connect-setup.html#starting-a-spark-connect-server) shows
how to restrict the interface and change the port.

Packages, JARs, SQL extensions, the JARs a catalog loads, and Hive support are all passed to this
command, because they describe the server's JVM; [Configuring the session](#configuring-the-session)
explains why they cannot be set from a client.

### In a container

The official [`apache/spark`](https://hub.docker.com/r/apache/spark) image includes Spark, Java
and Python. Add PyArrow and pandas on top, since Python UDFs use Arrow by default since Spark 4.2:

```dockerfile
FROM apache/spark:{{site.SPARK_VERSION_SHORT}}-scala{{site.SCALA_BINARY_VERSION}}-java21-python3-ubuntu
USER root
RUN python3 -m pip install --no-cache-dir pyarrow pandas
USER spark
```

Start the server with `--wait` as the first argument, which keeps it in the foreground as a
container expects:

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

Note the image's Python version; it matters for the client, as described under
[Python UDFs and the server's Python](#python-udfs-and-the-servers-python).

`start-connect-server.sh` takes `--master` and `--deploy-mode` like `spark-submit`. On YARN the
server can run in cluster deploy mode since Spark 4.2; on Kubernetes it runs in client deploy mode
only. A single server holds many sessions, each isolated from the others but all sharing the
server's memory and executors. Users who need stronger isolation than that need separate servers.

## Connecting from Python

Install the thin client and create the session with `remote()` instead of `master()`:

```bash
pip install pyspark-client=={{site.SPARK_VERSION_SHORT}}
```

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

Everything after the session line is ordinary PySpark, the same code you would run in classic
mode. The program builds the plan and the server optimizes and runs it, so `explain()` prints the
server's plan.

The connection string can also come from the environment. With `SPARK_REMOTE` set, a plain
`SparkSession.builder.getOrCreate()` connects to it, which lets the same script run against
whichever server the variable names:

```bash
export SPARK_REMOTE="sc://localhost:15002"
python my_pipeline.py
```

`pyspark-client` contains only the client: no JARs, no Py4J, no JVM needed. It accepts only `sc://` connection strings; the `local[*]` form needs
`pyspark[connect]` or `pyspark-connect`. [What you need locally](#what-you-need-locally) compares the packages.

### Sessions

`getOrCreate()` returns the existing session whenever one exists (the thread's active session,
otherwise the process default), even when the builder names a different server. A program that talks to several servers, or needs isolated sessions on
one server, uses `create()`, which always opens a new session:

```python
a = SparkSession.builder.remote("sc://server-a:15002").getOrCreate()
b = SparkSession.builder.remote("sc://server-b:15002").getOrCreate()   # returns a
c = SparkSession.builder.remote("sc://server-b:15002").create()        # a new session on server-b
```

`spark.newSession()` (since Spark 4.3) opens another session over the same connection.

Each session has its own temporary views, runtime configuration and uploaded files. `spark.stop()`
releases that one session on the server; the server and every other session keep running (with
`local[*]`, where PySpark started the server for this process, `stop()` shuts it down as well). A client
that exits without `stop()` leaves its session on the server until the server's session timeout
expires it.

## Connecting from Scala

Depend on the client library instead of `spark-sql`:

```scala
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "{{site.SPARK_VERSION_SHORT}}"
```

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

Run it as any JVM application; `spark-submit` is not involved. Two things about the JVM side that
`spark-submit` and `spark-shell` normally hide:

- Depend on `spark-connect-client-jvm` only. `org.apache.spark.sql.SparkSession.builder()` is
  shared between Spark Classic and Spark Connect, and when `spark-sql` is also on the classpath
  it builds a classic session and ignores `remote()`. If both have to be present, set
  `spark.api.mode` to `connect` on the builder (see
  [Switching between Spark Connect and Spark Classic](spark-connect-setup.html#switching-between-spark-connect-and-spark-classic)).
- On JDK 17 and later, start the JVM with `--add-opens=java.base/java.nio=ALL-UNNAMED`. The client
  moves results as Arrow batches, and Arrow's memory layer needs that package open; without it
  the first `createDataFrame` or `collect` fails with `sun.misc.Unsafe or
  java.nio.DirectByteBuffer.<init>(long, int) not available`. `spark-submit` and
  `spark-shell --remote` pass the flag for you.

The Scala client caches sessions by their builder configuration, so two builders with different
connection strings give different sessions, and `create()` always opens a new one.
`spark-shell --remote sc://localhost:15002` gives an interactive shell against the server.

UDFs and other operations that reference classes from your own build need those classes on the
server; see [Shipping code and files](#shipping-code-and-files).

## Connecting from Rust

Because the client only has to build a plan and read Arrow, it does not have to be Python or a JVM
language. The Rust client, [`apache/spark-connect-rust`](https://github.com/apache/spark-connect-rust),
is published on crates.io as `apache-spark-connect`:

```toml
[dependencies]
apache-spark-connect = "4.2"
```

Building the crate needs `protoc`, the Protocol Buffers compiler, on the build machine.

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

`collect_record_batches()` returns the result as Arrow record batches, which an Arrow-native
library can use without a conversion.

## Other clients

The same server takes every client, and they all start with the same connection string. The
Apache Software Foundation hosts clients for
[Go](https://github.com/apache/spark-connect-go) and
[Swift](https://github.com/apache/spark-connect-swift) alongside the Rust client, each released on
its own schedule, and the JDBC driver `spark-connect-client-jdbc` (since Spark 4.1) connects any
JDBC application with a URL of the form `jdbc:sc://host:15002`.

```go
spark, err := sql.NewSessionBuilder().Remote("sc://localhost:15002").Build(ctx)
df, err := spark.Sql(ctx, "SELECT 'apple' AS word, 123 AS count")
df.Show(ctx, 100, false)
```

## Running a job

A Spark Connect application has no separate submit step: running the program sends its queries to
the server. The machine that runs it needs the client library and the server's address, not a
Spark download, a JVM (for Python and Rust) or access to the cluster manager. The program's exit
status is the job's status: an unhandled Spark error ends the process with a non-zero exit code,
which is what a scheduler or CI step checks.

```bash
export SPARK_REMOTE="sc://spark-connect.example.com:15002"

python my_pipeline.py          # Python: the client package only
java --add-opens=java.base/java.nio=ALL-UNNAMED -jar my-pipeline.jar   # Scala or Java, JDK 17+
./target/release/my-pipeline   # Rust: a plain binary
```

The Python client reads `SPARK_REMOTE` when the builder does not call `remote()`, so a program
that builds its session with a plain `SparkSession.builder.getOrCreate()` runs as a Spark Connect
client when the variable is set. When it is not set, the full `pyspark` package runs the program as
a classic job, `pyspark-connect` starts a local Spark Connect server, and `pyspark-client` raises
`CONNECT_URL_NOT_SET`. The Scala client reads the
variable too, and also the `spark.remote` system property. The Rust client takes the address from
the builder only.

Deployment settings that used to be `spark-submit` flags move elsewhere: `--py-files`, `--jars`
and `--files` become `addArtifact` calls in the application (see
[Shipping code and files](#shipping-code-and-files)), while `--master`, `--packages`,
`--driver-memory` and `--conf` for static settings become part of the server's startup command.

### Running with spark-submit

`spark-submit` from a Spark download can start the client for you. It cannot take `--master` or
`--deploy-mode` together with `--remote`; the server already made those choices.

```bash
spark-submit --remote sc://spark-connect.example.com:15002 --class Main my-pipeline.jar
spark-submit --remote sc://spark-connect.example.com:15002 --conf spark.api.mode=connect my_pipeline.py
```

For a Python file the `spark.api.mode=connect` setting is required: without it `spark-submit`
starts the program as a classic job, which fails with `MASTER_URL_NOT_SET` because there is no
master. Setting `SPARK_REMOTE` in the environment instead of passing `--remote` has the same
effect, since the Python process inherits it. For a jar, `--remote` alone is enough; do not add
`spark.api.mode=connect` there, because for JVM programs that setting starts a local server
instead of connecting to the one in `--remote`.

Each run of `spark-submit` still launches a JVM on the submitting machine, so a plain
`python my_pipeline.py` with `SPARK_REMOTE` set is lighter and needs no Spark download.

### Switching an existing job with spark.api.mode

`spark.api.mode` switches a classic job to Spark Connect without changing its code: Spark starts a
Spark Connect server for the job and runs the code as its client. Set it in the session builder and
keep the existing `master`:

```python
spark = (SparkSession.builder
         .master("spark://my-cluster:7077")
         .config("spark.api.mode", "connect")
         .getOrCreate())
```

In PySpark this needs `pyspark[connect]`. [Switching between Spark Connect and Spark Classic](spark-connect-setup.html#switching-between-spark-connect-and-spark-classic)
describes the other forms.

### Running a declarative pipeline

The `spark-pipelines` command line takes the server with `--remote`:

```bash
spark-pipelines run --spec spark-pipeline.yml --remote sc://localhost:15002
```

`spark-pipelines` ships with the `pyspark` package and needs its `pipelines` extra
(`pip install pyspark[pipelines]`); it ignores `SPARK_REMOTE`. With only the thin client, which
includes those dependencies, run `python -m pyspark.pipelines.cli run --spec spark-pipeline.yml`, which has no
`--remote` option and connects to whatever `SPARK_REMOTE` names.

## Configuring the session

A classic application often configures the driver through the session builder: packages, SQL
extensions, Hive support and runtime options.

```python
# Spark Classic: everything is configured by the program that runs the driver.
spark = (
    SparkSession.builder
    .master("spark://my-cluster:7077")
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}}")
    .config("spark.sql.shuffle.partitions", "64")
    .enableHiveSupport()
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")
```

Under Spark Connect, the driver belongs to the server. Anything the server reads once when it starts (packages and
JARs, including the ones catalogs load, `spark.sql.extensions`, Hive support) moves to the command
that starts the server:

```bash
./sbin/start-connect-server.sh \
  --master spark://my-cluster:7077 \
  --packages org.apache.spark:spark-sql-kafka-0-10_{{site.SCALA_BINARY_VERSION}}:{{site.SPARK_VERSION_SHORT}} \
  --conf spark.sql.catalogImplementation=hive
```

and the client keeps what can change per session, which is every runtime SQL setting:

```python
spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .config("spark.sql.shuffle.partitions", "64")
    .getOrCreate()
)
spark.conf.set("spark.sql.session.timeZone", "UTC")
```

The log level moves to the server as well (its `conf/log4j2.properties`), since the logs are the
server's now.

In PySpark, Spark Connect does not stop a client from asking for a startup-only setting; through the
builder it warns and carries on without it. Asking for a package from the client produces this, and
no package:

```
UserWarning: Failed to set spark.jars.packages (CANNOT_MODIFY_CONFIG)
```

(Servers up to the 4.3 line word it as `Failed to set <key> to <value> due to [<condition>]`;
the key and the condition are the stable parts.)

`enableHiveSupport()` does the same with `CANNOT_MODIFY_STATIC_CONFIG` and hands back a session
without Hive, which may go unnoticed until a table is missing. The same keys set after the session
exists, with `spark.conf.set()`, raise instead of warning.

The Scala builder raises from the start: `.config("spark.jars.packages", ...)` makes
`getOrCreate()` fail with `CANNOT_MODIFY_CONFIG`, and a static key such as `spark.sql.extensions`
with `CANNOT_MODIFY_STATIC_CONFIG`. Its `enableHiveSupport()` never reaches the server; the client
logs a warning and the session keeps the default catalog.

Catalogs are the case that looks like an exception. `spark.sql.catalog.<name>` is runtime
configuration, so a client can set it and read it back, but the value is a class the server loads
the first time the catalog is used, and the JAR has to be on the server. Define catalogs and their
JARs in the server's startup command, and have an application that depends on a catalog check for
it when it starts (`spark.sql("SHOW NAMESPACES IN <catalog>")`), so that a server without the
catalog is reported at startup rather than inside a pipeline.

## Shipping code and files

If a UDF imports your own modules, the server does not have them. Ship them with the session using
`addArtifact`, which uploads local files to the session and distributes them to the executors the
way `--py-files`, `--archives` and `--files` do for a `spark-submit` job:

```python
spark.addArtifact("lib/geo.py", pyfile=True)        # importable by your UDFs
spark.addArtifact("model.tar.gz", archive=True)     # unpacked on the executors
spark.addArtifact("lookup.csv", file=True)          # SparkFiles.get("lookup.csv") in UDFs
```

Artifacts belong to the session, so add them before the operation that needs them runs:

```python
from pyspark.sql import functions as F

@F.udf("double")
def km_north(lat):
    from geo import haversine_km      # geo.py lives next to the application, not on the server
    return haversine_km(0.0, 0.0, lat, 0.0)

spark.addArtifact("lib/geo.py", pyfile=True)
spark.range(1).select(km_north(F.lit(1.0))).show()
```

Without the `addArtifact` call the UDF fails with `ModuleNotFoundError: No module named 'geo'`,
raised on an executor when the UDF first runs. `addArtifact` exists in Spark Classic as well since
Spark 4.0, so an application can adopt it before it moves to Spark Connect.

From Scala, `addArtifact` accepts local `.jar` and `.class` files and `ivy://` URIs, and operations
that reference classes from your own build (UDFs, `map`, `filter`) need a `ClassFinder` registered
so that the client can find and upload the compiled class files; `spark-shell --remote` does this
for you:

```scala
spark.addArtifact("/path/to/my-udfs.jar")
spark.addArtifact("ivy://com.example:my-library:1.0.0")

import org.apache.spark.sql.connect.client.REPLClassDirMonitor
spark.registerClassFinder(new REPLClassDirMonitor("/path/to/build/classes"))
```

JARs for catalogs and SQL extensions are the exception: the server loads them before any session
exists, so they have to be installed on the server. `addArtifact` also does not resolve the
transitive dependencies of a local JAR.

## Python UDFs and the server's Python

A Python UDF is pickled on the client and run by Python on the executors, and that interpreter is
the one the server was started with, not the client's. PySpark requires the two minor versions to
match:

```
[PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.12 than that in
driver: 3.11, PySpark cannot run with different minor versions.
```

The message calls the client "driver" because that is where the UDF was pickled. Match the client's
Python to the server's, or install the client's version on the server. Operations that run no
Python on the executors, such as built-in functions and `toPandas()`, do not care.

Packages the UDF imports have to be installed in the server's Python too; `addArtifact` covers
pure-Python modules, as shown above. Since Spark 4.2 Python UDFs are Arrow-optimized by default, so
the server's Python needs PyArrow even for UDFs that never use Arrow themselves. On a server
without it, a Python UDF fails on the executor with `ModuleNotFoundError: No module named
'pyarrow'` while `toPandas()` keeps working. Install PyArrow on the server, or declare the UDF with
`useArrow=False`.

Scala UDFs run in the server's JVM, need their classes uploaded as described above, and have no
interpreter version to match.

## Reading and writing data

Readers and writers are the same as in Spark Classic, but paths are resolved by the server.
`spark.read.text("README.md")` resolves the path against the server's working directory, not the
client's, and a path on the client machine is only readable if the server can reach it, through a
shared file system or object storage. The same applies
to checkpoint locations and output paths.

`spark.createDataFrame(rows)` uploads local rows to the server, which suits small lookup tables and
test fixtures. To parse local strings, `spark.read.json(df)`, `csv(df)` and `xml(df)` (all since
Spark 4.2) read a DataFrame with a single string column, replacing `spark.read.json(rdd)`:

```python
lines = spark.createDataFrame([('{"city": "Toronto", "n": 3}',)], "value string")
spark.read.json(lines).show()
# +-------+---+
# |   city|  n|
# +-------+---+
# |Toronto|  3|
# +-------+---+
```

Output from `print()` inside `foreach`, `foreachPartition` or a streaming `ForeachWriter` goes to
the executor logs, not to the client's terminal.

## Migrating existing code

Most DataFrame code does not change at all. What does change is anything that reached into the
driver, and accumulators are a good example:

```python
# Before: an accumulator, counted on the executors and read on the driver
bad_rows = spark.sparkContext.accumulator(0)
orders.foreach(lambda row: bad_rows.add(1) if row.total is None else None)
print(bad_rows.value)
```

```python
# After: an Observation, collected while the write runs
from pyspark.sql import Observation

obs = Observation("quality")
checked = orders.observe(obs, F.count(F.when(F.col("total").isNull(), 1)).alias("bad_rows"))
checked.write.mode("overwrite").saveAsTable("orders_checked")
print(obs.get["bad_rows"])
```

The metric is collected during the write, without a separate pass over the data. Other common
replacements follow. Most of them already exist in Spark Classic, so they can be made before the
application moves to Spark Connect:

| Spark Classic | Spark Connect |
|---------------|---------------|
| `spark.sparkContext.setLogLevel("WARN")` | set the level in the server's `conf/log4j2.properties` |
| `sc.addPyFile("lib/geo.py")`, `--py-files` | `spark.addArtifact("lib/geo.py", pyfile=True)` |
| `sc.addFile(path)`, `--files` | `spark.addArtifact(path, file=True)` |
| `sc.parallelize(data)` | `spark.createDataFrame(data, schema)`, `spark.range(n)` |
| `df.rdd.map(f)`, `df.rdd.mapPartitions(f)` | a UDF, `df.mapInArrow(f, schema)` or `df.mapInPandas(f, schema)` |
| `df.rdd.zipWithIndex()` | `df.zipWithIndex()` (Spark 4.2) |
| `df.rdd.isEmpty()` | `df.isEmpty()` |
| `spark.read.json(rdd_of_strings)` | `spark.read.json(df)` on a string column (Spark 4.2) |
| `sc.accumulator(0)` | `df.observe(Observation(...), ...)` |
| `sc.broadcast(lookup)` | a broadcast join, `df.join(F.broadcast(lookup_df), ...)` |
| `spark-submit job.py` | `python job.py` with `SPARK_REMOTE` set |

`spark.sparkContext`, `spark._jvm` and `df._jdf` raise `JVM_ATTRIBUTE_NOT_SUPPORTED` in PySpark
(`UNSUPPORTED_CONNECT_FEATURE` in Scala). `df.rdd` raises the same with `pyspark-client` and
`NOT_IMPLEMENTED` with the full `pyspark` packages. Either is the signal that a line needs one of
the replacements above.

The same file can run under `spark-submit` today and as a client tomorrow without branching:
with the full `pyspark` package, `getOrCreate()` returns a Spark Connect session when
`SPARK_REMOTE` is set and a classic one otherwise. Leave the master out of the builder. Under `spark-submit` the master comes from the
command line, and a builder option would override it (builder options are set on the `SparkConf`,
which takes precedence over `spark-submit`). Run as plain `python job.py` without `SPARK_REMOTE`,
the classic session defaults to `local[*]`.

```python
def build_session(app_name):
    # getOrCreate() connects to SPARK_REMOTE when it is set and otherwise builds a
    # classic session, whose master comes from spark-submit.
    return SparkSession.builder.appName(app_name).getOrCreate()
```

The main behavioral difference affects code that migrates without an error: Spark Classic checks a
DataFrame against the catalog the moment it is defined, while Spark Connect sends nothing to the
server until a result is needed. A typo in a column name fails at the `show()` or `write` rather
than at the `select`, a DataFrame built on a temporary view reads whatever the view points to when
it runs, and a UDF reads a variable's value when it runs. [Eager vs Lazy: Spark Connect vs Spark Classic](spark-connect-gotchas.html)
works through each case and how to write code that behaves the same in both modes, along with
the [session and configuration differences](spark-connect-gotchas.html#session-and-configuration-gotchas).

## What you need locally

You need Python 3.11 or later (the server's minor version, if you use Python UDFs) and the `pyspark-client`
package with its dependencies (PyArrow, pandas, NumPy, gRPC, PyYAML, zstandard). No Java, no Spark download, no
`SPARK_HOME`; that all lives on the server.

PyPI has three PySpark packages with nearly identical names, and `pyspark` has a `connect` extra:

| Package | What it installs | Java needed locally |
|---------|------------------|---------------------|
| `pyspark` | Full PySpark with the Spark JARs; the Spark Connect dependencies are not included | yes |
| `pyspark[connect]` | Full PySpark plus the Spark Connect dependencies; `remote()` accepts `sc://` and `local[*]` | yes |
| `pyspark-connect` | Full PySpark plus the Spark Connect dependencies, with `spark.api.mode=connect` as the default | yes |
| `pyspark-client` | The thin client: no JARs, no Py4J; `remote()` accepts `sc://` only | no |

`pyspark-connect` is not the thin client; `pyspark-client` is. To check which one an environment has, look for the JARs: `pyspark-client` installs no
`pyspark/jars` directory. If a full Spark is installed elsewhere on the machine, clear `SPARK_HOME`
and `PYTHONPATH` before starting, or they can shadow the thin client.

## Client and server versions

The client and the server do not have to run the same Spark version. The supported direction is an
older client against a newer server: the server's RPC definitions are kept backwards compatible,
which is what lets a server be upgraded without touching its applications. The other way round can
fail wherever the newer client relies on an RPC or a setting the server does not have; a 4.2 or
later client on a 4.1 server runs SQL and reads tables but cannot build a DataFrame from a local
list, because the older server reports a size limit in a format the newer client does not parse. Keep the client
at or below the server's version, and print `spark.version` at startup in any application that can
be pointed at servers of different versions.

## Debugging

**Which server answered.** A connection string does not identify the Spark version behind it, and
a server left running from an earlier project can answer with errors that look like bugs. Print `spark.version` first. The related trap in PySpark is `getOrCreate()`
returning the process's existing session for a different URL, described under [Sessions](#sessions).
A connection string that looks fine but is not is reported by the Python client as
`INVALID_CONNECT_URL`: the path must be empty (`/;key=value` parameters are fine), and parameter
values cannot contain `/`.

**Error classes.** Spark Connect errors carry an error class in square brackets that says
which side of the boundary the problem is on. `JVM_ATTRIBUTE_NOT_SUPPORTED` (or `NOT_IMPLEMENTED`
for `df.rdd` with the full `pyspark` packages) means the code reached for something that lived in
the local JVM (`sparkContext`, `rdd`, `_jvm`); use the replacement from
[Migrating existing code](#migrating-existing-code). `PYTHON_VERSION_MISMATCH` and a
`ModuleNotFoundError` inside a UDF point at the server's Python environment.
`CANNOT_MODIFY_CONFIG` and `CANNOT_MODIFY_STATIC_CONFIG` mean the setting belongs to the server's
startup command. In PySpark, `UNSUPPORTED_LOCAL_CONNECTION_STRING` means `create()` was called with
`local[*]`. `enableHiveSupport()` does not raise at all; it returns a session without
Hive (PySpark warns; the Scala client only logs), so read the warnings.

**Deferred errors.** A typo in a column name fails at the first action, schema access or
`explain()`, not on the line with the typo, and a `try`/`except` around the definition catches
nothing. `df.explain()` or `df.schema` forces analysis, which surfaces a deferred error at the
point where the DataFrame is built.

**Server UI and logs.** The server is a Spark application, so its Spark UI (port 4040 by
default) shows the jobs, stages and SQL queries that client sessions ran, and the **Connect** tab
lists every client session and request, which confirms which server an application reached and
whether it released its session. The driver logs are the server's logs, under `logs/` in the Spark
folder or in `docker logs` for a container. `df.explain()` prints the plan the server actually
built, and `spark.interruptAll()` cancels every running operation of the session without stopping
it.

**Version skew.** See [Client and server versions](#client-and-server-versions).
