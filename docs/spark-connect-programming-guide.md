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
---

* Table of contents
{:toc}

## What is Spark Connect for an application?

Spark Connect lets an application use Spark without running inside the Spark driver. The
application links a small client library, opens a session against a Spark Connect server over
gRPC, and builds DataFrames as usual; the client sends the resulting plans to the server, the
server runs them on the cluster, and the results stream back as Arrow batches. The application
can be a Python script on a laptop, a web service, a notebook kernel or a Scala program, and it
needs neither the Spark JARs nor a JVM of its own.

For the application author this changes a few things that were previously implicit:

- The session is a handle to state that lives on the server. Temporary views, runtime SQL
  configuration, registered UDFs and uploaded files belong to that server-side session.
- Everything that used to reach the driver process directly, such as `SparkContext`, RDDs,
  `spark-submit` options and the driver's file system, has a DataFrame-level replacement or
  belongs to the server's configuration instead.
- Python UDFs run in the server's Python interpreter, not the client's.

The [Spark Connect Overview](spark-connect-overview.html) describes the architecture and what is
supported. [Setting up Spark Connect](spark-connect-setup.html) covers running a server and
using the shells. [Eager vs Lazy: Spark Connect vs Spark Classic](spark-connect-gotchas.html)
covers deferred analysis, the one behavioral difference that affects existing DataFrame code.

## Quick Start

Start a server, or use one that is already running (see
[Starting a Spark Connect server](spark-connect-setup.html#starting-a-spark-connect-server)):

```bash
./sbin/start-connect-server.sh
```

Install the client and run a query against it:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight bash %}
pip install pyspark-client=={{site.SPARK_VERSION_SHORT}}
{% endhighlight %}

{% highlight python %}
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

df = spark.createDataFrame([(1, "Sarah"), (2, "Maria")], "id int, name string")
df.filter(F.col("id") > 1).show()
# +---+-----+
# | id| name|
# +---+-----+
# |  2|Maria|
# +---+-----+

print(spark.version)   # the version of the server, e.g. {{site.SPARK_VERSION_SHORT}}
spark.stop()
{% endhighlight %}

Run it with the Python interpreter, `python app.py`, with no `spark-submit` involved.
`pyspark-client` contains only the client; `pip install pyspark[connect]` installs the full
distribution with the client's dependencies instead (see [Packages](#packages)).
</div>

<div data-lang="scala"  markdown="1">
{% highlight sbt %}
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "{{site.SPARK_VERSION_SHORT}}"
{% endhighlight %}

{% highlight scala %}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val spark = SparkSession.builder().remote("sc://localhost:15002").getOrCreate()

val df = spark.createDataFrame(Seq((1, "Sarah"), (2, "Maria"))).toDF("id", "name")
df.filter(col("id") > 1).show()
// +---+-----+
// | id| name|
// +---+-----+
// |  2|Maria|
// +---+-----+

println(spark.version)   // the version of the server, e.g. {{site.SPARK_VERSION_SHORT}}
spark.stop()
{% endhighlight %}

Run it as any JVM application. UDFs and other operations that reference your own classes need a
`ClassFinder` registered so the client can upload the class files; see
[Dependencies](#dependencies).
</div>
</div>

## Key Concepts

### The session is a handle to a server-side session

`SparkSession.builder.remote(url)` opens a gRPC channel and asks the server for a session. The
object you get back is a `pyspark.sql.connect.session.SparkSession` (Python) or
`org.apache.spark.sql.connect.SparkSession` (Scala), a thin handle: `spark.version` is the
server's version, `spark.conf` reads and writes the server-side session's configuration, and
`spark.stop()` releases that one session on the server while the server and every other session
keep running. A client process that exits without `stop()` leaves its session, cached data and
temporary views on the server until the server's session timeout expires them.

Because the session is remote, a few attributes of the classic `SparkSession` do not exist:
`spark.sparkContext`, `DataFrame.rdd` and the Py4J internals (`spark._jvm`, `df._jdf`) raise
`JVM_ATTRIBUTE_NOT_SUPPORTED` in Python and `UNSUPPORTED_CONNECT_FEATURE` in Scala.
[Migrating from Spark Classic](#migrating-from-spark-classic) lists the replacements.

### Plans are analyzed on the server, lazily

A DataFrame on the client is an unresolved plan. The client does not check column names, view
names or types when you build it; those checks happen on the server when the plan is sent, which
is at an action (`collect`, `show`, `write`), at a schema access (`df.schema`, `df.columns`) or
at `df.explain()`. A typo in a column name therefore surfaces at the first of those calls, not at
the `select`. The [Eager vs Lazy](spark-connect-gotchas.html) page describes the consequences
for temporary views, UDF closures and error handling, and how to write code that behaves the
same in both modes.

### The client configures its session, the server configures itself

A client can change runtime SQL configuration of its own session (`spark.sql.shuffle.partitions`,
`spark.sql.session.timeZone`, and every other setting that the server reads while planning or
running a query). It cannot change what the server read when it started: static SQL
configuration such as `spark.sql.catalogImplementation` and `spark.sql.extensions`, and core
settings such as `spark.jars`, `spark.jars.packages` and `spark.executor.memory`. Those belong to
the `start-connect-server.sh` command line. [Configuration](#configuration) shows what happens
at the boundary.

## Writing Applications

### Sessions

A session is created with `getOrCreate()` or `create()`:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql import SparkSession

# Reuse the process's active session if there is one, otherwise create one.
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()

# Always create a new server-side session.
other = SparkSession.builder.remote("sc://localhost:15002").create()
{% endhighlight %}

In PySpark, `getOrCreate()` returns the process's active session whenever one exists, even if
the builder names a different `remote` URL. A program that talks to several servers should use
`create()` for each, and keep in mind that `stop()` on a session obtained from `getOrCreate()`
ends it for every caller that holds it. The [gotchas page](spark-connect-gotchas.html#6-session-reuse-across-connection-strings)
has an example.
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.SparkSession

// Reuse a cached session with the same builder configuration, otherwise create one.
val spark = SparkSession.builder().remote("sc://localhost:15002").getOrCreate()

// Always create a new server-side session.
val other = SparkSession.builder().remote("sc://localhost:15002").create()
{% endhighlight %}

The Scala client caches sessions by their builder configuration, so two builders with
different connection strings give different sessions, and two with the same configuration share
one.
</div>
</div>

Sessions created with `create()` are isolated from each other: each has its own temporary views,
runtime configuration and artifacts, and stopping one leaves the others running.
`spark.newSession()` creates another session with the current session's connection.

`create()` needs a `sc://` connection string. The `local[*]` form, which starts a Spark Connect
server inside the client process, is accepted only by `getOrCreate()`; `create()` rejects it with
`UNSUPPORTED_LOCAL_CONNECTION_STRING`.

If the application should release its session when the interpreter exits even without a
`stop()` call, set `SPARK_CONNECT_RELEASE_SESSION_ON_EXIT=1` in the client's environment
(PySpark). Released sessions show a finish time in the Spark UI of the server.

### Configuration

Set runtime SQL configuration on the session, either on the builder or after the session exists:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
spark = SparkSession.builder.remote("sc://localhost:15002") \
    .config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.get("spark.sql.shuffle.partitions")   # '8'
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
val spark = SparkSession.builder().remote("sc://localhost:15002")
  .config("spark.sql.shuffle.partitions", "8").getOrCreate()
spark.conf.set("spark.sql.session.timeZone", "UTC")
spark.conf.get("spark.sql.shuffle.partitions")   // "8"
{% endhighlight %}
</div>
</div>

Static and core configuration set with `spark.conf.set()` is refused with
`CANNOT_MODIFY_STATIC_CONFIG` (for example `spark.sql.extensions`) or `CANNOT_MODIFY_CONFIG`
(for example `spark.jars`). The same keys passed through the builder do not raise, because the
builder applies them after the session exists; PySpark emits a `UserWarning` and the value is
not applied:

{% highlight python %}
spark = SparkSession.builder.remote("sc://localhost:15002").enableHiveSupport().getOrCreate()
# UserWarning: Failed to set spark.sql.catalogImplementation to Some(hive) due to
#   [CANNOT_MODIFY_STATIC_CONFIG] Cannot modify the value of the static Spark config:
#   "spark.sql.catalogImplementation".
spark.conf.get("spark.sql.catalogImplementation")   # 'in-memory': whatever the server started with
{% endhighlight %}

`enableHiveSupport()` sets `spark.sql.catalogImplementation`, so whether a session uses the Hive
metastore is decided by the server's startup command, not by the client; the Scala client marks
the method as deprecated for that reason.

Catalog definitions sit on the client side of the boundary but need the server's help.
`spark.sql.catalog.<name>` is runtime configuration, so a client can set it and read it back,
but the value is a class that the server loads the first time the catalog is used, and the JAR
has to be on the server. Define catalogs, extensions and their JARs in the server's startup
command (see [Starting a Spark Connect server](spark-connect-setup.html#starting-a-spark-connect-server)),
and have an application that depends on a catalog check for it when it starts, for example with
`spark.sql("SHOW NAMESPACES IN <catalog>")`, so that a server without the catalog is reported at
startup rather than inside a pipeline.

Keys that are not SQL configuration are not readable either: `spark.conf.get("spark.api.mode")`
raises `SQL_CONF_NOT_FOUND`, and `spark.conf.get("spark.jars")` returns an empty value whatever
the server was started with.

### Dependencies

Code that runs on the executors, such as the modules a Python UDF imports or the classes a Scala
UDF uses, reaches them through `SparkSession.addArtifact`. The client uploads the files to its
session, and the server distributes them to the executors the way `--py-files`, `--jars` and
`--files` do for a `spark-submit` job. Artifacts belong to the session, so add them before the
operation that needs them runs.

<div class="codetabs">

<div data-lang="python"  markdown="1">
`addArtifact` takes one or more local paths and one of three flags:

<table class="table table-striped">
<thead><tr><th>Flag</th><th>Accepts</th><th>On the executors</th></tr></thead>
<tr><td><code>pyfile=True</code></td><td><code>.py</code>, <code>.zip</code>, <code>.egg</code>, <code>.jar</code></td><td>Added to the Python path, like <code>SparkContext.addPyFile</code> and <code>--py-files</code></td></tr>
<tr><td><code>archive=True</code></td><td><code>.zip</code>, <code>.jar</code>, <code>.tar.gz</code>, <code>.tgz</code>, <code>.tar</code></td><td>Unpacked, like <code>SparkContext.addArchive</code> and <code>--archives</code></td></tr>
<tr><td><code>file=True</code></td><td>any file</td><td>Downloaded as is, like <code>SparkContext.addFile</code> and <code>--files</code></td></tr>
</table>

{% highlight python %}
from pyspark.sql import functions as F

@F.udf("double")
def km_north(lat):
    from geo import haversine_km      # geo.py lives next to the application, not on the server
    return haversine_km(0.0, 0.0, lat, 0.0)

spark.addArtifact("lib/geo.py", pyfile=True)
spark.range(1).select(km_north(F.lit(1.0))).show()
{% endhighlight %}

Without the `addArtifact` call the UDF fails with `ModuleNotFoundError: No module named 'geo'`,
raised on an executor when the UDF first runs: not on the client, and not when the UDF is
defined. `addArtifact` is available in Spark Classic as well since Spark 4.0, so an application
can adopt it before it moves to Spark Connect.
</div>

<div data-lang="scala"  markdown="1">
`addArtifact` accepts local `.jar` and `.class` files and Apache Ivy URIs. A local JAR is
uploaded as it is; an `ivy://group:artifact:version` URI is resolved, with its dependencies, by
the server using the server's Ivy settings (or by the client when the URI carries a `repos`
parameter).

{% highlight scala %}
spark.addArtifact("/path/to/my-udfs.jar")
spark.addArtifact("ivy://com.example:my-library:1.0.0")
{% endhighlight %}

UDFs, `map`, `filter` and other operations that reference classes from your own build need a
`ClassFinder` registered so that the client can find and upload the compiled class files
(`spark-shell --remote` does this for you):

{% highlight scala %}
import org.apache.spark.sql.connect.client.REPLClassDirMonitor
spark.registerClassFinder(new REPLClassDirMonitor("/path/to/build/classes"))
{% endhighlight %}
</div>
</div>

Dependencies that the server loads before any session exists cannot be shipped by a client and
belong on the server: JARs for catalogs and SQL extensions, native libraries, and the transitive
dependencies of a local JAR, which `addArtifact` does not resolve.

### UDFs and the server's Python environment

A Python UDF is pickled on the client and executed by Python on the executors. That interpreter
is the one the server was started with (`PYSPARK_PYTHON` on the server), and PySpark requires
its minor version to match the client's:

{% highlight python %}
spark.range(1).select(my_udf("id")).collect()
# [PYTHON_VERSION_MISMATCH] Python in worker has different version: 3.12 than that in
#   driver: 3.11, PySpark cannot run with different minor versions.
{% endhighlight %}

The message calls the client "driver" because that is where the UDF was pickled. Match the
client's Python to the server's, or install the client's version on the server. Operations that
run no Python on the executors, such as `toPandas()` and built-in functions, do not care.

Packages that the UDF imports have to be installed in the server's Python too, with
`addArtifact` covering pure-Python modules as shown above. Since Spark 4.2 Python UDFs are
Arrow-optimized by default (`spark.sql.execution.pythonUDF.arrow.enabled` is `true`), so the
server's Python needs PyArrow even for UDFs that never use Arrow themselves; on a server without
it every Python UDF fails on the executor with `ModuleNotFoundError: No module named 'pyarrow'`
while `toPandas()` keeps working. Install PyArrow on the server, or declare a UDF with
`useArrow=False`; setting the configuration to `false` in the session affects only UDFs defined
after the change, because the client reads it when a UDF is created.

Scala UDFs run in the server's JVM. They need their classes uploaded as described under
[Dependencies](#dependencies) and have no interpreter version to match.

### Reading and writing data

Readers and writers are the same as in Spark Classic. Paths, however, are resolved by the server:
`spark.read.text("README.md")` reads the file next to the server's working directory, not the
client's, and a path on the client machine is only readable if the server can reach it (a shared
file system, object storage, or a file shipped with `addArtifact(..., file=True)`). The same
applies to checkpoint locations and to output paths. Local data goes the other way:
`spark.createDataFrame(rows)` uploads the rows to the server, so it is the right tool for small
lookup tables and test fixtures, and `spark.read.json(df)` (also `csv` and `xml`, since Spark
4.2) parses a DataFrame with a single string column, replacing `spark.read.json(rdd)`.

{% highlight python %}
lines = spark.createDataFrame([('{"city": "Toronto", "n": 3}',)], "value string")
spark.read.json(lines).show()
# +-------+---+
# |   city|  n|
# +-------+---+
# |Toronto|  3|
# +-------+---+
{% endhighlight %}

Output from `print()` inside `foreach`, `foreachPartition` or a streaming `ForeachWriter` goes
to the executor logs, not to the client's terminal.

### Packaging and running without spark-submit

A Spark Connect application is run by its own runtime: `python app.py`, `java -jar app.jar`, a
notebook kernel, a web server. The connection string can come from the code (`remote(...)`) or
from the `SPARK_REMOTE` environment variable, which `getOrCreate()` picks up when the builder
sets no `remote`:

{% highlight bash %}
SPARK_REMOTE="sc://spark-connect.example.com:15002/;user_agent=orders-pipeline" python app.py
{% endhighlight %}

Deployment settings that used to be `spark-submit` flags move elsewhere: `--py-files`, `--jars`
and `--files` become `addArtifact` calls in the application, while `--master`, `--conf` for
static settings, `--packages` and `--driver-memory` become part of the server's startup command.
`spark-submit` can still run a Spark Connect application when a scheduler expects it:
`spark-submit --remote sc://host:15002 app.py` starts the client and nothing else.

## Migrating from Spark Classic

An application that only uses the DataFrame API migrates by changing how it creates its session.
Anything that touched `SparkContext`, RDDs or the Py4J bridge needs one of the replacements
below, and most of them exist in the DataFrame API already, so the replacement can be made while
the application still runs on Spark Classic.

<table class="table table-striped">
<thead><tr><th>Spark Classic</th><th>Spark Connect</th></tr></thead>
<tr><td><code>spark.sparkContext.setLogLevel("WARN")</code></td><td>Logging belongs to the server: configure <code>log4j2</code> where the server starts</td></tr>
<tr><td><code>spark.sparkContext.addPyFile(path)</code>, <code>--py-files</code></td><td><code>spark.addArtifact(path, pyfile=True)</code></td></tr>
<tr><td><code>spark.sparkContext.addFile(path)</code>, <code>--files</code></td><td><code>spark.addArtifact(path, file=True)</code></td></tr>
<tr><td><code>spark.sparkContext.parallelize(data)</code></td><td><code>spark.createDataFrame(data, schema)</code>, <code>spark.range(n)</code></td></tr>
<tr><td><code>df.rdd.map(f)</code>, <code>df.rdd.mapPartitions(f)</code></td><td>A UDF, <code>df.mapInArrow(f, schema)</code> or <code>df.mapInPandas(f, schema)</code></td></tr>
<tr><td><code>df.rdd.zipWithIndex()</code></td><td><code>df.zipWithIndex()</code> (Spark 4.2)</td></tr>
<tr><td><code>spark.read.json(rdd)</code></td><td><code>spark.read.json(df)</code> with a single string column (Spark 4.2)</td></tr>
<tr><td><code>df.rdd.isEmpty()</code></td><td><code>df.isEmpty()</code></td></tr>
<tr><td><code>spark.sparkContext.accumulator(0)</code></td><td><code>df.observe(Observation, ...)</code></td></tr>
<tr><td><code>spark.sparkContext.broadcast(value)</code></td><td>A broadcast join hint, <code>df.join(broadcast(small_df), ...)</code>, with the lookup data as a DataFrame</td></tr>
<tr><td><code>spark.sparkContext.uiWebUrl</code>, <code>applicationId</code></td><td>The server's Spark UI; see <a href="#debugging">Debugging</a></td></tr>
<tr><td><code>df._jdf</code>, <code>spark._jvm</code>, <code>spark._jsc</code></td><td>No replacement; the JVM is on the server</td></tr>
<tr><td><code>spark-submit app.py</code></td><td><code>python app.py</code> with <code>SPARK_REMOTE</code> set or <code>remote()</code> in the code</td></tr>
</table>

### Session creation

A builder that works in both modes reads the connection string from the environment and falls
back to a classic session, so the same file runs under `spark-submit` today and as a client
tomorrow:

{% highlight python %}
import os
from pyspark.sql import SparkSession

def build_session(app_name):
    builder = SparkSession.builder.appName(app_name)
    if "SPARK_REMOTE" in os.environ:          # a Spark Connect client
        return builder.getOrCreate()
    return builder.master("local[*]").getOrCreate()   # Spark Classic
{% endhighlight %}

`spark.api.mode=connect` is the other route: it keeps `master()` and starts a local Spark
Connect server for the application, as described in
[Switching between Spark Connect and Spark Classic](spark-connect-setup.html#switching-between-spark-connect-and-spark-classic).

### Counting rows with Observation instead of an accumulator

Accumulators are created through `SparkContext` and have no Spark Connect equivalent.
`Observation` covers the common use, counting or summing during a single action:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
# Before: an accumulator incremented from a UDF
rows = spark.sparkContext.accumulator(0)

# After: named metrics collected by the action that writes the table
from pyspark.sql import Observation
from pyspark.sql import functions as F

obs = Observation("rows")
df.observe(obs, F.count(F.lit(1)).alias("n")).write.saveAsTable("t")
obs.get["n"]     # blocks until the action completes
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
// Before: a LongAccumulator incremented inside a map
val rows = spark.sparkContext.longAccumulator("rows")

// After: named metrics collected by the action that writes the table
import org.apache.spark.sql.Observation
import org.apache.spark.sql.functions._

val obs = new Observation("rows")
df.observe(obs, count(lit(1)).as("n")).write.saveAsTable("t")
obs.get("n")     // blocks until the action completes
{% endhighlight %}
</div>
</div>

### Shipping a module instead of addPyFile

{% highlight python %}
# Before
spark.sparkContext.addPyFile("lib/geo.py")

# After
spark.addArtifact("lib/geo.py", pyfile=True)
{% endhighlight %}

The UDF that imports `geo` does not change. If the application also passed `--py-files` on the
`spark-submit` command line, those files move to `addArtifact` calls as well.

### Replacing an RDD-only operation

{% highlight python %}
# Before
if events.rdd.isEmpty():
    return
indexed = events.rdd.zipWithIndex().toDF()

# After
if events.isEmpty():
    return
indexed = events.zipWithIndex()
{% endhighlight %}

For row-level logic that `rdd.map` used to carry, a UDF keeps the per-row shape and
`mapInArrow` or `mapInPandas` keeps the per-partition shape with a batch at a time.

### Configuration that moved to the server

{% highlight python %}
# Before: static settings on the builder
spark = (SparkSession.builder
    .config("spark.sql.shuffle.partitions", "8")
    .config("spark.jars", "/opt/jars/my-catalog.jar")
    .config("spark.sql.catalog.demo", "com.example.DemoCatalog")
    .enableHiveSupport()
    .getOrCreate())

# After: the runtime setting stays with the session; the rest is in the server's start command
spark = SparkSession.builder.remote("sc://localhost:15002").getOrCreate()
spark.conf.set("spark.sql.shuffle.partitions", "8")
{% endhighlight %}

```bash
./sbin/start-connect-server.sh \
  --jars /opt/jars/my-catalog.jar \
  --conf spark.sql.catalog.demo=com.example.DemoCatalog \
  --conf spark.sql.catalogImplementation=hive
```

Behavior that migrates without an error is described on the
[Eager vs Lazy](spark-connect-gotchas.html) page: analysis errors move from the transformation to
the action, temporary views are looked up by name when a plan runs, and UDF closures are
serialized at execution time.

## Compatibility

### Client and server versions

The client library and the server do not have to run the same Spark version. The supported
direction is an existing client against a newer server: the server's RPC definitions are kept
backwards compatible, which is what lets a server be upgraded without touching the applications.
A client newer than its server can fail wherever it relies on an RPC or a setting the server
does not have. For example, a 4.3.0 client on a 4.1.3 server runs SQL and reads tables, but
`createDataFrame` fails because the client reads `spark.sql.session.localRelationSizeLimit`
from the server before uploading local data and the older server reports it in a format the
newer client does not parse (`ValueError: invalid literal for int() with base 10: '3221225472b'`).
Keep the client at or below the server's version, and check `spark.version` at startup in an
application that can be pointed at servers of different versions.

### Packages

PyPI carries four distributions that provide the `pyspark` module:

<table class="table table-striped">
<thead><tr><th>Package</th><th>Contains</th><th><code>remote()</code> accepts</th></tr></thead>
<tr><td><code>pyspark</code></td><td>Spark Classic with the Spark JARs; the Spark Connect dependencies (pandas, PyArrow, gRPC, zstandard, PyYAML) are not installed</td><td>Nothing until the <code>connect</code> extra is installed</td></tr>
<tr><td><code>pyspark[connect]</code></td><td><code>pyspark</code> plus the Spark Connect dependencies</td><td><code>sc://</code> URLs, and <code>local[*]</code>, which starts a server in-process from the bundled JARs</td></tr>
<tr><td><code>pyspark-connect</code></td><td><code>pyspark</code> plus the Spark Connect dependencies, and makes <code>spark.api.mode=connect</code> the default, so a builder with <code>master()</code> and no <code>remote()</code> starts a local Spark Connect server; <code>SPARK_CONNECT_MODE=0</code> restores the classic default</td><td><code>sc://</code> URLs and <code>local[*]</code></td></tr>
<tr><td><code>pyspark-client</code></td><td>Only the Spark Connect client: no JARs, no Py4J, no JVM needed. Requires Python 3.11 or later</td><td><code>sc://</code> URLs only; <code>local[*]</code> raises <code>INVALID_CONNECT_URL</code></td></tr>
</table>

`pyspark-client` is the choice for a client machine or container that talks to a server
elsewhere. Its own dependencies are still a few hundred megabytes, so it removes the JDK and the
JARs rather than producing a small environment. To tell which package an environment has, look
for the JARs: `pyspark-client` installs no `pyspark/jars` directory.

Scala and Java applications depend on `spark-connect-client-jvm` as shown in the
[Quick Start](#quick-start). Clients for Go, Rust and Swift are developed in separate Apache
repositories and listed in the [overview](spark-connect-overview.html#what-is-supported).

## Debugging

### Error classes

Spark Connect errors carry an error class that says which side of the boundary the problem is
on:

<table class="table table-striped">
<thead><tr><th>Error class</th><th>Meaning</th></tr></thead>
<tr><td><code>JVM_ATTRIBUTE_NOT_SUPPORTED</code></td><td>Code reached for <code>sparkContext</code>, <code>rdd</code>, <code>_jvm</code>, <code>_jdf</code> or another attribute that exists only in the driver JVM (PySpark); the Scala client throws <code>UNSUPPORTED_CONNECT_FEATURE.*</code> for the same accesses</td></tr>
<tr><td><code>INVALID_CONNECT_URL</code></td><td>The connection string is malformed: a scheme other than <code>sc://</code>, a non-empty path, or a <code>/</code> inside a parameter value</td></tr>
<tr><td><code>UNSUPPORTED_LOCAL_CONNECTION_STRING</code></td><td><code>create()</code> was called with <code>local[*]</code>; use <code>getOrCreate()</code></td></tr>
<tr><td><code>CANNOT_MODIFY_STATIC_CONFIG</code>, <code>CANNOT_MODIFY_CONFIG</code></td><td>The setting belongs to the server's startup command</td></tr>
<tr><td><code>SQL_CONF_NOT_FOUND</code></td><td>The key is not SQL configuration and cannot be read from a client</td></tr>
<tr><td><code>PYTHON_VERSION_MISMATCH</code></td><td>The client's and the server's Python minor versions differ</td></tr>
<tr><td><code>CANNOT_LOAD_CATALOG</code></td><td>A catalog was configured whose class is not on the server</td></tr>
<tr><td><code>NO_ACTIVE_SESSION</code></td><td>The session was stopped and then used again</td></tr>
</table>

### The Spark UI of the server

The server is a Spark application, so its UI (port 4040 by default) shows the jobs, stages and
SQL queries that client sessions run. The **Connect** tab (`/connect/`) lists every session with
its user, start time, finish time and request count, and every request with its operation and
status, which is the place to confirm which server an application reached and whether it released
its session. Driver logs are the server's logs, under `logs/` in the Spark folder or
`SPARK_LOG_DIR`.

### Seeing the plan

`df.explain()` prints the plan the server produced for the DataFrame, and in doing so forces
analysis, so it surfaces a deferred analysis error at the point where a DataFrame is built;
`df.schema` does the same without printing. `spark.interruptAll()` cancels every running
operation of the session, and `spark.interruptTag(tag)` cancels the operations tagged with
`spark.addTag(tag)`, which is how an application stops a query it no longer needs without
stopping the session.
