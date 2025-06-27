---
layout: global
title: Setting up Spark Connect
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

Spark Connect supports PySpark and Scala applications. We will walk through how to run an
Apache Spark server with Spark Connect and connect to it from a client application using the
Spark Connect client library.

* This will become a table of contents (this text will be scraped).
{:toc}

## Download and start Spark server with Spark Connect

First, download Spark from the
[Download Apache Spark](https://spark.apache.org/downloads.html) page. Choose the
latest release in  the release drop down at the top of the page. Then choose your package type, typically
“Pre-built for Apache Hadoop 3.3 and later”, and click the link to download.

Now extract the Spark package you just downloaded on your computer, for example:

```bash
tar -xvf spark-{{site.SPARK_VERSION_SHORT}}-bin-hadoop3.tgz
```

In a terminal window, go to the `spark` folder in the location where you extracted
Spark before and run the `start-connect-server.sh` script to start Spark server with
Spark Connect, like in this example:

```bash
./sbin/start-connect-server.sh
```

Make sure to use the same version  of the package as the Spark version you
downloaded previously. In this example, Spark {{site.SPARK_VERSION_SHORT}} with Scala 2.13.

Now Spark server is running and ready to accept Spark Connect sessions from client
applications. In the next section we will walk through how to use Spark Connect
when writing client applications.

## Use Spark Connect for interactive analysis
<div class="codetabs">

<div data-lang="python" markdown="1">
When creating a Spark session, you can specify that you want to use Spark Connect
and there are a few ways to do that outlined as follows.

If you do not use one of the mechanisms outlined here, your Spark session will
work just like before, without leveraging Spark Connect.

### Set SPARK_REMOTE environment variable

If you set the `SPARK_REMOTE` environment variable on the client machine where your
Spark client application is running and create a new Spark Session as in the following
example, the session will be a Spark Connect session. With this approach, there is no
code change needed to start using Spark Connect.

In a terminal window, set the `SPARK_REMOTE` environment variable to point to the
local Spark server you started previously on your computer:

```bash
export SPARK_REMOTE="sc://localhost"
```

And start the Spark shell as usual:

```bash
./bin/pyspark
```

The PySpark shell is now connected to Spark using Spark Connect as indicated in the welcome message:

```python
Client connected to the Spark Connect server at localhost
```

### Specify Spark Connect when creating Spark session

You can also specify that you want to use Spark Connect explicitly when you
create a Spark session.

For example, you can launch the PySpark shell with Spark Connect as
illustrated here.

To launch the PySpark shell with Spark Connect, simply include the `remote`
parameter and specify the location of your Spark server. We are using `localhost`
in this example to connect to the local Spark server we started previously:

```bash
./bin/pyspark --remote "sc://localhost"
```

And you will notice that the PySpark shell welcome message tells you that
you have connected to Spark using Spark Connect:

```python
Client connected to the Spark Connect server at localhost
```

You can also check the Spark session type. If it includes `.connect.` you
are using Spark Connect as shown in this example:

```python
SparkSession available as 'spark'.
>>> type(spark)
<class 'pyspark.sql.connect.session.SparkSession'>
```

Now you can run PySpark code in the shell to see Spark Connect in action:

```python
>>> columns = ["id", "name"]
>>> data = [(1,"Sarah"), (2,"Maria")]
>>> df = spark.createDataFrame(data).toDF(*columns)
>>> df.show()
+---+-----+
| id| name|
+---+-----+
|  1|Sarah|
|  2|Maria|
+---+-----+
```

</div>

<div data-lang="scala"  markdown="1">
For the Scala shell, we use an Ammonite-based REPL. Otherwise, very similar with PySpark shell.

```bash
./bin/spark-shell --remote "sc://localhost"
```

A greeting message will appear when the REPL successfully initializes:
```bash
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 4.1.0-SNAPSHOT
      /_/

Type in expressions to have them evaluated.
Spark session available as 'spark'.
```

By default, the REPL will attempt to connect to a local Spark Server.
Run the following Scala code in the shell to see Spark Connect in action:

```scala
@ spark.range(10).count
res0: Long = 10L
```

### Configure client-server connection

By default, the REPL will attempt to connect to a local Spark Server on port 15002.
The connection, however, may be configured in several ways as described in this configuration
[reference](https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md).

#### Set SPARK_REMOTE environment variable

The SPARK_REMOTE environment variable can be set on the client machine to customize the client-server
connection that is initialized at REPL startup.

```bash
export SPARK_REMOTE="sc://myhost.com:443/;token=ABCDEFG"
./bin/spark-shell
```

or

```bash
SPARK_REMOTE="sc://myhost.com:443/;token=ABCDEFG" spark-connect-repl
```

#### Configure programmatically with a connection string

The connection may also be programmatically created using _SparkSession#builder_ as in this example:

```scala
@ import org.apache.spark.sql.SparkSession
@ val spark = SparkSession.builder.remote("sc://localhost:443/;token=ABCDEFG").getOrCreate()
```

</div>
</div>

## Use Spark Connect in standalone applications

<div class="codetabs">

<div data-lang="python"  markdown="1">

First, install PySpark with `pip install pyspark[connect]=={{site.SPARK_VERSION_SHORT}}` or if building a packaged PySpark application/library,
add it your setup.py file as:
```python
install_requires=[
    'pyspark[connect]=={{site.SPARK_VERSION_SHORT}}'
]
```

When writing your own code, include the `remote` function with a reference to
your Spark server when you create a Spark session, as in this example:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
```


For illustration purposes, we’ll create a simple Spark Connect application, SimpleApp.py:
```python
"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "YOUR_SPARK_HOME/README.md"  # Should be some file on your system
spark = SparkSession.builder.remote("sc://localhost").appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
```

This program just counts the number of lines containing ‘a’ and the number containing ‘b’ in a text file.
Note that you’ll need to replace YOUR_SPARK_HOME with the location where Spark is installed.

We can run this application with the regular Python interpreter as follows:
```python
# Use the Python interpreter to run your application
$ python SimpleApp.py
...
Lines with a: 72, lines with b: 39
```
</div>


<div data-lang="scala"  markdown="1">
To use Spark Connect as part of a Scala application/project, we first need to include the right dependencies.
Using the `sbt` build system as an example, we add the following dependencies to the `build.sbt` file:
```sbt
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "{{site.SPARK_VERSION_SHORT}}"
```

When writing your own code, include the `remote` function with a reference to
your Spark server when you create a Spark session, as in this example:

```scala
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().remote("sc://localhost").getOrCreate()
```


**Note**: Operations that reference User Defined Code such as UDFs, filter, map, etc require a
[ClassFinder](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/client/ClassFinder.scala)
to be registered to pickup and upload any required classfiles. Also, any JAR dependencies must be uploaded to the server using `SparkSession#AddArtifact`.

Example:
```scala
import org.apache.spark.sql.connect.client.REPLClassDirMonitor
// Register a ClassFinder to monitor and upload the classfiles from the build output.
val classFinder = new REPLClassDirMonitor(<ABSOLUTE_PATH_TO_BUILD_OUTPUT_DIR>)
spark.registerClassFinder(classFinder)

// Upload JAR dependencies
spark.addArtifact(<ABSOLUTE_PATH_JAR_DEP>)
```
Here, `ABSOLUTE_PATH_TO_BUILD_OUTPUT_DIR` is the output directory where the build system writes classfiles into
and `ABSOLUTE_PATH_JAR_DEP` is the location of the JAR on the local file system.

The `REPLClassDirMonitor` is a provided implementation of `ClassFinder` that monitors a specific directory but
one may implement their own class extending `ClassFinder` for customized search and monitoring.

</div>
</div>

# Switching between Spark Connect and Spark Classic

Spark provides the `spark.api.mode` configuration, enabling Spark Classic applications
to seamlessly switch to Spark Connect. Depending on the value of `spark.api.mode`, the application
can run in either Spark Classic or Spark Connect mode.

Here is an example:

```python
from pyspark.sql import SparkSession

SparkSession.builder.config("spark.api.mode", "connect").master("...").getOrCreate()
```

You can also apply this configuration when submitting applications (Scala or Python) via `spark-submit`:

```bash
spark-submit --master "..." --conf spark.api.mode=connect
```

Additionally, Spark Connect offers convenient options for local testing. By setting `spark.remote`
to `local[...]` or `local-cluster[...]`, you can start a local Spark Connect server and access a Spark
Connect session.

This is similar to using `--conf spark.api.mode=connect` with `--master ...`. However, note that
`spark.remote` and `--remote` are limited to `local*` values, while `--conf spark.api.mode=connect`
with `--master ...` supports additional cluster URLs, such as spark://, for broader compatibility with
Spark Classic.
