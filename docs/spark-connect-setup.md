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

Spark Connect ships with every Spark release since 3.4 and supports PySpark and Scala
applications. For what Spark Connect is and how it differs from classic Spark, see the
[Spark Connect Overview](spark-connect-overview.html); for writing applications against a server,
see the [Spark Connect Programming Guide](spark-connect-programming-guide.html).

* This will become a table of contents (this text will be scraped).
{:toc}

# Starting a Spark Connect server

First, download Spark from the
[Download Apache Spark](https://spark.apache.org/downloads.html) page. Choose the
latest release in  the release drop down at the top of the page. Then choose your package type, typically
“Pre-built for Apache Hadoop 3.5 and later”, and click the link to download.

Now extract the Spark package you just downloaded on your computer, for example:

```bash
tar -xvf spark-{{site.SPARK_VERSION_SHORT}}-bin-hadoop3.tgz
```

In a terminal window, go to the `spark` folder in the location where you extracted
Spark before and run the `start-connect-server.sh` script to start Spark server with
Spark Connect. If you already have Spark installed and `SPARK_HOME` defined, you can use that too.

```bash
./sbin/start-connect-server.sh

# alternately
"$SPARK_HOME/sbin/start-connect-server.sh"
```

Alternatively, `./bin/spark-connect-shell` starts an interactive Scala shell with the Connect server hosted inside the shell process itself.

The script accepts the same options as `spark-submit`, so `--master`, `--jars`, `--packages`
and `--conf` work as they do for any other Spark application. Everything that the server
reads at startup, such as `spark.sql.extensions`, `spark.jars`, `spark.jars.packages` and the
JARs that catalogs load, is set here. Clients can only change runtime configuration of their own
session; that includes defining a catalog (`spark.sql.catalog.<name>`) whose JAR is already on the
server.

By default the server listens on port 15002 on all interfaces and runs as a background daemon
(`./sbin/stop-connect-server.sh` stops it). Pass `--wait` as the first argument to keep it in
the foreground instead, for example as a container entrypoint or under a process supervisor. With
`--wait`, the script sets `SPARK_NO_DAEMONIZE=1`, writes no PID file and sends the logs to standard
output. `spark.connect.grpc.binding.address` restricts the listening interface and
`spark.connect.grpc.binding.port` changes the port:

```bash
./sbin/start-connect-server.sh --wait \
  --conf spark.connect.grpc.binding.address=127.0.0.1 \
  --conf spark.connect.grpc.binding.port=15002
```

Spark does not retry another port when the configured one is taken
(`spark.connect.grpc.port.maxRetries` defaults to 0), so a port conflict fails the server with a
`java.net.BindException` instead of silently moving it. A daemonized server logs to `logs/` under
the Spark folder (or `SPARK_LOG_DIR`), and the Spark UI of the server, at port 4040 by default, has a
**Connect** tab listing sessions and requests.

# Interactive analysis with Spark Connect
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

{% highlight bash %}
export SPARK_REMOTE="sc://localhost"
{% endhighlight %}

And start the Spark shell as usual:

{% highlight bash %}
./bin/pyspark
{% endhighlight %}

The PySpark shell is now connected to Spark using Spark Connect as indicated in the welcome message:

{% highlight python %}
Client connected to the Spark Connect server at localhost
{% endhighlight %}

### Specify Spark Connect when creating Spark session

You can also specify that you want to use Spark Connect explicitly when you
create a Spark session.

For example, you can launch the PySpark shell with Spark Connect as
illustrated here.

To launch the PySpark shell with Spark Connect, simply include the `remote`
parameter and specify the location of your Spark server. We are using `localhost`
in this example to connect to the local Spark server we started previously:

{% highlight bash %}
./bin/pyspark --remote "sc://localhost"
{% endhighlight %}

And you will notice that the PySpark shell welcome message tells you that
you have connected to Spark using Spark Connect:

{% highlight python %}
Client connected to the Spark Connect server at localhost
{% endhighlight %}

You can also check the Spark session type. If it includes `.connect.` you
are using Spark Connect as shown in this example:

{% highlight python %}
SparkSession available as 'spark'.
>>> type(spark)
<class 'pyspark.sql.connect.session.SparkSession'>
{% endhighlight %}

Now you can run PySpark code in the shell to see Spark Connect in action:

{% highlight python %}
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
{% endhighlight %}

</div>

<div data-lang="scala" markdown="1">
For the Scala shell, we use an Ammonite-based REPL. Otherwise, very similar with PySpark shell.

{% highlight bash %}
./bin/spark-shell --remote "sc://localhost"
{% endhighlight %}

A greeting message will appear when the REPL successfully initializes:
{% highlight bash %}
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version {{site.SPARK_VERSION}}
      /_/

Type in expressions to have them evaluated.
Spark session available as 'spark'.
{% endhighlight %}

By default, the REPL will attempt to connect to a local Spark Server.
Run the following Scala code in the shell to see Spark Connect in action:

{% highlight scala %}
@ spark.range(10).count
res0: Long = 10L
{% endhighlight %}

</div>
</div>

# Connection strings

By default, the shells and the `remote()` builder attempt to connect to a local Spark Server on
port 15002. The connection, however, may be configured in several ways as described in this
configuration [reference](https://github.com/apache/spark/blob/master/sql/connect/docs/client-connection-string.md).
A connection string has the form `sc://host:port/;param1=value;param2=value`: the scheme is
always `sc://`, the path component must stay empty because gRPC uses the HTTP/2 path for its own
method names, and parameters such as `token`, `use_ssl`, `user_id` and `user_agent` follow a `/;`
separator. Setting `token` turns on TLS for any host except `localhost`; to use TLS with
`localhost`, set `use_ssl=true` as well.

In the Python client, a parameter value cannot contain a literal `/`, because its parser reads
everything from the first `/` as the path:

{% highlight python %}
SparkSession.builder.remote("sc://localhost:15002/;user_agent=myapp/1").getOrCreate()
# [INVALID_CONNECT_URL] Invalid URL for Spark Connect:
#   The path component '/;user_agent=myapp/1' must be empty.
{% endhighlight %}

Write the `/` as `%2F` instead: `user_agent=myapp%2F1` is accepted and read as `myapp/1`.

## Set SPARK_REMOTE environment variable

The SPARK_REMOTE environment variable can be set on the client machine to customize the client-server
connection that is initialized at REPL startup.

{% highlight bash %}
export SPARK_REMOTE="sc://myhost.com:443/;token=ABCDEFG"
./bin/spark-shell
{% endhighlight %}

## Configure programmatically with a connection string

The connection may also be programmatically created using _SparkSession#builder_ as in this example:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost:443/;token=ABCDEFG").getOrCreate()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
@ import org.apache.spark.sql.SparkSession
@ val spark = SparkSession.builder.remote("sc://localhost:443/;token=ABCDEFG").getOrCreate()
{% endhighlight %}
</div>
</div>

# Faster local iteration with a persistent Connect server
When you develop or test locally with

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("local[*]").getOrCreate()
```

PySpark boots a fresh in-process Spark Connect server that lives only as long as that Python
process. Every `python script.py` invocation (or every new test worker process) therefore re-pays
the one-time startup cost -- JVM warmup, `SparkContext` construction, and Connect server boot --
which can take a few seconds and makes a quick edit/run loop feel slow.

The in-process server binds port 15002 by default and the client dials `sc://localhost`,
so if another Spark Connect server is already listening on that port (a container, or a server
left running from an earlier session) the start fails with
`java.net.BindException: Failed to bind to address 0.0.0.0/0.0.0.0:15002`. Moving the port with
`spark.connect.grpc.binding.port` does not help here, because the client would still dial
15002 and reach the other server; stop the other server or connect to it by URL instead.

To amortize that cost across runs, start one persistent local Spark Connect server and point
every run at it:

```bash
# Start once; it stays up across runs. (--master is optional; it defaults to local[*].)
$SPARK_HOME/sbin/start-connect-server.sh --master "local[*]"

# Every run reconnects instead of booting a new server.
python -c 'from pyspark.sql import SparkSession; SparkSession.builder.remote("sc://localhost:15002").getOrCreate()'

# Stop it when you are done.
$SPARK_HOME/sbin/stop-connect-server.sh
```

Alternatively, on POSIX systems PySpark can manage this persistent server for you. With
`SPARK_LOCAL_CONNECT_REUSE=1` set (or `spark.local.connect.reuse=true` on the builder),
`SparkSession.builder.remote("local[*]").getOrCreate()` starts a persistent server through
`sbin/start-connect-server.sh` on the first run and reconnects to it on later runs, so scripts
keep the plain `local[*]` URL:

```bash
export SPARK_LOCAL_CONNECT_REUSE=1

# The first run starts the server; later runs reconnect to it.
python -c 'from pyspark.sql import SparkSession; SparkSession.builder.remote("local[*]").getOrCreate()'

# Stop the managed server when you are done.
python -m pyspark.sql.connect.local_server --stop
```

The managed server is an ordinary `spark-daemon.sh` daemon, but it runs with a per-user pid
directory and ident string so it cannot collide with a server you started by hand -- which also
means a plain `sbin/stop-connect-server.sh` does not find it. The `--stop` command signals the
recorded server and cleans up the discovery file; killing the server's pid directly also works,
and the next run notices the dead server and starts a fresh one.

This managed-server workflow is experimental. The `--stop` command and the discovery file
location and format may change in a future release, for example if local server management is
folded into a unified `spark connect` CLI.

The connection details (host, port, auth token, pid, Spark version) are recorded in a discovery
file in a private per-user directory; set `SPARK_LOCAL_CONNECT_DISCOVERY` to override its
location. A run reconnects only to a server whose Spark version matches. After upgrading Spark,
the server from the previous version cannot be reused and the next run fails with an error asking
you to stop it; run the `--stop` command above and rerun to start a fresh server.

Each run connects as its own Connect session, so session-local state -- temp views, runtime SQL
configurations, and session artifacts -- is fresh on every run and never leaks between runs. State
backed by the shared `SparkContext` (the persistent catalog/warehouse, global temp views, and
cached datasets) *is* shared across runs, so namespace per-run databases or clear that state
yourself if your runs must be fully isolated.

# Spark Connect in standalone applications
<div class="codetabs">


<div data-lang="python"  markdown="1">

First, install PySpark with `pip install pyspark-client=={{site.SPARK_VERSION_SHORT}}` or if building a packaged PySpark application/library,
add it your setup.py file as:
{% highlight python %}
install_requires=[
'pyspark-client=={{site.SPARK_VERSION_SHORT}}'
]
{% endhighlight %}

When writing your own code, include the `remote` function with a reference to
your Spark server when you create a Spark session, as in this example:

{% highlight python %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost").getOrCreate()
{% endhighlight %}


For illustration purposes, we’ll create a simple Spark Connect application, SimpleApp.py:
{% highlight python %}
"""SimpleApp.py"""
from pyspark.sql import SparkSession

logFile = "YOUR_SPARK_HOME/README.md"  # a path the server can read
spark = SparkSession.builder.remote("sc://localhost").appName("SimpleApp").getOrCreate()
logData = spark.read.text(logFile).cache()

numAs = logData.filter(logData.value.contains('a')).count()
numBs = logData.filter(logData.value.contains('b')).count()

print("Lines with a: %i, lines with b: %i" % (numAs, numBs))

spark.stop()
{% endhighlight %}

This program just counts the number of lines containing ‘a’ and the number containing ‘b’ in a text file.
Note that you’ll need to replace YOUR_SPARK_HOME with the location where Spark is installed.

We can run this application with the regular Python interpreter as follows:
{% highlight python %}
# Use the Python interpreter to run your application
$ python SimpleApp.py
...
Lines with a: 72, lines with b: 39
{% endhighlight %}
</div>


<div data-lang="scala"  markdown="1">
To use Spark Connect as part of a Scala application/project, we first need to include the right dependencies.
Using the `sbt` build system as an example, we add the following dependencies to the `build.sbt` file:
{% highlight sbt %}
libraryDependencies += "org.apache.spark" %% "spark-connect-client-jvm" % "{{site.SPARK_VERSION_SHORT}}"
{% endhighlight %}

Depend on `spark-connect-client-jvm` only, and on JDK 17 and later start the JVM with
`--add-opens=java.base/java.nio=ALL-UNNAMED`;
[Scala dependency](spark-connect-programming-guide.html#scala-dependency) explains both.

When writing your own code, include the `remote` function with a reference to
your Spark server when you create a Spark session, as in this example:

{% highlight scala %}
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().remote("sc://localhost").getOrCreate()
{% endhighlight %}


**Note**: Operations that reference User Defined Code such as UDFs, filter, map, etc require a
[ClassFinder](https://github.com/apache/spark/blob/master/sql/connect/common/src/main/scala/org/apache/spark/sql/connect/client/ClassFinder.scala)
to be registered to pickup and upload any required classfiles. Also, any JAR dependencies must be uploaded to the server using `SparkSession#AddArtifact`.

Example:
{% highlight scala %}
import org.apache.spark.sql.connect.client.REPLClassDirMonitor
// Register a ClassFinder to monitor and upload the classfiles from the build output.
val classFinder = new REPLClassDirMonitor(<ABSOLUTE_PATH_TO_BUILD_OUTPUT_DIR>)
spark.registerClassFinder(classFinder)

// Upload JAR dependencies
spark.addArtifact(<ABSOLUTE_PATH_JAR_DEP>)
{% endhighlight %}
Here, `ABSOLUTE_PATH_TO_BUILD_OUTPUT_DIR` is the output directory where the build system writes classfiles into
and `ABSOLUTE_PATH_JAR_DEP` is the location of the JAR on the local file system.

The `REPLClassDirMonitor` is a provided implementation of `ClassFinder` that monitors a specific directory but
one may implement their own class extending `ClassFinder` for customized search and monitoring.

</div>
</div>

For extending Spark Connect with custom server-side functionality, see
[Extending Spark with Spark Server Libraries](spark-connect-server-libs.html).

# Switching between Spark Connect and Spark Classic

Spark provides the API mode, `spark.api.mode` configuration, enabling Spark Classic applications
to switch to Spark Connect. Depending on the value of `spark.api.mode`, the application
can run in either Spark Classic or Spark Connect mode. Here is an example:

{% highlight python %}
from pyspark.sql import SparkSession

SparkSession.builder.config("spark.api.mode", "connect").master("...").getOrCreate()
{% endhighlight %}

You can also apply this configuration to both Scala and PySpark applications when submitting yours:

{% highlight bash %}
spark-submit --master "..." --conf spark.api.mode=connect
{% endhighlight %}

Additionally, Spark Connect offers convenient options for local testing. By setting `spark.remote`
to `local[...]` or `local-cluster[...]`, you can start a local Spark Connect server and access a Spark
Connect session.

This is similar to using `--conf spark.api.mode=connect` with `--master ...`. However, a `local*`
value of `spark.remote` or `--remote` starts a local server and an `sc://` value connects to an
existing server, while `--conf spark.api.mode=connect` with `--master ...` supports additional cluster URLs, such as spark://, for broader compatibility with
Spark Classic.

# Client application authentication

Spark Connect has no user-facing authentication of its own. Its gRPC HTTP/2
interface allows for the use of authenticating proxies, which makes it possible
to secure Spark Connect without having to implement authentication logic in
Spark directly.

# Routing through a shared ingress or reverse proxy

When several services share a single hostname behind a Kubernetes Ingress (or another
reverse proxy), it is tempting to give each service a URL path prefix (for example
`sc://host/sparkConnect`) and route on that path. This does not work for gRPC: the
gRPC method name *is* the HTTP/2 `:path` (`/spark.connect.SparkConnectService/ExecutePlan`),
so a connection string cannot carry a separate routing path. Prepending a prefix to the
`:path` produces an unknown method and the server responds with `UNIMPLEMENTED`, unless the
proxy is configured to strip the prefix back off before forwarding, a two-sided contract
that is not part of gRPC's design.

The routing dimension that *is* free is the HTTP/2 `:authority` (the virtual host). Set it
to a routing tag with the `grpc.default_authority` channel option, and route on it at the
proxy (for example, an Ingress `host:` rule). The client still dials the shared hostname
(`default_authority` only overrides the `:authority` header used for routing, not the address
it connects to), and the gRPC method `:path` is never touched, so no path rewrite is needed.
This is the approach the gRPC maintainers recommend for this scenario
([grpc/grpc#14900](https://github.com/grpc/grpc/issues/14900)).

The example below uses the Python client, which exposes gRPC channel options directly:

{% highlight python %}
from pyspark.sql.connect.session import SparkSession
from pyspark.sql.connect.client import DefaultChannelBuilder

cb = DefaultChannelBuilder("sc://myhost.com:443")
cb.setChannelOption("grpc.default_authority", "sparkconnect")  # routing tag
spark = SparkSession.builder.channelBuilder(cb).getOrCreate()
{% endhighlight %}

A corresponding Kubernetes Ingress routes on that tag and keeps the service at path `/`
(no subpath, no rewrite). Note the routing tag must be a lowercase
[RFC 1123](https://datatracker.ietf.org/doc/html/rfc1123) name, since a Kubernetes Ingress
`host:` requires one:

{% highlight yaml %}
spec:
  rules:
  - host: sparkconnect            # matches grpc.default_authority
    http:
      paths:
      - path: /
        pathType: Prefix
        backend:
          service:
            name: spark-connect-server
            port: { number: 15002 }
{% endhighlight %}

**Over TLS**, gRPC uses the `:authority` as the certificate-verification name by default,
so overriding it with a routing tag would fail verification (the tag is not in the server
certificate's SAN). Keep the two names separate: set `grpc.ssl_target_name_override` to the
real server hostname (used for certificate verification and SNI) and `grpc.default_authority`
to the routing tag.

{% highlight python %}
cb = DefaultChannelBuilder("sc://myhost.com:443/;use_ssl=true")
cb.setChannelOption("grpc.ssl_target_name_override", "myhost.com")  # certificate verification / SNI
cb.setChannelOption("grpc.default_authority", "sparkconnect")       # routing tag
spark = SparkSession.builder.channelBuilder(cb).getOrCreate()
{% endhighlight %}

`use_ssl=true` verifies the server certificate against the system's trusted CA store. If your
gateway's certificate is issued by a CA the client does not trust by default (a self-signed or
internal CA), make that CA trusted on the client side (for example, via
`GRPC_DEFAULT_SSL_ROOTS_FILE_PATH`) rather than through the connection string. Configuring the
proxy's own TLS (which certificate it presents for which hostname) is part of your ingress
setup and is out of scope here.

Two notes on `grpc.ssl_target_name_override`:
gRPC documents it as testing-oriented because its typical misuse is to *mask* a certificate
name mismatch (verifying against a name the server does not actually present, which defeats
hostname verification). Here it is set to the real, verified hostname, so the certificate is
still checked correctly; it only prevents the routing tag from being used as the verification
name. If you prefer to avoid the option entirely, issue the server certificate with the routing
tag included in its SAN; then `grpc.default_authority` alone is sufficient and no override is
needed.
