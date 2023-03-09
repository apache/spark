---
layout: global
title: Spark Connect Overview
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
**Building client-side Spark applications**

In Apache Spark 3.4, Spark Connect introduced a decoupled client-server architecture that allows remote connectivity to Spark clusters using the DataFrame API and unresolved logical plans as the protocol. The separation between client and server allows Spark and its open ecosystem to be leveraged from everywhere. It can be embedded in modern data applications, in IDEs, Notebooks and programming languages.

To get started, see [Quickstart: Spark Connect](api/python/getting_started/quickstart_connect.html).

<p style="text-align: center;">
  <img src="img/spark-connect-api.png" title="Spark Connect API" alt="Spark Connect API Diagram" />
</p>

# How Spark Connect works

The Spark Connect client library is designed to simplify Spark application development. It is a thin API that can be embedded everywhere: in application servers, IDEs, notebooks, and programming languages. The Spark Connect API builds on Spark's DataFrame API using unresolved logical plans as a language-agnostic protocol between the client and the Spark driver.

The Spark Connect client translates DataFrame operations into unresolved logical query plans which are encoded using protocol buffers. These are sent to the server using the gRPC framework.

The Spark Connect endpoint embedded on the Spark Server, receives and translates unresolved logical plans into Spark's logical plan operators. This is similar to parsing a SQL query, where attributes and relations are parsed and an initial parse plan is built. From there, the standard Spark execution process kicks in, ensuring that Spark Connect leverages all of Spark's optimizations and enhancements. Results are streamed back to the client via gRPC as Apache Arrow-encoded row batches.

<p style="text-align: center;">
  <img src="img/spark-connect-communication.png" title="Spark Connect communication" alt="Spark Connect communication" />
</p>

# Operational benefits of Spark Connect

With this new architecture, Spark Connect mitigates several operational issues:

**Stability**: Applications that use too much memory will now only impact their own environment as they can run in their own processes. Users can define their own dependencies on the client and don't need to worry about potential conflicts with the Spark driver.

**Upgradability**: The Spark driver can now seamlessly be upgraded independently of applications, e.g. to benefit from performance improvements and security fixes. This means applications can be forward-compatible, as long as the server-side RPC definitions are designed to be backwards compatible.

**Debuggability and Observability**: Spark Connect enables interactive debugging during development directly from your favorite IDE. Similarly, applications can be monitored using the application's framework native metrics and logging libraries.

# How to use Spark Connect

Starting with Spark 3.4, Spark Connect is available and supports PySpark applications. When creating a Spark session, you can specify that you want to use Spark Connect and there are a few ways to do that as outlined below.

If you do not use one of the mechanisms outlined below, your Spark session will work just like before, without leveraging Spark Connect, and your application code will run on the Spark driver node.

## Set SPARK_REMOTE environment variable

If you set the SPARK_REMOTE environment variable on the client machine where your Spark client application is running and create a new Spark Session as illustrated below, the session will be a Spark Connect session. With this approach, there is no code change needed to start using Spark Connect.

Set the SPARK_REMOTE environment variable to point to your Spark server:

{% highlight bash %}
export SPARK_REMOTE="sc://localhost/"
{% endhighlight %}

And now start the Spark shell, for example:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight bash %}
./bin/pyspark
{% endhighlight %}

The shell is now connected to Spark using Spark Connect as indicated in the welcome message.
</div>

<div data-lang="scala"  markdown="1">
{% highlight bash %}
./bin/spark-shell
{% endhighlight %}
</div>

</div>

And if you write your own program, create a Spark session as shown in this example:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().getOrCreate()
{% endhighlight %}
</div>

</div>

Which will create a Spark Connect session by reading the SPARK_REMOTE environment variable we set above.

## Specify Spark Connect when creating Spark session

You can also specify that you want to use Spark Connect when you create a Spark session explicitly.

For example, when launching the PySpark or Scala CLI, simply include the remote parameter as illustrated here:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight bash %}
./bin/pyspark --remote "sc://localhost"
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight bash %}
./bin/spark-shell --remote "sc://localhost"
{% endhighlight %}
</div>

</div>

And again you will notice that the PySpark welcome message tells you that you are connected to Spark using Spark Connect.

Or, in your code, include the remote function with a reference to your Spark server when you create a Spark session, as in this example:

<div class="codetabs">

<div data-lang="python"  markdown="1">
{% highlight python %}
from pyspark.sql import SparkSession
spark = SparkSession.builder.remote("sc://localhost/").getOrCreate()
{% endhighlight %}
</div>

<div data-lang="scala"  markdown="1">
{% highlight scala %}
import org.apache.spark.sql.SparkSession
val spark = SparkSession.builder().remote("sc://localhost/").getOrCreate()
{% endhighlight %}
</div>

</div>

# Client application authentication

While Spark Connect does not have built-in authentication, it is designed to work seamlessly with your existing authentication infrastructure. Its gRPC HTTP/2 interface allows for the use of authenticating proxies, which makes it possible to secure Spark Connect without having to implement authentication logic in Spark directly.

# What is supported in Spark 3.4

**PySpark**: In Spark 3.4, Spark Connect supports most PySpark APIs, including [DataFrame](api/python/reference/pyspark.sql/dataframe.html), [Functions](api/python/reference/pyspark.sql/functions.html), and [Column](api/python/reference/pyspark.sql/column.html). However, some APIs such as [SparkContext](api/python/reference/api/pyspark.SparkContext.html) and [RDD](api/python/reference/api/pyspark.RDD.html) are not supported yet. You can check which APIs are currently
supported in the [API reference](api/python/reference/index.html) documentation. Supported APIs are labeled "Supports Spark Connect" so you can check whether the APIs you are using are available before migrating existing code to Spark Connect.

**Scala**: In Spark 3.4, Spark Connect supports most Scala APIs, ...

Support for Spark Connect clients in other languages is planned for the future.