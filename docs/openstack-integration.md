---
layout: global
title: OpenStack Integration
---

* This will become a table of contents (this text will be scraped).
{:toc}


# Accessing OpenStack Swift from Spark

Spark's file interface allows it to process data in OpenStack Swift using the same URI 
formats that are supported for Hadoop. You can specify a path in Swift as input through a 
URI of the form <code>swift://<container.PROVIDER/path</code>. You will also need to set your 
Swift security credentials, through <code>core-sites.xml</code> or via
<code>SparkContext.hadoopConfiguration</code>. 
Openstack Swift driver was merged in Hadoop version 2.3.0
([Swift driver](https://issues.apache.org/jira/browse/HADOOP-8545)).
Users that wish to use previous Hadoop versions will need to configure Swift driver manually.
Current Swift driver requires Swift to use Keystone authentication method. There are recent efforts
to support temp auth [Hadoop-10420](https://issues.apache.org/jira/browse/HADOOP-10420).

# Configuring Swift 
Proxy server of Swift should include <code>list_endpoints</code> middleware. More information
available
[here](https://github.com/openstack/swift/blob/master/swift/common/middleware/list_endpoints.py)

# Dependencies

Spark should be compiled with <code>hadoop-openstack-2.3.0.jar</code> that is distributted with
Hadoop 2.3.0. For the Maven builds, the <code>dependencyManagement</code> section of Spark's main
<code>pom.xml</code> should include:
{% highlight xml %}
<dependencyManagement>
  ...
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-openstack</artifactId>
    <version>2.3.0</version>
  </dependency>
  ...
</dependencyManagement>
{% endhighlight %}

In addition, both <code>core</code> and <code>yarn</code> projects should add
<code>hadoop-openstack</code> to the <code>dependencies</code> section of their
<code>pom.xml</code>:
{% highlight xml %}
<dependencies>
  ...
  <dependency>
    <groupId>org.apache.hadoop</groupId>
    <artifactId>hadoop-openstack</artifactId>
  </dependency>
  ...
</dependencies>
{% endhighlight %}

# Configuration Parameters

Create <code>core-sites.xml</code> and place it inside <code>/spark/conf</code> directory.
There are two main categories of parameters that should to be configured: declaration of the
Swift driver and the parameters that are required by Keystone. 

Configuration of Hadoop to use Swift File system achieved via 

<table class="table">
<tr><th>Property Name</th><th>Value</th></tr>
<tr>
  <td>fs.swift.impl</td>
  <td>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</td>
</tr>
</table>

Additional parameters required by Keystone and should be provided to the Swift driver. Those 
parameters will be used to perform authentication in Keystone to access Swift. The following table 
contains a list of Keystone mandatory parameters. <code>PROVIDER</code> can be any name.

<table class="table">
<tr><th>Property Name</th><th>Meaning</th><th>Required</th></tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.auth.url</code></td>
  <td>Keystone Authentication URL</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.auth.endpoint.prefix</code></td>
  <td>Keystone endpoints prefix</td>
  <td>Optional</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.tenant</code></td>
  <td>Tenant</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.username</code></td>
  <td>Username</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.password</code></td>
  <td>Password</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.http.port</code></td>
  <td>HTTP port</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.region</code></td>
  <td>Keystone region</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td><code>fs.swift.service.PROVIDER.public</code></td>
  <td>Indicates if all URLs are public</td>
  <td>Mandatory</td>
</tr>
</table>

For example, assume <code>PROVIDER=SparkTest</code> and Keystone contains user <code>tester</code> with password <code>testing</code>
defined for tenant <code>tenant</code>. Than <code>core-sites.xml</code> should include:

{% highlight xml %}
<configuration>
  <property>
    <name>fs.swift.impl</name>
    <value>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.auth.url</name>
    <value>http://127.0.0.1:5000/v2.0/tokens</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.auth.endpoint.prefix</name>
    <value>endpoints</value>
  </property>
    <name>fs.swift.service.SparkTest.http.port</name>
    <value>8080</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.region</name>
    <value>RegionOne</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.public</name>
    <value>true</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.tenant</name>
    <value>test</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.username</name>
    <value>tester</value>
  </property>
  <property>
    <name>fs.swift.service.SparkTest.password</name>
    <value>testing</value>
  </property>
</configuration>
{% endhighlight %}

Notice that
<code>fs.swift.service.PROVIDER.tenant</code>,
<code>fs.swift.service.PROVIDER.username</code>, 
<code>fs.swift.service.PROVIDER.password</code> contains sensitive information and keeping them in
<code>core-sites.xml</code> is not always a good approach.
We suggest to keep those parameters in <code>core-sites.xml</code> for testing purposes when running Spark
via <code>spark-shell</code>.
For job submissions they should be provided via <code>sparkContext.hadoopConfiguration</code>.

# Usage examples

Assume Keystone's authentication URL is <code>http://127.0.0.1:5000/v2.0/tokens</code> and Keystone contains tenant <code>test</code>, user <code>tester</code> with password <code>testing</code>. In our example we define <code>PROVIDER=SparkTest</code>. Assume that Swift contains container <code>logs</code> with an object <code>data.log</code>. To access <code>data.log</code> from Spark the <code>swift://</code> scheme should be used.


## Running Spark via spark-shell

Make sure that <code>core-sites.xml</code> contains <code>fs.swift.service.SparkTest.tenant</code>, <code>fs.swift.service.SparkTest.username</code>, 
<code>fs.swift.service.SparkTest.password</code>. Run Spark via <code>spark-shell</code> and access Swift via <code>swift://</code> scheme.

{% highlight scala %}
val sfdata = sc.textFile("swift://logs.SparkTest/data.log")
sfdata.count()
{% endhighlight %}


## Sample Application

In this case <code>core-sites.xml</code> need not contain <code>fs.swift.service.SparkTest.tenant</code>, <code>fs.swift.service.SparkTest.username</code>, 
<code>fs.swift.service.SparkTest.password</code>. Example of Java usage:

{% highlight java %}
/* SimpleApp.java */
import org.apache.spark.api.java.*;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.function.Function;

public class SimpleApp {
  public static void main(String[] args) {
    String logFile = "swift://logs.SparkTest/data.log";
    SparkConf conf = new SparkConf().setAppName("Simple Application");
    JavaSparkContext sc = new JavaSparkContext(conf);
    sc.hadoopConfiguration().set("fs.swift.service.ibm.tenant", "test");
    sc.hadoopConfiguration().set("fs.swift.service.ibm.password", "testing");
    sc.hadoopConfiguration().set("fs.swift.service.ibm.username", "tester");
    
    JavaRDD<String> logData = sc.textFile(logFile).cache();

    long num = logData.count();

    System.out.println("Total number of lines: " + num);
  }
}
{% endhighlight %}

The directory structure is 
{% highlight bash %}
./src
./src/main
./src/main/java
./src/main/java/SimpleApp.java
{% endhighlight %}

Maven pom.xml should contain:
{% highlight xml %}
<project>
  <groupId>edu.berkeley</groupId>
  <artifactId>simple-project</artifactId>
  <modelVersion>4.0.0</modelVersion>
  <name>Simple Project</name>
  <packaging>jar</packaging>
  <version>1.0</version>
  <repositories>
  <repository>
    <id>Akka repository</id>
    <url>http://repo.akka.io/releases</url>
  </repository>
  </repositories>
  <build>
	  <plugins>
	    <plugin>
	      <groupId>org.apache.maven.plugins</groupId>
	      <artifactId>maven-compiler-plugin</artifactId>
	      <version>2.3</version>
	      <configuration>
          <source>1.6</source>
          <target>1.6</target>
	      </configuration>
	    </plugin>
	  </plugins>
  </build>
  <dependencies>
	  <dependency> <!-- Spark dependency -->
	    <groupId>org.apache.spark</groupId>
	    <artifactId>spark-core_2.10</artifactId>
	    <version>1.0.0</version>
	  </dependency>
  </dependencies>
</project>
{% endhighlight %}

Compile and execute
{% highlight bash %}
mvn package
SPARK_HOME/spark-submit --class SimpleApp --master local[4] target/simple-project-1.0.jar
{% endhighlight %}
