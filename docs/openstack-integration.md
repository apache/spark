layout: global
title: Accessing Openstack Swift from Spark
---

# Accessing Openstack Swift from Spark

Spark's file interface allows it to process data in Openstack Swift using the same URI 
formats that are supported for Hadoop. You can specify a path in Swift as input through a 
URI of the form `swift://<container.PROVIDER/path`. You will also need to set your 
Swift security credentials, through `core-sites.xml` or via `SparkContext.hadoopConfiguration`. 
Openstack Swift driver was merged in Hadoop version 2.3.0 ([Swift driver](https://issues.apache.org/jira/browse/HADOOP-8545)).  Users that wish to use previous Hadoop versions will need to configure Swift driver manually. Current Swift driver 
requieres Swift to use Keystone authentication method. There are recent efforts to support 
temp auth [Hadoop-10420](https://issues.apache.org/jira/browse/HADOOP-10420).

# Configuring Swift 
Proxy server of Swift should include `list_endpoints` middleware. More information 
available [here](https://github.com/openstack/swift/blob/master/swift/common/middleware/list_endpoints.py)

# Compilation of Spark
Spark should be compiled with `hadoop-openstack-2.3.0.jar` that is distributted with Hadoop 2.3.0. 
For the Maven builds, the `dependencyManagement` section of Spark's main `pom.xml` should include 

	<dependencyManagement>
	---------
	<dependency>
		<groupId>org.apache.hadoop</groupId>
		<artifactId>hadoop-openstack</artifactId>
		<version>2.3.0</version>
	</dependency>
	----------
	</dependencyManagement>

in addition, both `core` and `yarn` projects should add `hadoop-openstack` to the `dependencies` section of their `pom.xml`

	<dependencies>
	----------
	<dependency>
		<groupId>org.apache.hadoop</groupId>
		<artifactId>hadoop-openstack</artifactId>
	</dependency>
	----------
	</dependencies>
# Configuration of Spark
Create `core-sites.xml` and place it inside `/spark/conf` directory. There are two main categories of parameters that should to be 
configured: declaration of the Swift driver and the parameters that are required by Keystone. 

Configuration of Hadoop to use Swift File system achieved via 

<table class="table">
<tr><th>Property Name</th><th>Value</th></tr>
<tr>
  <td>fs.swift.impl</td>
  <td>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</td>
<tr>
</table>

Additional parameters requiered by Keystone and should be provided to the Swift driver. Those 
parameters will be used to perform authentication in Keystone to access Swift. The following table 
contains a list of Keystone mandatory parameters. `PROVIDER` can be any name.

<table class="table">
<tr><th>Property Name</th><th>Meaning</th><th>Required</th></tr>
<tr>
  <td>fs.swift.service.PROVIDER.auth.url</td>
  <td>Keystone Authentication URL</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.auth.endpoint.prefix</td>
  <td>Keystone endpoints prefix</td>
  <td>Optional</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.tenant</td>
  <td>Tenant</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.username</td>
  <td>Username</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.password</td>
  <td>Password</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.http.port</td>
  <td>HTTP port</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.region</td>
  <td>Keystone region</td>
  <td>Mandatory</td>
</tr>
<tr>
  <td>fs.swift.service.PROVIDER.public</td>
  <td>Indicates if all URLs are public</td>
  <td>Mandatory</td>
</tr>
</table>

For example, assume `PROVIDER=SparkTest` and Keystone contains user `tester` with password `testing` defined for tenant `tenant`. 
Than `core-sites.xml` should include:

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

Notice that `fs.swift.service.PROVIDER.tenant`, `fs.swift.service.PROVIDER.username`, 
`fs.swift.service.PROVIDER.password` contains sensitive information and keeping them in `core-sites.xml` is not always a good approach. 
We suggest to keep those parameters in `core-sites.xml` for testing purposes when running Spark via `spark-shell`. For job submissions they should be provided via `sparkContext.hadoopConfiguration`

# Usage examples
Assume Keystone's authentication URL is `http://127.0.0.1:5000/v2.0/tokens` and Keystone contains tenant `test`, user `tester` with password `testing`. In our example we define `PROVIDER=SparkTest`. Assume that Swift contains container `logs` with an object `data.log`. To access `data.log` 
from Spark the `swift://` scheme should be used.

## Running Spark via spark-shell
Make sure that `core-sites.xml` contains `fs.swift.service.SparkTest.tenant`, `fs.swift.service.SparkTest.username`, 
`fs.swift.service.SparkTest.password`. Run Spark via `spark-shell` and access Swift via `swift:\\` scheme.

	val sfdata = sc.textFile("swift://logs.SparkTest/data.log")
	sfdata.count()

## Job submission via spark-submit
In this case `core-sites.xml` need not contain `fs.swift.service.SparkTest.tenant`, `fs.swift.service.SparkTest.username`, 
`fs.swift.service.SparkTest.password`. Example of Java usage:

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

The directory sturture is 

	find .
	./src
	./src/main
	./src/main/java
	./src/main/java/SimpleApp.java

Maven pom.xml is

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

Compile and execute

	mvn package
	SPARK_HOME/spark-submit  --class "SimpleApp"   --master local[4]   target/simple-project-1.0.jar

