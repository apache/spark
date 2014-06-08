---
layout: global
title: Accessing Openstack Swift storage from Spark
---

# Accessing Openstack Swift storage from Spark

Spark's file interface allows it to process data in Openstack Swift using the same URI formats that are supported for Hadoop. You can specify a path in Swift as input through a URI of the form `swift://<container.service_provider>/path`. You will also need to set your Swift security credentials, through `SparkContext.hadoopConfiguration`. 

#Configuring Hadoop to use Openstack Swift
Openstack Swift driver was merged in Hadoop verion 2.3.0 ([Swift driver](https://issues.apache.org/jira/browse/HADOOP-8545)).  Users that wish to use previous Hadoop versions will need to configure Swift driver manually. Current Swift driver requieres Swift to use Keystone authentication method. There are recent efforts to support also temp auth [Hadoop-10420](https://issues.apache.org/jira/browse/HADOOP-10420).
To configure Hadoop to work with Swift one need to modify core-sites.xml of Hadoop and setup Swift FS.
  
	<configuration>
		<property>
			<name>fs.swift.impl</name>
			<value>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</value>
		</property>
	</configuration>

#Configuring Swift 
Proxy server of Swift should include `list_endpoints` middleware. More information available [here](https://github.com/openstack/swift/blob/master/swift/common/middleware/list_endpoints.py)

#Configuring Spark
To use Swift driver, Spark need to be compiled with `hadoop-openstack-2.3.0.jar` distributted with Hadoop 2.3.0. 
For the Maven builds, Spark's main pom.xml should include 

	<swift.version>2.3.0</swift.version>


	<dependency>
		<groupId>org.apache.hadoop</groupId>
		<artifactId>hadoop-openstack</artifactId>
		<version>${swift.version}</version>
	</dependency>

in addition, pom.xml of the `core` and `yarn` projects should include

	<dependency>
		<groupId>org.apache.hadoop</groupId>
		<artifactId>hadoop-openstack</artifactId>
	</dependency>


Additional parameters has to be provided to the Swift driver. Swift driver will use those parameters to perform authentication in Keystone prior  accessing Swift. List of mandatory parameters is : `fs.swift.service.<PROVIDER>.auth.url`, `fs.swift.service.<PROVIDER>.auth.endpoint.prefix`, `fs.swift.service.<PROVIDER>.tenant`, `fs.swift.service.<PROVIDER>.username`,
`fs.swift.service.<PROVIDER>.password`, `fs.swift.service.<PROVIDER>.http.port`, `fs.swift.service.<PROVIDER>.http.port`, `fs.swift.service.<PROVIDER>.public`, where `PROVIDER` is any name. `fs.swift.service.<PROVIDER>.auth.url` should point to the Keystone authentication URL.

Create core-sites.xml with the mandatory parameters and place it under /spark/conf directory. For example:


	<property>
		<name>fs.swift.service.<PROVIDER>.auth.url</name>
		<value>http://127.0.0.1:5000/v2.0/tokens</value>
	</property>
	<property>
		<name>fs.swift.service.<PROVIDER>.auth.endpoint.prefix</name>
		<value>endpoints</value>
	</property>
		<name>fs.swift.service.<PROVIDER>.http.port</name>
		<value>8080</value>
	</property>
	<property>
		<name>fs.swift.service.<PROVIDER>.region</name>
		<value>RegionOne</value>
	</property>
	<property>
		<name>fs.swift.service.<PROVIDER>.public</name>
		<value>true</value>
	</property>

We left with `fs.swift.service.<PROVIDER>.tenant`, `fs.swift.service.<PROVIDER>.username`, `fs.swift.service.<PROVIDER>.password`. The best way to provide those parameters to SparkContext in run time, which seems to be impossible yet.
Another approach is to adapt Swift driver to obtain those values from system environment variables. For now we provide them via core-sites.xml. 
Assume a tenant `test` with user `tester` was defined in Keystone, then the core-sites.xml shoud include: 

	<property>
		<name>fs.swift.service.<PROVIDER>.tenant</name>
		<value>test</value>
	</property>
	<property>
		<name>fs.swift.service.<PROVIDER>.username</name>
		<value>tester</value>
	</property>
	<property>
		<name>fs.swift.service.<PROVIDER>.password</name>
		<value>testing</value>
	</property>
# Usage
Assume there exists Swift container `logs` with an object `data.log`. To access `data.log` from Spark the `swift://` scheme should be used. 
For example:

	val sfdata = sc.textFile("swift://logs.<PROVIDER>/data.log")

