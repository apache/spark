---
layout: global
title: Accessing Openstack Swift storage from Spark
---

# Accessing Openstack Swift storage from Spark

Spark's file interface allows it to process data in Openstack Swift using the same URI formats that are supported for Hadoop. You can specify a path in Swift as input through a URI of the form `swift://<container.service_provider>/path`. You will also need to set your Swift security credentials, through `SparkContext.hadoopConfiguration`. 

#Configuring Hadoop to use Openstack Swift
Openstack Swift driver was merged in Hadoop verion 2.3.0 ([Swift driver](https://issues.apache.org/jira/browse/HADOOP-8545))  Users that wish to use previous Hadoop versions will need to configure Swift driver manually. 
<h2>Hadoop 2.3.0 and above.</h2>
An Openstack Swift driver was merged into Haddop 2.3.0 . Current Hadoop driver requieres Swift to use Keystone authentication. There are additional efforts to support temp auth for Hadoop [Hadoop-10420](https://issues.apache.org/jira/browse/HADOOP-10420).
To configure Hadoop to work with Swift one need to modify core-sites.xml of Hadoop and setup Swift FS.
  
    <configuration>
      <property>
	  <name>fs.swift.impl</name>
	    <value>org.apache.hadoop.fs.swift.snative.SwiftNativeFileSystem</value>
	  </property>
    </configuration>


<h2>Configuring Spark - stand alone cluster</h2>
You need to configure the compute-classpath.sh and add Hadoop classpath for 

  
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/common/lib/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/hdfs/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/tools/lib/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/hdfs/lib/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/mapreduce/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/mapreduce/lib/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/yarn/*
    CLASSPATH = <YOUR HADOOP PATH>/share/hadoop/yarn/lib/*

Additional parameters has to be provided to the Hadoop from Spark. Swift driver of Hadoop uses those parameters to perform authentication in Keystone needed to access Swift.
List of mandatory parameters is : `fs.swift.service.<PROVIDER>.auth.url`, `fs.swift.service.<PROVIDER>.auth.endpoint.prefix`, `fs.swift.service.<PROVIDER>.tenant`, `fs.swift.service.<PROVIDER>.username`,
`fs.swift.service.<PROVIDER>.password`, `fs.swift.service.<PROVIDER>.http.port`, `fs.swift.service.<PROVIDER>.http.port`, `fs.swift.service.<PROVIDER>.public`. 
Create core-sites.xml and place it under /spark/conf directory. Configure core-sites.xml with general Keystone parameters, for example


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
Another approach is to change Hadoop Swift FS driver to provide them via system environment variables. For now we provide them via core-sites.xml 

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
	  <property>
<h3> Usage </h3>
Assume you have a Swift container `logs` with an object `data.log`. You can use `swift://` scheme to access objects from Swift.

    val sfdata = sc.textFile("swift://logs.<PROVIDER>/data.log")

