---
layout: global
title: Accessing OpenStack Swift from Spark
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

Spark's support for Hadoop InputFormat allows it to process data in OpenStack Swift using the
same URI formats as in Hadoop. You can specify a path in Swift as input through a 
URI of the form <code>swift://container.PROVIDER/path</code>. You will also need to set your 
Swift security credentials, through <code>core-site.xml</code> or via
<code>SparkContext.hadoopConfiguration</code>.
The current Swift driver requires Swift to use the Keystone authentication method, or
its Rackspace-specific predecessor.

# Configuring Swift for Better Data Locality

Although not mandatory, it is recommended to configure the proxy server of Swift with
<code>list_endpoints</code> to have better data locality. More information is
[available here](https://github.com/openstack/swift/blob/master/swift/common/middleware/list_endpoints.py).


# Dependencies

The Spark application should include <code>hadoop-openstack</code> dependency, which can
be done by including the `hadoop-cloud` module for the specific version of spark used.
For example, for Maven support, add the following to the <code>pom.xml</code> file:

{% highlight xml %}
<dependencyManagement>
  ...
  <dependency>
    <groupId>org.apache.spark</groupId>
    <artifactId>hadoop-cloud_2.12</artifactId>
    <version>${spark.version}</version>
  </dependency>
  ...
</dependencyManagement>
{% endhighlight %}

# Configuration Parameters

Create <code>core-site.xml</code> and place it inside Spark's <code>conf</code> directory.
The main category of parameters that should be configured is the authentication parameters
required by Keystone.

The following table contains a list of Keystone mandatory parameters. <code>PROVIDER</code> can be
any (alphanumeric) name.

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
  <td>Indicates whether to use the public (off cloud) or private (in cloud; no transfer fees) endpoints</td>
  <td>Mandatory</td>
</tr>
</table>

For example, assume <code>PROVIDER=SparkTest</code> and Keystone contains user <code>tester</code> with password <code>testing</code>
defined for tenant <code>test</code>. Then <code>core-site.xml</code> should include:

{% highlight xml %}
<configuration>
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
<code>core-site.xml</code> is not always a good approach.
We suggest to keep those parameters in <code>core-site.xml</code> for testing purposes when running Spark
via <code>spark-shell</code>.
For job submissions they should be provided via <code>sparkContext.hadoopConfiguration</code>.
