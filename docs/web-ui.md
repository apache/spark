---
layout: global
title: Web UI
description: Web UI guide for Spark SPARK_VERSION_SHORT
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

Apache Spark provides a suite of web user interfaces (UIs) that you can use
to monitor the status and resource consumption of your Spark cluster.


**Table of Contents**

* This will become a table of contents (this text will be scraped).
{:toc}

## Jobs Tab
The Jobs tab displays a summary page of all jobs in the Spark application and a details page
for each job. The summary page shows high-level information, such as the status, duration, and
progress of all jobs and the overall event timeline. When you click on a job on the summary
page, you see the details page for that job. The details page further shows the event timeline,
DAG visualization, and all stages of the job.

The information that is displayed in this section is
* User: Current Spark user
* Total uptime: Time since Spark application started
* Scheduling mode: See [job scheduling](job-scheduling.html#configuring-pool-properties)
* Number of jobs per status: Active, Completed, Failed

<p style="text-align: center;">
  <img src="img/AllJobsPageDetail1.png" title="Basic info" alt="Basic info" width="20%"/>
</p>

* Event timeline: Displays in chronological order the events related to the executors (added, removed) and the jobs

<p style="text-align: center;">
  <img src="img/AllJobsPageDetail2.png" title="Event timeline" alt="Event timeline"/>
</p>

* Details of jobs grouped by status: Displays detailed information of the jobs including Job ID, description (with a link to detailed job page), submitted time, duration, stages summary and tasks progress bar

<p style="text-align: center;">
  <img src="img/AllJobsPageDetail3.png" title="Details of jobs grouped by status" alt="Details of jobs grouped by status"/>
</p>


When you click on a specific job, you can see the detailed information of this job.

### Jobs detail

This page displays the details of a specific job identified by its job ID. 
* Job Status: (running, succeeded, failed)
* Number of stages per status (active, pending, completed, skipped, failed)
* Associated SQL Query: Link to the sql tab for this job
* Event timeline: Displays in chronological order the events related to the executors (added, removed) and the stages of the job

<p style="text-align: center;">
  <img src="img/JobPageDetail1.png" title="Event timeline" alt="Event timeline"/>
</p>

* DAG visualization: Visual representation of the directed acyclic graph of this job where vertices represent the RDDs or DataFrames and the edges represent an operation to be applied on RDD.

<p style="text-align: center;">
  <img src="img/JobPageDetail2.png" title="DAG" alt="DAG" width="40%">
</p>

* List of stages (grouped by state active, pending, completed, skipped, and failed)
	* Stage ID
	* Description of the stage
	* Submitted timestamp
	* Duration of the stage
	* Tasks progress bar
	* Input: Bytes read from storage in this stage
	* Output: Bytes written in storage in this stage
	* Shuffle read: Total shuffle bytes and records read, includes both data read locally and data read from remote executors
	* Shuffle write: Bytes and records written to disk in order to be read by a shuffle in a future stage

<p style="text-align: center;">
  <img src="img/JobPageDetail3.png" title="DAG" alt="DAG">
</p>

## Stages Tab
The Stages tab displays a summary page that shows the current state of all stages of all jobs in
the Spark application, and, when you click on a stage, a details page for that stage. The details
page shows the event timeline, DAG visualization, and all tasks for the stage.

## Storage Tab
The Storage tab displays the persisted RDDs and DataFrames, if any, in the application. The summary
page shows the storage levels, sizes and partitions of all RDDs, and the details page shows the
sizes and using executors for all partitions in an RDD or DataFrame.

{% highlight scala %}
scala> import org.apache.spark.storage.StorageLevel._
import org.apache.spark.storage.StorageLevel._

scala> val rdd = sc.range(0, 100, 1, 5).setName("rdd")
rdd: org.apache.spark.rdd.RDD[Long] = rdd MapPartitionsRDD[1] at range at <console>:27

scala> rdd.persist(MEMORY_ONLY_SER)
res0: rdd.type = rdd MapPartitionsRDD[1] at range at <console>:27

scala> rdd.count
res1: Long = 100                                                                

scala> val df = Seq((1, "andy"), (2, "bob"), (2, "andy")).toDF("count", "name")
df: org.apache.spark.sql.DataFrame = [count: int, name: string]

scala> df.persist(DISK_ONLY)
res2: df.type = [count: int, name: string]

scala> df.count
res3: Long = 3
{% endhighlight %}

<p style="text-align: center;">
  <img src="img/webui-storage-tab.png"
       title="Storage tab"
       alt="Storage tab"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

After running the above example, we can find two RDDs listed in the Storage tab. Basic information like
storage level, number of partitions and memory overhead are provided. Note that the newly persisted RDDs
or DataFrames are not shown in the tab before they are materialized. To monitor a specific RDD or DataFrame,
make sure an action operation has been triggered.

<p style="text-align: center;">
  <img src="img/webui-storage-detail.png"
       title="Storage detail"
       alt="Storage detail"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

You can click the RDD name 'rdd' for obtaining the details of data persistence, such as the data
distribution on the cluster.


## Environment Tab
The Environment tab displays the values for the different environment and configuration variables,
including JVM, Spark, and system properties.

<p style="text-align: center;">
  <img src="img/webui-env-tab.png"
       title="Env tab"
       alt="Env tab"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

This environment page has five parts. It is a useful place to check whether your properties have
been set correctly.
The first part 'Runtime Information' simply contains the [runtime properties](configuration.html#runtime-environment)
like versions of Java and Scala.
The second part 'Spark Properties' lists the [application properties](configuration.html#application-properties) like
['spark.app.name'](configuration.html#application-properties) and 'spark.driver.memory'.

<p style="text-align: center;">
  <img src="img/webui-env-hadoop.png"
       title="Hadoop Properties"
       alt="Hadoop Properties"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>
Clicking the 'Hadoop Properties' link displays properties relative to Hadoop and YARN. Note that properties like
['spark.hadoop.*'](configuration.html#execution-behavior) are shown not in this part but in 'Spark Properties'.

<p style="text-align: center;">
  <img src="img/webui-env-sys.png"
       title="System Properties"
       alt="System Properties"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>
'System Properties' shows more details about the JVM.

<p style="text-align: center;">
  <img src="img/webui-env-class.png"
       title="Classpath Entries"
       alt="Classpath Entries"
       width="100%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

The last part 'Classpath Entries' lists the classes loaded from different sources, which is very useful
to resolve class conflicts.

## Executors Tab
The Executors tab displays summary information about the executors that were created for the
application, including memory and disk usage and task and shuffle information. The Storage Memory
column shows the amount of memory used and reserved for caching data.

## SQL Tab
If the application executes Spark SQL queries, the SQL tab displays information, such as the duration,
jobs, and physical and logical plans for the queries. Here we include a basic example to illustrate
this tab:
{% highlight scala %}
scala> val df = Seq((1, "andy"), (2, "bob"), (2, "andy")).toDF("count", "name")
df: org.apache.spark.sql.DataFrame = [count: int, name: string]

scala> df.count
res0: Long = 3                                                                  

scala> df.createGlobalTempView("df")

scala> spark.sql("select name,sum(count) from global_temp.df group by name").show
+----+----------+
|name|sum(count)|
+----+----------+
|andy|         3|
| bob|         2|
+----+----------+
{% endhighlight %}

<p style="text-align: center;">
  <img src="img/webui-sql-tab.png"
       title="SQL tab"
       alt="SQL tab"
       width="80%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

Now the above three dataframe/SQL operators are shown in the list. If we click the
'show at \<console\>: 24' link of the last query, we will see the DAG of the job.

<p style="text-align: center;">
  <img src="img/webui-sql-dag.png"
       title="SQL DAG"
       alt="SQL DAG"
       width="50%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>

We can see that details information of each stage. The first block 'WholeStageCodegen'  
compile multiple operator ('LocalTableScan' and 'HashAggregate') together into a single Java
function to improve performance, and metrics like number of rows and spill size are listed in
the block. The second block 'Exchange' shows the metrics on the shuffle exchange, including
number of written shuffle records, total data size, etc.


<p style="text-align: center;">
  <img src="img/webui-sql-plan.png"
       title="logical plans and the physical plan"
       alt="logical plans and the physical plan"
       width="80%" />
  <!-- Images are downsized intentionally to improve quality on retina displays -->
</p>
Clicking the 'Details' link on the bottom displays the logical plans and the physical plan, which
illustrate how Spark parses, analyzes, optimizes and performs the query.


## Streaming Tab
The web UI includes a Streaming tab if the application uses Spark streaming. This tab displays
scheduling delay and processing time for each micro-batch in the data stream, which can be useful
for troubleshooting the streaming application.

