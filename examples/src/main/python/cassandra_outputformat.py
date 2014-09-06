#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

import sys

from pyspark import SparkContext

"""
Create data in Cassandra fist
(following: https://wiki.apache.org/cassandra/GettingStarted)

cqlsh> CREATE KEYSPACE test
   ... WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : 1 };
cqlsh> use test;
cqlsh:test> CREATE TABLE users (
        ...   user_id int PRIMARY KEY,
        ...   fname text,
        ...   lname text
        ... );

> cassandra_outputformat <host> test users 1745 john smith
> cassandra_outputformat <host> test users 1744 john doe
> cassandra_outputformat <host> test users 1746 john smith

cqlsh:test> SELECT * FROM users;

 user_id | fname | lname
---------+-------+-------
    1745 |  john | smith
    1744 |  john |   doe
    1746 |  john | smith
"""
if __name__ == "__main__":
    if len(sys.argv) != 7:
        print >> sys.stderr, """
        Usage: cassandra_outputformat <host> <keyspace> <cf> <user_id> <fname> <lname>

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/cassandra_outputformat.py <args>
        Assumes you have created the following table <cf> in Cassandra already,
        running on <host>, in <keyspace>.

        cqlsh:<keyspace>> CREATE TABLE <cf> (
           ...   user_id int PRIMARY KEY,
           ...   fname text,
           ...   lname text
           ... );
        """
        exit(-1)

    host = sys.argv[1]
    keyspace = sys.argv[2]
    cf = sys.argv[3]
    sc = SparkContext(appName="CassandraOutputFormat")

    conf = {"cassandra.output.thrift.address": host,
            "cassandra.output.thrift.port": "9160",
            "cassandra.output.keyspace": keyspace,
            "cassandra.output.partitioner.class": "Murmur3Partitioner",
            "cassandra.output.cql": "UPDATE " + keyspace + "." + cf + " SET fname = ?, lname = ?",
            "mapreduce.output.basename": cf,
            "mapreduce.outputformat.class": "org.apache.cassandra.hadoop.cql3.CqlOutputFormat",
            "mapreduce.job.output.key.class": "java.util.Map",
            "mapreduce.job.output.value.class": "java.util.List"}
    key = {"user_id": int(sys.argv[4])}
    sc.parallelize([(key, sys.argv[5:])]).saveAsNewAPIHadoopDataset(
        conf=conf,
        keyConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLKeyConverter",
        valueConverter="org.apache.spark.examples.pythonconverters.ToCassandraCQLValueConverter")

    sc.stop()
