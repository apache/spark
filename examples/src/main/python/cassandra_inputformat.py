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
cqlsh:test> INSERT INTO users (user_id,  fname, lname)
        ...   VALUES (1745, 'john', 'smith');
cqlsh:test> INSERT INTO users (user_id,  fname, lname)
        ...   VALUES (1744, 'john', 'doe');
cqlsh:test> INSERT INTO users (user_id,  fname, lname)
        ...   VALUES (1746, 'john', 'smith');
cqlsh:test> SELECT * FROM users;

 user_id | fname | lname
---------+-------+-------
    1745 |  john | smith
    1744 |  john |   doe
    1746 |  john | smith
"""
if __name__ == "__main__":
    if len(sys.argv) != 4:
        print >> sys.stderr, """
        Usage: cassandra_inputformat <host> <keyspace> <cf>

        Run with example jar:
        ./bin/spark-submit --driver-class-path /path/to/example/jar \
        /path/to/examples/cassandra_inputformat.py <host> <keyspace> <cf>
        Assumes you have some data in Cassandra already, running on <host>, in <keyspace> and <cf>
        """
        exit(-1)

    host = sys.argv[1]
    keyspace = sys.argv[2]
    cf = sys.argv[3]
    sc = SparkContext(appName="CassandraInputFormat")

    conf = {"cassandra.input.thrift.address": host,
            "cassandra.input.thrift.port": "9160",
            "cassandra.input.keyspace": keyspace,
            "cassandra.input.columnfamily": cf,
            "cassandra.input.partitioner.class": "Murmur3Partitioner",
            "cassandra.input.page.row.size": "3"}
    cass_rdd = sc.newAPIHadoopRDD(
        "org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat",
        "java.util.Map",
        "java.util.Map",
        keyConverter="org.apache.spark.examples.pythonconverters.CassandraCQLKeyConverter",
        valueConverter="org.apache.spark.examples.pythonconverters.CassandraCQLValueConverter",
        conf=conf)
    output = cass_rdd.collect()
    for (k, v) in output:
        print (k, v)

    sc.stop()
