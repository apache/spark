/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.examples

import java.nio.ByteBuffer

import scala.collection.JavaConversions._
import scala.collection.mutable.ListBuffer
import scala.collection.immutable.Map

import org.apache.cassandra.hadoop.ConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlPagingInputFormat
import org.apache.cassandra.hadoop.cql3.CqlConfigHelper
import org.apache.cassandra.hadoop.cql3.CqlOutputFormat
import org.apache.cassandra.utils.ByteBufferUtil
import org.apache.hadoop.mapreduce.Job

import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.SparkContext._


/*
  Need to create following keyspace and column family in cassandra before running this example
  Start CQL shell using ./bin/cqlsh and execute following commands
  CREATE KEYSPACE retail WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1};
  use retail;
  CREATE TABLE salecount (prod_id text, sale_count int, PRIMARY KEY (prod_id));
  CREATE TABLE ordercf (user_id text,
    time timestamp,
    prod_id text,
    quantity int,
    PRIMARY KEY (user_id, time));
  INSERT INTO ordercf (user_id,
    time,
    prod_id,
    quantity) VALUES  ('bob', 1385983646000, 'iphone', 1);
  INSERT INTO ordercf (user_id,
    time,
    prod_id,
    quantity) VALUES ('tom', 1385983647000, 'samsung', 4);
  INSERT INTO ordercf (user_id,
    time,
    prod_id,
    quantity) VALUES ('dora', 1385983648000, 'nokia', 2);
  INSERT INTO ordercf (user_id,
    time,
    prod_id,
    quantity) VALUES ('charlie', 1385983649000, 'iphone', 2);
*/

/**
 * This example demonstrates how to read and write to cassandra column family created using CQL3
 * using Spark.
 * Parameters : <cassandra_node> <cassandra_port>
 * Usage: ./bin/spark-submit examples.jar \
 *  --class org.apache.spark.examples.CassandraCQLTest localhost 9160
 */
object CassandraCQLTest {

  def main(args: Array[String]) {
    val sparkConf = new SparkConf().setAppName("CQLTestApp")

    val sc = new SparkContext(sparkConf)
    val cHost: String = args(0)
    val cPort: String = args(1)
    val KeySpace = "retail"
    val InputColumnFamily = "ordercf"
    val OutputColumnFamily = "salecount"

    val job = new Job()
    job.setInputFormatClass(classOf[CqlPagingInputFormat])
    ConfigHelper.setInputInitialAddress(job.getConfiguration(), cHost)
    ConfigHelper.setInputRpcPort(job.getConfiguration(), cPort)
    ConfigHelper.setInputColumnFamily(job.getConfiguration(), KeySpace, InputColumnFamily)
    ConfigHelper.setInputPartitioner(job.getConfiguration(), "Murmur3Partitioner")
    CqlConfigHelper.setInputCQLPageRowSize(job.getConfiguration(), "3")

    /** CqlConfigHelper.setInputWhereClauses(job.getConfiguration(), "user_id='bob'") */

    /** An UPDATE writes one or more columns to a record in a Cassandra column family */
    val query = "UPDATE " + KeySpace + "." + OutputColumnFamily + " SET sale_count = ? "
    CqlConfigHelper.setOutputCql(job.getConfiguration(), query)

    job.setOutputFormatClass(classOf[CqlOutputFormat])
    ConfigHelper.setOutputColumnFamily(job.getConfiguration(), KeySpace, OutputColumnFamily)
    ConfigHelper.setOutputInitialAddress(job.getConfiguration(), cHost)
    ConfigHelper.setOutputRpcPort(job.getConfiguration(), cPort)
    ConfigHelper.setOutputPartitioner(job.getConfiguration(), "Murmur3Partitioner")

    val casRdd = sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[CqlPagingInputFormat],
      classOf[java.util.Map[String,ByteBuffer]],
      classOf[java.util.Map[String,ByteBuffer]])

    println("Count: " + casRdd.count)
    val productSaleRDD = casRdd.map {
      case (key, value) => {
        (ByteBufferUtil.string(value.get("prod_id")), ByteBufferUtil.toInt(value.get("quantity")))
      }
    }
    val aggregatedRDD = productSaleRDD.reduceByKey(_ + _)
    aggregatedRDD.collect().foreach {
      case (productId, saleCount) => println(productId + ":" + saleCount)
    }

    val casoutputCF  = aggregatedRDD.map {
      case (productId, saleCount) => {
        val outColFamKey = Map("prod_id" -> ByteBufferUtil.bytes(productId))
        val outKey: java.util.Map[String, ByteBuffer] = outColFamKey
        var outColFamVal = new ListBuffer[ByteBuffer]
        outColFamVal += ByteBufferUtil.bytes(saleCount)
        val outVal: java.util.List[ByteBuffer] = outColFamVal
       (outKey, outVal)
      }
    }

    casoutputCF.saveAsNewAPIHadoopFile(
        KeySpace,
        classOf[java.util.Map[String, ByteBuffer]],
        classOf[java.util.List[ByteBuffer]],
        classOf[CqlOutputFormat],
        job.getConfiguration()
      )

    sc.stop()
  }
}
