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

package org.apache.spark.hbase

import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.rdd.RDD
import java.io.ByteArrayOutputStream
import java.io.DataOutputStream
import java.io.ByteArrayInputStream
import java.io.DataInputStream
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.hbase.client.HConnectionManager
import org.apache.spark.api.java.JavaPairRDD
import java.io.OutputStream
import org.apache.hadoop.hbase.client.HTable
import org.apache.hadoop.hbase.client.Scan
import org.apache.hadoop.hbase.client.Get
import java.util.ArrayList
import org.apache.hadoop.hbase.client.Result
import scala.reflect.ClassTag
import org.apache.hadoop.hbase.client.HConnection
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.client.Increment
import org.apache.hadoop.hbase.client.Delete
import org.apache.spark.SparkContext
import org.apache.hadoop.hbase.mapreduce.TableInputFormat
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil
import org.apache.hadoop.hbase.io.ImmutableBytesWritable
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.mapreduce.Job
import org.apache.hadoop.hbase.mapreduce.TableMapper
import org.apache.hadoop.hbase.mapreduce.IdentityTableMapper
import org.apache.hadoop.hbase.util.Base64
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.SerializableWritable
import java.util.HashMap
import java.util.concurrent.atomic.AtomicInteger
import org.apache.hadoop.hbase.HConstants
import java.util.concurrent.atomic.AtomicLong
import java.util.Timer
import java.util.TimerTask
import org.apache.hadoop.hbase.client.Mutation
import scala.collection.mutable.MutableList
import org.apache.spark.streaming.dstream.DStream

/**
 * HBaseContext is a façade of simple and complex HBase operations
 * like bulk put, get, increment, delete, and scan
 *
 * HBase Context will take the responsibilities to happen to
 * complexity of disseminating the configuration information
 * to the working and managing the life cycle of HConnections.
 *
 * First constructor:
 *  @param sc              Active SparkContext
 *  @param broadcastedConf This is a Broadcast object that holds a
 * serializable Configuration object
 *
 */
@serializable class HBaseContext(@transient sc: SparkContext,
  broadcastedConf: Broadcast[SerializableWritable[Configuration]]) {

  /**
   * Second constructor option:
   *  @param sc     Active SparkContext
   *  @param config Configuration object to make connection to HBase
   */
  def this(@transient sc: SparkContext, @transient config: Configuration) {
    this(sc, sc.broadcast(new SerializableWritable(config)))
  }

  /**
   * A simple enrichment of the traditional Spark RDD foreachPartition.
   * This function differs from the original in that it offers the 
   * developer access to a already connected HConnection object
   * 
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   * 
   * @param RDD  Original RDD with data to iterate over
   * @param f    Function to be given a iterator to iterate through
   *             the RDD values and a HConnection object to interact 
   *             with HBase 
   */
  def foreachPartition[T](rdd: RDD[T],
    f: (Iterator[T], HConnection) => Unit) = {
    rdd.foreachPartition(
      it => hbaseForeachPartition(broadcastedConf, it, f))
  }

  /**
   * A simple enrichment of the traditional Spark Streaming dStream foreach
   * This function differs from the original in that it offers the 
   * developer access to a already connected HConnection object
   * 
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   * 
   * @param DStream     Original DStream with data to iterate over
   * @param f           Function to be given a iterator to iterate through
   *                    the DStream values and a HConnection object to 
   *                    interact with HBase 
   */
  def streamForeach[T](dstream: DStream[T],
    f: (Iterator[T], HConnection) => Unit) = {
    dstream.foreach((rdd, time) => {
      foreachPartition(rdd, f)
    })
  }

  /**
   * A simple enrichment of the traditional Spark RDD mapPartition.
   * This function differs from the original in that it offers the 
   * developer access to a already connected HConnection object
   * 
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   * 
   * Note: Make sure to partition correctly to avoid memory issue when
   *       getting data from HBase
   * 
   * @param RDD     Original RDD with data to iterate over
   * @param mp      Function to be given a iterator to iterate through
   *                the RDD values and a HConnection object to interact 
   *                with HBase
   * @return        Returns a new RDD generated by the user definition
   *                function just like normal mapPartition
   */
  def mapPartition[T, U: ClassTag](rdd: RDD[T],
    mp: (Iterator[T], HConnection) => Iterator[U]): RDD[U] = {

    rdd.mapPartitions[U](it => hbaseMapPartition[T, U](broadcastedConf,
      it,
      mp), true)
  }

  /**
   * A simple enrichment of the traditional Spark Streaming DStream
   * mapPartition.
   * 
   * This function differs from the original in that it offers the 
   * developer access to a already connected HConnection object
   * 
   * Note: Do not close the HConnection object.  All HConnection
   * management is handled outside this method
   * 
   * Note: Make sure to partition correctly to avoid memory issue when
   *       getting data from HBase
   * 
   * @param DStream    Original DStream with data to iterate over
   * @param mp         Function to be given a iterator to iterate through
   *                   the DStream values and a HConnection object to 
   *                   interact with HBase
   * @return           Returns a new DStream generated by the user 
   *                   definition function just like normal mapPartition
   */
  def streamMap[T, U: ClassTag](dstream: DStream[T],
    mp: (Iterator[T], HConnection) => Iterator[U]): DStream[U] = {

    dstream.mapPartitions(it => hbaseMapPartition[T, U](broadcastedConf,
      it,
      mp), true)
  }

  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   * 
   * It allow addition support for a user to take RDD 
   * and generate puts and send them to HBase.  
   * The complexity of even the HConnection is 
   * removed from the developer
   * 
   * @param RDD        Original RDD with data to iterate over
   * @param tableNm  The name of the table to put into 
   * @param f          Function to convert a value in the RDD to a HBase Put
   * @autoFlush        If autoFlush should be turned on
   */
  def bulkPut[T](rdd: RDD[T], tableNm: String, f: (T) => Put, autoFlush: Boolean) {

    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, hConnection) => {
          val htable = hConnection.getTable(tableNm)
          htable.setAutoFlush(autoFlush, true)
          iterator.foreach(T => htable.put(f(T)))
          htable.flushCommits()
          htable.close()
        }))
  }

  /**
   * A simple abstraction over the HBaseContext.streamMapPartition method.
   * 
   * It allow addition support for a user to take a DStream and 
   * generate puts and send them to HBase.  
   * 
   * The complexity of even the HConnection is 
   * removed from the developer
   * 
   * @param DStream    Original DStream with data to iterate over
   * @param tableNm  The name of the table to put into 
   * @param f          Function to convert a value in the RDD to a HBase Put
   * @autoFlush        If autoFlush should be turned on
   */
  def streamBulkPut[T](dstream: DStream[T],
    tableNm: String,
    f: (T) => Put,
    autoFlush: Boolean) = {
    dstream.foreach((rdd, time) => {
      bulkPut(rdd, tableNm, f, autoFlush)
    })
  }

  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   * 
   * It allow addition support for a user to take a RDD and 
   * generate increments and send them to HBase.  
   * 
   * The complexity of even the HConnection is 
   * removed from the developer
   * 
   * @param RDD        Original RDD with data to iterate over
   * @param tableNm  The name of the table to increment to 
   * @param f          Function to convert a value in the RDD to a 
   *                   HBase Increments
   * @batchSize        The number of increments to batch before sending to HBase
   */
  def bulkIncrement[T](rdd: RDD[T], tableNm:String, f:(T) => Increment, batchSize: Int) {
    bulkMutation(rdd, tableNm, f, batchSize)
  }
  
  /**
   * A simple abstraction over the HBaseContext.foreachPartition method.
   * 
   * It allow addition support for a user to take a RDD and generate delete
   * and send them to HBase.  The complexity of even the HConnection is 
   * removed from the developer
   *  
   * @param RDD     Original RDD with data to iterate over
   * @param tableNm     The name of the table to delete from 
   * @param f       function to convert a value in the RDD to a 
   *                HBase Deletes
   * @batchSize     The number of delete to batch before sending to HBase
   */
  def bulkDelete[T](rdd: RDD[T], tableNm:String, f:(T) => Delete, batchSize: Int) {
    bulkMutation(rdd, tableNm, f, batchSize)
  }
  
  /** 
   *  Under lining function to support all bulk mutations
   *  
   *  May be opened up if requested
   */
  private def bulkMutation[T](rdd: RDD[T], tableNm: String, f: (T) => Mutation, batchSize: Int) {
    rdd.foreachPartition(
      it => hbaseForeachPartition[T](
        broadcastedConf,
        it,
        (iterator, hConnection) => {
          val htable = hConnection.getTable(tableNm)
          val mutationList = new ArrayList[Mutation]
          iterator.foreach(T => {
            mutationList.add(f(T))
            if (mutationList.size >= batchSize) {
              htable.batch(mutationList)
              mutationList.clear()
            }
          })
          if (mutationList.size() > 0) {
            htable.batch(mutationList)
            mutationList.clear()
          }
          htable.close()
        }))
  }

  /**
   * A simple abstraction over the HBaseContext.streamForeach method.
   * 
   * It allow addition support for a user to take a DStream and 
   * generate Increments and send them to HBase.  
   * 
   * The complexity of even the HConnection is 
   * removed from the developer
   * 
   * @param DStream    Original DStream with data to iterate over
   * @param tableNm  The name of the table to increments into 
   * @param f          function to convert a value in the RDD to a 
   *                   HBase Increments
   * @batchSize        The number of increments to batch before sending to HBase
   */
  def streamBulkIncrement[T](dstream: DStream[T],
    tableNm: String,
    f: (T) => Increment,
    batchSize: Int) = {
    streamBulkMutation(dstream, tableNm, f, batchSize)
  }
  
  /**
   * A simple abstraction over the HBaseContext.streamForeach method.
   * 
   * It allow addition support for a user to take a DStream and 
   * generate Delete and send them to HBase.  
   * 
   * The complexity of even the HConnection is 
   * removed from the developer
   * 
   * @param DStream   Original DStream with data to iterate over
   * @param tableNm The name of the table to delete from 
   * @param f         function to convert a value in the RDD to a 
   *                  HBase Delete
   * @batchSize       The number of deletes to batch before sending to HBase
   */
  def streamBulkDelete[T](dstream: DStream[T],
    tableNm: String,
    f: (T) => Delete,
    batchSize: Int) = {
    streamBulkMutation(dstream, tableNm, f, batchSize)
  }
  
  /** 
   *  Under lining function to support all bulk streaming mutations
   *  
   *  May be opened up if requested
   */
  private def streamBulkMutation[T](dstream: DStream[T],
    tableNm: String,
    f: (T) => Mutation,
    batchSize: Int) = {
    dstream.foreach((rdd, time) => {
      bulkMutation(rdd, tableNm, f, batchSize)
    })
  }


  /**
   * A simple abstraction over the HBaseContext.mapPartition method.
   * 
   * It allow addition support for a user to take a RDD and generates a
   * new RDD based on Gets and the results they bring back from HBase
   * 
   * @param RDD        Original RDD with data to iterate over
   * @param tableNm  The name of the table to get from 
   * @param makeGet    Function to convert a value in the RDD to a 
   *                   HBase Get
   * @param convertResult This will convert the HBase Result object to 
   *                   what ever the user wants to put in the resulting 
   *                   RDD
   * return            New RDD that is created by the Get to HBase
   */
  def bulkGet[T, U: ClassTag](tableNm: String,
    batchSize: Int,
    rdd: RDD[T],
    makeGet: (T) => Get,
    convertResult: (Result) => U): RDD[U] = {

    val getMapPartition = new GetMapPartition(tableNm,
      batchSize,
      makeGet,
      convertResult)

    rdd.mapPartitions[U](it => hbaseMapPartition[T, U](broadcastedConf,
      it,
      getMapPartition.run), true)
  }

  /**
   * A simple abstraction over the HBaseContext.streamMap method.
   * 
   * It allow addition support for a user to take a DStream and 
   * generates a new DStream based on Gets and the results 
   * they bring back from HBase
   * 
   * @param DStream    Original DStream with data to iterate over
   * @param tableNm  The name of the table to get from 
   * @param makeGet    Function to convert a value in the DStream to a 
   *                   HBase Get
   * @param convertResult This will convert the HBase Result object to 
   *                   what ever the user wants to put in the resulting 
   *                   DStream
   * return            new DStream that is created by the Get to HBase    
   */
  def streamBulkGet[T, U: ClassTag](tableNm: String,
      batchSize:Int,
      dstream: DStream[T],
      makeGet: (T) => Get, 
      convertResult: (Result) => U): DStream[U] = {

    val getMapPartition = new GetMapPartition(tableNm,
      batchSize,
      makeGet,
      convertResult)

    dstream.mapPartitions[U](it => hbaseMapPartition[T, U](broadcastedConf,
      it,
      getMapPartition.run), true)
  }

  /**
   * This function will use the native HBase TableInputFormat with the 
   * given scan object to generate a new RDD
   * 
   *  @param tableNm The name of the table to scan
   *  @param scan      The HBase scan object to use to read data from HBase
   *  @param f         Function to convert a Result object from HBase into 
   *                   what the user wants in the final generated RDD
   *  @return          New RDD with results from scan 
   */
  def hbaseRDD[U: ClassTag](tableNm: String, scan: Scan, f: ((ImmutableBytesWritable, Result)) => U): RDD[U] = {

    var job: Job = new Job(broadcastedConf.value.value)

    TableMapReduceUtil.initTableMapperJob(tableNm, scan, classOf[IdentityTableMapper], null, null, job)

    sc.newAPIHadoopRDD(job.getConfiguration(),
      classOf[TableInputFormat],
      classOf[ImmutableBytesWritable],
      classOf[Result]).map(f)
  }

  /**
   * A overloaded version of HBaseContext hbaseRDD that predefines the 
   * type of the outputing RDD
   * 
   *  @param tableNm The name of the table to scan
   *  @param scan      The HBase scan object to use to read data from HBase
   *  @return          New RDD with results from scan 
   * 
   */
  def hbaseRDD(tableNm: String, scans: Scan): RDD[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])] = {
    hbaseRDD[(Array[Byte], java.util.List[(Array[Byte], Array[Byte], Array[Byte])])](
      tableNm,
      scans,
      (r: (ImmutableBytesWritable, Result)) => {
        val it = r._2.list().iterator()
        val list = new ArrayList[(Array[Byte], Array[Byte], Array[Byte])]()

        while (it.hasNext()) {
          val kv = it.next()
          list.add((kv.getFamily(), kv.getQualifier(), kv.getValue()))
        }

        (r._1.copyBytes(), list)
      })
  }
  
  /** 
   *  Under lining wrapper all foreach functions in HBaseContext
   *  
   */
  private def hbaseForeachPartition[T](configBroadcast: Broadcast[SerializableWritable[Configuration]],
    it: Iterator[T],
    f: (Iterator[T], HConnection) => Unit) = {

    val config = configBroadcast.value.value

    val hConnection = HConnectionStaticCache.getHConnection(config)
    try {
      f(it, hConnection)
    } finally {
      HConnectionStaticCache.finishWithHConnection(config, hConnection)
    }
  }

  /** 
   *  Under lining wrapper all mapPartition functions in HBaseContext
   *  
   */
  private def hbaseMapPartition[K, U](configBroadcast: Broadcast[SerializableWritable[Configuration]],
    it: Iterator[K],
    mp: (Iterator[K], HConnection) => Iterator[U]): Iterator[U] = {

    val config = configBroadcast.value.value

    val hConnection = HConnectionStaticCache.getHConnection(config)

    try {
      val res = mp(it, hConnection)
      res
    } finally {
      HConnectionStaticCache.finishWithHConnection(config, hConnection)
    }
  }
  
  /** 
   *  Under lining wrapper all get mapPartition functions in HBaseContext
   *  
   */
  @serializable private  class GetMapPartition[T, U: ClassTag](tableNm: String, 
      batchSize: Int,
      makeGet: (T) => Get,
      convertResult: (Result) => U) {
    
    
    def run(iterator: Iterator[T], hConnection: HConnection): Iterator[U] = {
      val htable = hConnection.getTable(tableNm)

      val gets = new ArrayList[Get]()
      var res = List[U]()

      while (iterator.hasNext) {
        gets.add(makeGet(iterator.next))

        if (gets.size() == batchSize) {
          var results = htable.get(gets)
          res = res ++ results.map(convertResult)
          gets.clear()
        }
      }
      if (gets.size() > 0) {
        val results = htable.get(gets)
        res = res ++ results.map(convertResult)
        gets.clear()
      }
      htable.close()
      res.iterator
    }
  }
}