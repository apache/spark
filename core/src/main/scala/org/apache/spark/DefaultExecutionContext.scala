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
package org.apache.spark

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapred.{ FileInputFormat, InputFormat, JobConf }
import org.apache.hadoop.mapreduce.{ InputFormat => NewInputFormat, Job => NewHadoopJob }
import org.apache.hadoop.mapreduce.lib.input.{ FileInputFormat => NewFileInputFormat }
import scala.reflect.ClassTag
import org.apache.spark.rdd.RDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.rdd.HadoopRDD
import org.apache.spark.rdd.NewHadoopRDD
import org.apache.spark.rdd.UnionRDD
import org.apache.spark.storage.StorageLevel
/**
 * Default implementation of JobExecutionContext which delegates to native Spark execution
 */
class DefaultExecutionContext extends JobExecutionContext with Logging {
  
  /**
   * 
   */
  def hadoopFile[K, V](
    sc:SparkContext,
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = 1): RDD[(K, V)] = {
    // A Hadoop configuration can be about 10 KB, which is pretty big, so broadcast it.
    val confBroadcast = broadcast(sc, new SerializableWritable(sc.hadoopConfiguration))
    val setInputPathsFunc = (jobConf: JobConf) => FileInputFormat.setInputPaths(jobConf, path)
    new HadoopRDD(
      sc,
      confBroadcast,
      Some(setInputPathsFunc),
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions).setName(path)
  }

  /**
   * 
   */
  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    sc:SparkContext,
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration = new Configuration): RDD[(K, V)] = {
    
    val job = new NewHadoopJob(conf)
    NewFileInputFormat.addInputPath(job, new Path(path))
    val updatedConf = job.getConfiguration
    new NewHadoopRDD(sc, fClass, kClass, vClass, updatedConf).setName(path)
  }

  /**
   * 
   */
  def broadcast[T: ClassTag](sc:SparkContext, value: T): Broadcast[T] = {
    val bc = sc.env.broadcastManager.newBroadcast[T](value, sc.isLocal)
    val callSite = sc.getCallSite
    logInfo("Created broadcast " + bc.id + " from " + callSite.shortForm)
    sc.cleaner.foreach(_.registerBroadcastForCleanup(bc))
    bc
  }

  /**
   * 
   */
  def runJob[T, U: ClassTag](
    sc:SparkContext,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit) = {
    
    if (sc.dagScheduler == null) {
      throw new SparkException("SparkContext has been shutdown")
    }
    val callSite = sc.getCallSite
    val cleanedFunc = sc.clean(func)
    logInfo("Starting job: " + callSite.shortForm)
    sc.dagScheduler.runJob(rdd, cleanedFunc, partitions, callSite, allowLocal,
      resultHandler, sc.getLocalProperties)
    rdd.doCheckpoint()
  }
  
  /**
   * 
   */
  def persist(sc:SparkContext, rdd:RDD[_], newLevel: StorageLevel):RDD[_] = {
    sc.persistRDD(rdd)
    // Register the RDD with the ContextCleaner for automatic GC-based cleanup
    sc.cleaner.foreach(_.registerRDDForCleanup(rdd))
    rdd
  }
  
  /**
   * 
   */
  def unpersist(sc:SparkContext, rdd:RDD[_], blocking: Boolean = true): RDD[_] = {
    sc.unpersistRDD(rdd.id, blocking)
    rdd
  }
}
