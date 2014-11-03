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
import org.scalatest.FunSuite
import org.apache.spark.storage.StorageLevel

class JobExecutionContextSuite extends FunSuite {

  test("nonCompleteExecutionContextURL") {
    try {
      new SparkContext("execution-context", "foo")
    } catch {
      case e: Exception => assert(e.isInstanceOf[SparkException])
    }
  }

  test("classNotFound1") {
    try {
      new SparkContext("execution-context:", "foo")
    } catch {
      case e: Exception => assert(e.isInstanceOf[ClassNotFoundException])
    }
  }

  test("classNotFound2") {
    try {
      new SparkContext("execution-context:foo.bar.Baz", "foo")
    } catch {
      case e: Exception => assert(e.isInstanceOf[ClassNotFoundException])
    }
  }

  test("invalidContextClass") {
    try {
      new SparkContext("execution-context:org.apache.spark.InvalidJobExecutionContext", "foo")
    } catch {
      case e: Exception => assert(e.isInstanceOf[InstantiationException])
    }
  }

  test("validURLandClass") {
    val sc = new SparkContext("execution-context:org.apache.spark.ValidJobExecutionContext", "foo")
    val f = sc.getClass.getDeclaredField("executionContext")
    f.setAccessible(true)
    assert(f.get(sc).isInstanceOf[ValidJobExecutionContext])
  }
  
  test("defaultExecutionContext") {
    val sc = new SparkContext("local", "foo")
    val f = sc.getClass.getDeclaredField("executionContext")
    f.setAccessible(true)
    assert(f.get(sc).isInstanceOf[DefaultExecutionContext])
  }
}

/**
 * 
 */
private class InvalidJobExecutionContext(s: String) extends JobExecutionContext {
  def hadoopFile[K, V](
    sc: SparkContext,
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = 1): RDD[(K, V)] = {
    null
  }

  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    sc: SparkContext,
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration = new Configuration): RDD[(K, V)] = {
    null
  }

  def broadcast[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    null
  }

  def runJob[T, U: ClassTag](
    sc: SparkContext,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit) = {

  }
  
  def persist(sc:SparkContext, rdd:RDD[_], newLevel: StorageLevel):RDD[_] = {
    rdd
  }
  
  def unpersist(sc:SparkContext, rdd:RDD[_], blocking: Boolean = true): RDD[_] = {
    rdd
  }
}

/**
 * 
 */
private class ValidJobExecutionContext extends JobExecutionContext {
  def hadoopFile[K, V](
    sc: SparkContext,
    path: String,
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int = 1): RDD[(K, V)] = {
    null
  }

  def newAPIHadoopFile[K, V, F <: NewInputFormat[K, V]](
    sc: SparkContext,
    path: String,
    fClass: Class[F],
    kClass: Class[K],
    vClass: Class[V],
    conf: Configuration = new Configuration): RDD[(K, V)] = {
    null
  }

  def broadcast[T: ClassTag](sc: SparkContext, value: T): Broadcast[T] = {
    null
  }

  def runJob[T, U: ClassTag](
    sc: SparkContext,
    rdd: RDD[T],
    func: (TaskContext, Iterator[T]) => U,
    partitions: Seq[Int],
    allowLocal: Boolean,
    resultHandler: (Int, U) => Unit) = {

  }
  
  def persist(sc:SparkContext, rdd:RDD[_], newLevel: StorageLevel):RDD[_] = {
    rdd
  }
  
  def unpersist(sc:SparkContext, rdd:RDD[_], blocking: Boolean = true): RDD[_] = {
    rdd
  }
}