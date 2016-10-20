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
package org.apache.spark.rdd

import java.io.EOFException
import org.apache.spark.TaskKilledException

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred._
import org.apache.spark._
import org.apache.spark.annotation.{DeveloperApi, Experimental}
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.storage.StorageLevel
import org.apache.spark.util.NextIterator

import scala.reflect.ClassTag
import scala.util.control.NonFatal
import scala.util.{Failure, Success, Try}

@Experimental
class HadoopReliableRDD[K, V](sc: SparkContext,
    broadcastedConf: Broadcast[SerializableWritable[Configuration]],
    initLocalJobConfFuncOpt: Option[JobConf => Unit],
    inputFormatClass: Class[_ <: InputFormat[K, V]],
    keyClass: Class[K],
    valueClass: Class[V],
    minPartitions: Int)
    extends RDD[Try[(K, V)]](sc, Nil) with Logging {

  private val hadoopRDD: HadoopRDD[K,V] = new HadoopRDD[K,V](sc, broadcastedConf,
    initLocalJobConfFuncOpt,inputFormatClass, keyClass,valueClass,minPartitions)

  def this(sc: SparkContext,
            conf: JobConf,
            inputFormatClass: Class[_ <: InputFormat[K, V]],
            keyClass: Class[K],
            valueClass: Class[V],
            minPartitions: Int) = {
    this(
      sc,
      sc.broadcast(new SerializableWritable(conf))
        .asInstanceOf[Broadcast[SerializableWritable[Configuration]]],
      None /* initLocalJobConfFuncOpt */,
      inputFormatClass,
      keyClass,
      valueClass,
      minPartitions)
  }

  override def getPartitions: Array[Partition] = hadoopRDD.getPartitions

  /** Maps over a partition, providing the InputSplit that was used as the base of the partition */
  @DeveloperApi
  def mapPartitionsWithInputSplit[U: ClassTag](
      f: (InputSplit, Iterator[(K, V)]) => Iterator[U],
      preservesPartitioning: Boolean = false): RDD[U] = {
    hadoopRDD.mapPartitionsWithInputSplit(f, preservesPartitioning)
  }

  override def getPreferredLocations(split: Partition): Seq[String] =
    hadoopRDD.getPreferredLocations(split)

  override def checkpoint() {
    // Do nothing. Hadoop RDD should not be checkpointed.
  }

  override def persist(storageLevel: StorageLevel): this.type = {
    if (storageLevel.deserialized) {
      logWarning("Caching NewHadoopRDDs as deserialized objects usually leads to undesired" +
        "behavior because Hadoop's RecordReader reuses the same Writable object for all records." +
        "Use a map transformation to make copies of the records.")
    }
    super.persist(storageLevel)
  }

  def compute(theSplit: Partition, context: TaskContext): InterruptibleIterator[Try[(K, V)]] = {
    val iter:  NextIterator[(K,V)] = hadoopRDD.constructIter(theSplit, context)

    val tryIter = new  NextIterator[Try[(K,V)]] {
      var moreToFollow = true

      override protected def getNext(): Try[(K, V)] = {
        if (moreToFollow) {
          // The first failure is a valid value and should be returned
          // any iteration after the first failure should return the end of input
          val readAttempt = SparkTaskTry(iter.next())
          readAttempt match {
            case Failure(e) => {
              moreToFollow = false
              logWarning(s"Exception on read attempt ${e.getClass.getSimpleName} " +
                s"- ${e.getMessage} - stopping any further read attempts from this split")
            }
            case Success(_) => {
              moreToFollow = iter.hasNext
            }
          }
          readAttempt
        } else {
          // we passed a Failure in the previous get, no more data will be passed from this split
          finished = true
          Failure(new IllegalStateException("Should not attempt to read NextIterator.getNext " +
            " if finished = true")) // return value will be ignored if finished = true
        }
      }
      override protected def close(): Unit = iter.closeIfNeeded()
    }
    new InterruptibleIterator[Try[(K, V)]](context, tryIter)
  }
}

object SparkTaskTry {
  /**
   *  Extends the normal Try constructor to allow TaskKilledExceptions to propagate
   */
  def apply[T](r: => T): Try[T] =
    try Success(r) catch {
      case e: TaskKilledException => throw e
      case NonFatal(e) => Failure(e)
    }
}
