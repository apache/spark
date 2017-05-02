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

package org.apache.spark.api.r

import java.io.File
import java.util.{Map => JMap}

import scala.collection.JavaConverters._
import scala.reflect.ClassTag

import org.apache.spark._
import org.apache.spark.api.java.{JavaPairRDD, JavaRDD, JavaSparkContext}
import org.apache.spark.api.python.PythonRDD
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD

private abstract class BaseRRDD[T: ClassTag, U: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Broadcast[Object]])
  extends RDD[U](parent) with Logging {
  override def getPartitions: Array[Partition] = parent.partitions

  override def compute(partition: Partition, context: TaskContext): Iterator[U] = {
    val runner = new RRunner[U](
      func, deserializer, serializer, packageNames, broadcastVars, numPartitions)

    // The parent may be also an RRDD, so we should launch it first.
    val parentIterator = firstParent[T].iterator(partition, context)

    runner.compute(parentIterator, partition.index)
  }
}

/**
 * Form an RDD[(Int, Array[Byte])] from key-value pairs returned from R.
 * This is used by SparkR's shuffle operations.
 */
private class PairwiseRRDD[T: ClassTag](
    parent: RDD[T],
    numPartitions: Int,
    hashFunc: Array[Byte],
    deserializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Object])
  extends BaseRRDD[T, (Int, Array[Byte])](
    parent, numPartitions, hashFunc, deserializer,
    SerializationFormats.BYTE, packageNames,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {
  lazy val asJavaPairRDD : JavaPairRDD[Int, Array[Byte]] = JavaPairRDD.fromRDD(this)
}

/**
 * An RDD that stores serialized R objects as Array[Byte].
 */
private class RRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    serializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Object])
  extends BaseRRDD[T, Array[Byte]](
    parent, -1, func, deserializer, serializer, packageNames,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {
  lazy val asJavaRDD : JavaRDD[Array[Byte]] = JavaRDD.fromRDD(this)
}

/**
 * An RDD that stores R objects as Array[String].
 */
private class StringRRDD[T: ClassTag](
    parent: RDD[T],
    func: Array[Byte],
    deserializer: String,
    packageNames: Array[Byte],
    broadcastVars: Array[Object])
  extends BaseRRDD[T, String](
    parent, -1, func, deserializer, SerializationFormats.STRING, packageNames,
    broadcastVars.map(x => x.asInstanceOf[Broadcast[Object]])) {
  lazy val asJavaRDD : JavaRDD[String] = JavaRDD.fromRDD(this)
}

private[r] object RRDD {
  def createSparkContext(
      master: String,
      appName: String,
      sparkHome: String,
      jars: Array[String],
      sparkEnvirMap: JMap[Object, Object],
      sparkExecutorEnvMap: JMap[Object, Object]): JavaSparkContext = {
    val sparkConf = new SparkConf().setAppName(appName)
                                   .setSparkHome(sparkHome)

    // Override `master` if we have a user-specified value
    if (master != "") {
      sparkConf.setMaster(master)
    } else {
      // If conf has no master set it to "local" to maintain
      // backwards compatibility
      sparkConf.setIfMissing("spark.master", "local")
    }

    for ((name, value) <- sparkEnvirMap.asScala) {
      sparkConf.set(name.toString, value.toString)
    }
    for ((name, value) <- sparkExecutorEnvMap.asScala) {
      sparkConf.setExecutorEnv(name.toString, value.toString)
    }

    if (sparkEnvirMap.containsKey("spark.r.sql.derby.temp.dir") &&
        System.getProperty("derby.stream.error.file") == null) {
      // This must be set before SparkContext is instantiated.
      System.setProperty("derby.stream.error.file",
                         Seq(sparkEnvirMap.get("spark.r.sql.derby.temp.dir").toString, "derby.log")
                         .mkString(File.separator))
    }

    val jsc = new JavaSparkContext(sparkConf)
    jars.foreach { jar =>
      jsc.addJar(jar)
    }
    jsc
  }

  /**
   * Create an RRDD given a sequence of byte arrays. Used to create RRDD when `parallelize` is
   * called from R.
   */
  def createRDDFromArray(jsc: JavaSparkContext, arr: Array[Array[Byte]]): JavaRDD[Array[Byte]] = {
    JavaRDD.fromRDD(jsc.sc.parallelize(arr, arr.length))
  }

  /**
   * Create an RRDD given a temporary file name. This is used to create RRDD when parallelize is
   * called on large R objects.
   *
   * @param fileName name of temporary file on driver machine
   * @param parallelism number of slices defaults to 4
   */
  def createRDDFromFile(jsc: JavaSparkContext, fileName: String, parallelism: Int):
  JavaRDD[Array[Byte]] = {
    PythonRDD.readRDDFromFile(jsc, fileName, parallelism)
  }
}
