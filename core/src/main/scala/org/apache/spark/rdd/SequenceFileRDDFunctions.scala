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

import scala.reflect.ClassTag

import org.apache.hadoop.io.Writable
import org.apache.hadoop.io.compress.CompressionCodec
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapred.SequenceFileOutputFormat

import org.apache.spark.internal.{Logging, LogKeys, MDC}

/**
 * Extra functions available on RDDs of (key, value) pairs to create a Hadoop SequenceFile,
 * through an implicit conversion.
 *
 * @note This can't be part of PairRDDFunctions because we need more implicit parameters to
 * convert our keys and values to Writable.
 */
class SequenceFileRDDFunctions[K: IsWritable: ClassTag, V: IsWritable: ClassTag](
    self: RDD[(K, V)],
    _keyWritableClass: Class[_ <: Writable],
    _valueWritableClass: Class[_ <: Writable])
  extends Logging
  with Serializable {

  /**
   * Output the RDD as a Hadoop SequenceFile using the Writable types we infer from the RDD's key
   * and value types. If the key or value are Writable, then we use their classes directly;
   * otherwise we map primitive types such as Int and Double to IntWritable, DoubleWritable, etc,
   * byte arrays to BytesWritable, and Strings to Text. The `path` can be on any Hadoop-supported
   * file system.
   */
  def saveAsSequenceFile(
      path: String,
      codec: Option[Class[_ <: CompressionCodec]] = None): Unit = self.withScope {
    def anyToWritable[U: IsWritable](u: U): Writable = u

    // TODO We cannot force the return type of `anyToWritable` be same as keyWritableClass and
    // valueWritableClass at the compile time. To implement that, we need to add type parameters to
    // SequenceFileRDDFunctions. however, SequenceFileRDDFunctions is a public class so it will be a
    // breaking change.
    val convertKey = self.keyClass != _keyWritableClass
    val convertValue = self.valueClass != _valueWritableClass

    logInfo(log"Saving as sequence file of type " +
      log"(${MDC(LogKeys.KEY, _keyWritableClass.getSimpleName)}," +
      log"${MDC(LogKeys.VALUE, _valueWritableClass.getSimpleName)})")
    val format = classOf[SequenceFileOutputFormat[Writable, Writable]]
    val jobConf = new JobConf(self.context.hadoopConfiguration)
    if (!convertKey && !convertValue) {
      self.saveAsHadoopFile(path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    } else if (!convertKey && convertValue) {
      self.map(x => (x._1, anyToWritable(x._2))).saveAsHadoopFile(
        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    } else if (convertKey && !convertValue) {
      self.map(x => (anyToWritable(x._1), x._2)).saveAsHadoopFile(
        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    } else if (convertKey && convertValue) {
      self.map(x => (anyToWritable(x._1), anyToWritable(x._2))).saveAsHadoopFile(
        path, _keyWritableClass, _valueWritableClass, format, jobConf, codec)
    }
  }
}
