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

package org.apache.spark.api.python

import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import scala.util.{Failure, Success, Try}
import org.apache.spark.annotation.Experimental


/**
 * :: Experimental ::
 * A trait for use with reading custom classes in PySpark. Implement this trait and add custom
 * transformation code by overriding the convert method.
 */
@Experimental
trait Converter[T, U] extends Serializable {
  def convert(obj: T): U
}

private[python] object Converter extends Logging {

  def getInstance(converterClass: Option[String]): Converter[Any, Any] = {
    converterClass.map { cc =>
      Try {
        val c = Class.forName(cc).newInstance().asInstanceOf[Converter[Any, Any]]
        logInfo(s"Loaded converter: $cc")
        c
      } match {
        case Success(c) => c
        case Failure(err) =>
          logError(s"Failed to load converter: $cc")
          throw err
      }
    }.getOrElse { new DefaultConverter }
  }
}

/**
 * A converter that handles conversion of common [[org.apache.hadoop.io.Writable]] objects.
 * Other objects are passed through without conversion.
 */
private[python] class DefaultConverter extends Converter[Any, Any] {

  /**
   * Converts a [[org.apache.hadoop.io.Writable]] to the underlying primitive, String or
   * object representation
   */
  private def convertWritable(writable: Writable): Any = {
    import collection.JavaConversions._
    writable match {
      case iw: IntWritable => iw.get()
      case dw: DoubleWritable => dw.get()
      case lw: LongWritable => lw.get()
      case fw: FloatWritable => fw.get()
      case t: Text => t.toString
      case bw: BooleanWritable => bw.get()
      case byw: BytesWritable => byw.getBytes
      case n: NullWritable => null
      case aw: ArrayWritable => aw.get().map(convertWritable(_))
      case mw: MapWritable => mapAsJavaMap(mw.map { case (k, v) =>
        (convertWritable(k), convertWritable(v))
      }.toMap)
      case other => other
    }
  }

  def convert(obj: Any): Any = {
    obj match {
      case writable: Writable =>
        convertWritable(writable)
      case _ =>
        obj
    }
  }
}

/** Utilities for working with Python objects <-> Hadoop-related objects */
private[python] object PythonHadoopUtil {

  /**
   * Convert a [[java.util.Map]] of properties to a [[org.apache.hadoop.conf.Configuration]]
   */
  def mapToConf(map: java.util.Map[String, String]): Configuration = {
    import collection.JavaConversions._
    val conf = new Configuration()
    map.foreach{ case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * Merges two configurations, returns a copy of left with keys from right overwriting
   * any matching keys in left
   */
  def mergeConfs(left: Configuration, right: Configuration): Configuration = {
    import collection.JavaConversions._
    val copy = new Configuration(left)
    right.iterator().foreach(entry => copy.set(entry.getKey, entry.getValue))
    copy
  }

  /**
   * Converts an RDD of key-value pairs, where key and/or value could be instances of
   * [[org.apache.hadoop.io.Writable]], into an RDD[(K, V)]
   */
  def convertRDD[K, V](rdd: RDD[(K, V)],
                       keyConverter: Converter[Any, Any],
                       valueConverter: Converter[Any, Any]): RDD[(Any, Any)] = {
    rdd.map { case (k, v) => (keyConverter.convert(k), valueConverter.convert(v)) }
  }

}
