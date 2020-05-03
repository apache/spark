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

import scala.collection.JavaConverters._
import scala.util.{Failure, Success, Try}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._

import org.apache.spark.SparkException
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.internal.Logging
import org.apache.spark.rdd.RDD
import org.apache.spark.util.{SerializableConfiguration, Utils}

/**
 * A trait for use with reading custom classes in PySpark. Implement this trait and add custom
 * transformation code by overriding the convert method.
 */
trait Converter[-T, +U] extends Serializable {
  def convert(obj: T): U
}

private[python] object Converter extends Logging {

  def getInstance[T, U](converterClass: Option[String],
                        defaultConverter: Converter[_ >: T, _ <: U]): Converter[T, U] = {
    converterClass.map { cc =>
      Try {
        val c = Utils.classForName[Converter[T, U]](cc).getConstructor().newInstance()
        logInfo(s"Loaded converter: $cc")
        c
      } match {
        case Success(c) => c
        case Failure(err) =>
          logError(s"Failed to load converter: $cc")
          throw err
      }
    }.getOrElse { defaultConverter }
  }
}

/**
 * A converter that handles conversion of common [[org.apache.hadoop.io.Writable]] objects.
 * Other objects are passed through without conversion.
 */
private[python] class WritableToJavaConverter(
    conf: Broadcast[SerializableConfiguration]) extends Converter[Any, Any] {

  /**
   * Converts a [[org.apache.hadoop.io.Writable]] to the underlying primitive, String or
   * object representation
   */
  private def convertWritable(writable: Writable): Any = {
    writable match {
      case iw: IntWritable => iw.get()
      case dw: DoubleWritable => dw.get()
      case lw: LongWritable => lw.get()
      case fw: FloatWritable => fw.get()
      case t: Text => t.toString
      case bw: BooleanWritable => bw.get()
      case byw: BytesWritable =>
        val bytes = new Array[Byte](byw.getLength)
        System.arraycopy(byw.getBytes(), 0, bytes, 0, byw.getLength)
        bytes
      case n: NullWritable => null
      case aw: ArrayWritable =>
        // Due to erasure, all arrays appear as Object[] and they get pickled to Python tuples.
        // Since we can't determine element types for empty arrays, we will not attempt to
        // convert to primitive arrays (which get pickled to Python arrays). Users may want
        // write custom converters for arrays if they know the element types a priori.
        aw.get().map(convertWritable(_))
      case mw: MapWritable =>
        val map = new java.util.HashMap[Any, Any]()
        mw.asScala.foreach { case (k, v) => map.put(convertWritable(k), convertWritable(v)) }
        map
      case w: Writable => WritableUtils.clone(w, conf.value.value)
      case other => other
    }
  }

  override def convert(obj: Any): Any = {
    obj match {
      case writable: Writable =>
        convertWritable(writable)
      case _ =>
        obj
    }
  }
}

/**
 * A converter that converts common types to [[org.apache.hadoop.io.Writable]]. Note that array
 * types are not supported since the user needs to subclass [[org.apache.hadoop.io.ArrayWritable]]
 * to set the type properly. See [[org.apache.spark.api.python.DoubleArrayWritable]] and
 * [[org.apache.spark.api.python.DoubleArrayToWritableConverter]] for an example. They are used in
 * PySpark RDD `saveAsNewAPIHadoopFile` doctest.
 */
private[python] class JavaToWritableConverter extends Converter[Any, Writable] {

  /**
   * Converts common data types to [[org.apache.hadoop.io.Writable]]. Note that array types are not
   * supported out-of-the-box.
   */
  private def convertToWritable(obj: Any): Writable = {
    obj match {
      case i: java.lang.Integer => new IntWritable(i)
      case d: java.lang.Double => new DoubleWritable(d)
      case l: java.lang.Long => new LongWritable(l)
      case f: java.lang.Float => new FloatWritable(f)
      case s: java.lang.String => new Text(s)
      case b: java.lang.Boolean => new BooleanWritable(b)
      case aob: Array[Byte] => new BytesWritable(aob)
      case null => NullWritable.get()
      case map: java.util.Map[_, _] =>
        val mapWritable = new MapWritable()
        map.asScala.foreach { case (k, v) =>
          mapWritable.put(convertToWritable(k), convertToWritable(v))
        }
        mapWritable
      case array: Array[Any] =>
        val arrayWriteable = new ArrayWritable(classOf[Writable])
        arrayWriteable.set(array.map(convertToWritable(_)))
        arrayWriteable
      case other => throw new SparkException(
        s"Data of type ${other.getClass.getName} cannot be used")
    }
  }

  override def convert(obj: Any): Writable = obj match {
    case writable: Writable => writable
    case other => convertToWritable(other)
  }
}

/** Utilities for working with Python objects <-> Hadoop-related objects */
private[python] object PythonHadoopUtil {

  /**
   * Convert a [[java.util.Map]] of properties to a [[org.apache.hadoop.conf.Configuration]]
   */
  def mapToConf(map: java.util.Map[String, String]): Configuration = {
    val conf = new Configuration(false)
    map.asScala.foreach { case (k, v) => conf.set(k, v) }
    conf
  }

  /**
   * Merges two configurations, returns a copy of left with keys from right overwriting
   * any matching keys in left
   */
  def mergeConfs(left: Configuration, right: Configuration): Configuration = {
    val copy = new Configuration(left)
    right.asScala.foreach(entry => copy.set(entry.getKey, entry.getValue))
    copy
  }

  /**
   * Converts an RDD of key-value pairs, where key and/or value could be instances of
   * [[org.apache.hadoop.io.Writable]], into an RDD of base types, or vice versa.
   */
  def convertRDD[K, V](rdd: RDD[(K, V)],
                       keyConverter: Converter[K, Any],
                       valueConverter: Converter[V, Any]): RDD[(Any, Any)] = {
    rdd.map { case (k, v) => (keyConverter.convert(k), valueConverter.convert(v)) }
  }

}
