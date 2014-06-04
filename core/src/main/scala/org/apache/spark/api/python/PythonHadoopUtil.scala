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
import org.apache.spark.{Logging, SparkContext}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.io._
import scala.util.{Failure, Success, Try}


trait Converter {
  def convert(obj: Any): Any
}

object DefaultConverter extends Converter {

  /**
   * Converts a [[org.apache.hadoop.io.Writable]] to the underlying primitive, String or
   * object representation
   */
  private def convertWritable(writable: Writable): Any = {
    import collection.JavaConversions._
    writable match {
      case iw: IntWritable => SparkContext.intWritableConverter().convert(iw)
      case dw: DoubleWritable => SparkContext.doubleWritableConverter().convert(dw)
      case lw: LongWritable => SparkContext.longWritableConverter().convert(lw)
      case fw: FloatWritable => SparkContext.floatWritableConverter().convert(fw)
      case t: Text => SparkContext.stringWritableConverter().convert(t)
      case bw: BooleanWritable => SparkContext.booleanWritableConverter().convert(bw)
      case byw: BytesWritable => SparkContext.bytesWritableConverter().convert(byw)
      case n: NullWritable => null
      case aw: ArrayWritable => aw.get().map(convertWritable(_))
      case mw: MapWritable => mapAsJavaMap(mw.map{ case (k, v) =>
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

class ConverterRegistry extends Logging {

  var keyConverter: Converter = DefaultConverter
  var valueConverter: Converter = DefaultConverter

  def convertKey(obj: Any): Any = keyConverter.convert(obj)

  def convertValue(obj: Any): Any = valueConverter.convert(obj)

  def registerKeyConverter(converterClass: String) = {
    keyConverter = register(converterClass)
    logInfo(s"Loaded and registered key converter ($converterClass)")
  }

  def registerValueConverter(converterClass: String) = {
    valueConverter = register(converterClass)
    logInfo(s"Loaded and registered value converter ($converterClass)")
  }

  private def register(converterClass: String): Converter = {
    Try {
      val converter = Class.forName(converterClass).newInstance().asInstanceOf[Converter]
      converter
    } match {
      case Success(s) => s
      case Failure(err) =>
        logError(s"Failed to register converter: $converterClass")
        throw err
    }

  }
}

/** Utilities for working with Python objects -> Hadoop-related objects */
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
                       keyClass: String,
                       keyConverter: Option[String],
                       valueClass: String,
                       valueConverter: Option[String]) = {
    rdd.mapPartitions { case iter =>
      val registry = new ConverterRegistry
      keyConverter.foreach(registry.registerKeyConverter(_))
      valueConverter.foreach(registry.registerValueConverter(_))
      iter.map { case (k, v) =>
        (registry.convertKey(k).asInstanceOf[K], registry.convertValue(v).asInstanceOf[V])
      }
    }
  }

}
