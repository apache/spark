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

import java.util.{ArrayList => JArrayList}

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.util.Failure
import scala.util.Try

import net.razorvine.pickle.{Pickler, Unpickler}

import org.apache.spark.SparkException
import org.apache.spark.api.java.JavaRDD
import org.apache.spark.internal.{Logging, MDC}
import org.apache.spark.internal.LogKeys._
import org.apache.spark.rdd.RDD
import org.apache.spark.util.ArrayImplicits._

/** Utilities for serialization / deserialization between Python and Java, using Pickle. */
private[spark] object SerDeUtil extends Logging {
  class ByteArrayConstructor extends net.razorvine.pickle.objects.ByteArrayConstructor {
    override def construct(args: Array[Object]): Object = {
      // Deal with an empty byte array pickled by Python 3.
      if (args.length == 0) {
        Array.emptyByteArray
      } else {
        super.construct(args)
      }
    }
  }

  private var initialized = false
  // This should be called before trying to unpickle array.array from Python
  // In cluster mode, this should be put in closure
  def initialize(): Unit = {
    synchronized {
      if (!initialized) {
        Unpickler.registerConstructor("__builtin__", "bytearray", new ByteArrayConstructor())
        Unpickler.registerConstructor("builtins", "bytearray", new ByteArrayConstructor())
        Unpickler.registerConstructor("__builtin__", "bytes", new ByteArrayConstructor())
        Unpickler.registerConstructor("_codecs", "encode", new ByteArrayConstructor())
        initialized = true
      }
    }
  }
  initialize()


  /**
   * Convert an RDD of Java objects to Array (no recursive conversions).
   * It is only used by pyspark.sql.
   */
  def toJavaArray(jrdd: JavaRDD[Any]): JavaRDD[Array[_]] = {
    jrdd.rdd.map {
      case objs: JArrayList[_] =>
        objs.toArray
      case obj if obj.getClass.isArray =>
        obj.asInstanceOf[Array[_]].toArray
    }.toJavaRDD()
  }

  /**
   * Choose batch size based on size of objects
   */
  private[spark] class AutoBatchedPickler(iter: Iterator[Any]) extends Iterator[Array[Byte]] {
    private val pickle = new Pickler(/* useMemo = */ true,
      /* valueCompare = */ false)
    private var batch = 1
    private val buffer = new mutable.ArrayBuffer[Any]

    override def hasNext: Boolean = iter.hasNext

    override def next(): Array[Byte] = {
      while (iter.hasNext && buffer.length < batch) {
        buffer += iter.next()
      }
      val bytes = pickle.dumps(buffer.toArray)
      val size = bytes.length
      // let  1M < size < 10M
      if (size < 1024 * 1024) {
        batch *= 2
      } else if (size > 1024 * 1024 * 10 && batch > 1) {
        batch /= 2
      }
      buffer.clear()
      bytes
    }
  }

  /**
   * Convert an RDD of Java objects to an RDD of serialized Python objects, that is usable by
   * PySpark.
   */
  def javaToPython(jRDD: JavaRDD[_]): JavaRDD[Array[Byte]] = {
    jRDD.rdd.mapPartitions { iter => new AutoBatchedPickler(iter) }
  }

  /**
   * Convert an RDD of serialized Python objects to RDD of objects, that is usable by PySpark.
   */
  def pythonToJava(pyRDD: JavaRDD[Array[Byte]], batched: Boolean): JavaRDD[Any] = {
    pyRDD.rdd.mapPartitions { iter =>
      initialize()
      val unpickle = new Unpickler
      iter.flatMap { row =>
        val obj = unpickle.loads(row)
        if (batched) {
          obj match {
            case array: Array[Any] => array.toImmutableArraySeq
            case _ => obj.asInstanceOf[JArrayList[_]].asScala
          }
        } else {
          Seq(obj)
        }
      }
    }.toJavaRDD()
  }

  private def checkPickle(t: (Any, Any)): (Boolean, Boolean) = {
    val pickle = new Pickler(/* useMemo = */ true,
      /* valueCompare = */ false)
    val kt = Try {
      pickle.dumps(t._1)
    }
    val vt = Try {
      pickle.dumps(t._2)
    }
    (kt, vt) match {
      case (Failure(kf), Failure(vf)) =>
        logWarning(log"""
               |Failed to pickle Java object as key:
               |${MDC(CLASS_NAME, t._1.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, kf.getMessage)}""".stripMargin)
        logWarning(log"""
               |Failed to pickle Java object as value:
               |${MDC(CLASS_NAME, t._2.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, vf.getMessage)}""".stripMargin)
        (true, true)
      case (Failure(kf), _) =>
        logWarning(log"""
               |Failed to pickle Java object as key:
               |${MDC(CLASS_NAME, t._1.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, kf.getMessage)}""".stripMargin)
        (true, false)
      case (_, Failure(vf)) =>
        logWarning(log"""
               |Failed to pickle Java object as value:
               |${MDC(CLASS_NAME, t._2.getClass.getSimpleName)}, falling back
               |to 'toString'. Error: ${MDC(ERROR, vf.getMessage)}""".stripMargin)
        (false, true)
      case _ =>
        (false, false)
    }
  }

  /**
   * Convert an RDD of key-value pairs to an RDD of serialized Python objects, that is usable
   * by PySpark. By default, if serialization fails, toString is called and the string
   * representation is serialized
   */
  def pairRDDToPython(rdd: RDD[(Any, Any)], batchSize: Int): RDD[Array[Byte]] = {
    val (keyFailed, valueFailed) = rdd.take(1) match {
      case Array() => (false, false)
      case Array(first) => checkPickle(first)
    }

    rdd.mapPartitions { iter =>
      val cleaned = iter.map { case (k, v) =>
        val key = if (keyFailed) k.toString else k
        val value = if (valueFailed) v.toString else v
        Array[Any](key, value)
      }
      if (batchSize == 0) {
        new AutoBatchedPickler(cleaned)
      } else {
        val pickle = new Pickler(/* useMemo = */ true,
          /* valueCompare = */ false)
        cleaned.grouped(batchSize).map(batched => pickle.dumps(batched.asJava))
      }
    }
  }

  /**
   * Convert an RDD of serialized Python tuple (K, V) to RDD[(K, V)].
   */
  def pythonToPairRDD[K, V](pyRDD: RDD[Array[Byte]], batched: Boolean): RDD[(K, V)] = {
    def isPair(obj: Any): Boolean = {
      Option(obj.getClass.getComponentType).exists(!_.isPrimitive) &&
        obj.asInstanceOf[Array[_]].length == 2
    }

    val rdd = pythonToJava(pyRDD, batched).rdd
    rdd.take(1) match {
      case Array(obj) if isPair(obj) =>
        // we only accept (K, V)
      case Array() =>
        // we also accept empty collections
      case Array(other) => throw new SparkException(
        s"RDD element of type ${other.getClass.getName} cannot be used")
    }
    rdd.map { obj =>
      val arr = obj.asInstanceOf[Array[_]]
      (arr.head.asInstanceOf[K], arr.last.asInstanceOf[V])
    }
  }
}
