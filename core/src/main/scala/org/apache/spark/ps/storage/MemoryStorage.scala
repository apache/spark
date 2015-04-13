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

package org.apache.spark.ps.storage

import scala.collection.mutable
import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.SparkConf

/**
 * memory kv storage
 */
class MemoryStorage(conf: SparkConf) extends PSStorage {

  private val keyValues = new mutable.HashMap[String, Array[Double]]()
  private val updatedKeyValues = new mutable.HashMap[String, ArrayBuffer[Array[Double]]]()

  private val inValidV = new Array[Double](0)

  /**
   * fetch value from parameter server storage.
   * @param k: key
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return value
   */
  override def get[K: ClassTag, V: ClassTag](k: K): Option[V] = {
    Some(keyValues.getOrElse(k.asInstanceOf[String], inValidV)).asInstanceOf[Option[V]]
  }

  /**
   * fetch multi values from parameter server storage.
   * @param ks: multi keys
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return multi values
   */
  override def multiGet[K: ClassTag, V: ClassTag](ks: Array[K]): Array[Option[V]] = {
    Array(None)
  }

  /**
   * put value into parameter server storage with specific key
   * @param k: key
   * @param v: value
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  override def put[K: ClassTag, V: ClassTag](k: K, v: V): Boolean = {
    keyValues.synchronized {
      keyValues(k.asInstanceOf[String]) = v.asInstanceOf[Array[Double]]
    }

    true
  }

  /**
   * put values into parameter server storage with specific key
   * @param ks: keys
   * @param vs: values
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  override def multiPut[K: ClassTag, V: ClassTag](ks: Array[K], vs: Array[V]): Boolean = false

  override def update[K: ClassTag, V: ClassTag](k: K, v: V): Boolean = {
    updatedKeyValues.synchronized {
      if (updatedKeyValues.contains(k.asInstanceOf[String])) {
        updatedKeyValues(k.asInstanceOf[String]) += v.asInstanceOf[Array[Double]]
      } else {
        val updatedValues = new ArrayBuffer[Array[Double]]
        updatedValues += v.asInstanceOf[Array[Double]]
        updatedKeyValues(k.asInstanceOf[String]) = updatedValues
      }
    }

    true
  }

  /**
   * whether caching data in memory
   * @param bool flag
   */
  override def setCacheInMemory(bool: Boolean = true): Unit = ???

  /**
   * clear data of specific table from parameter server storage.
   * @param tbId: table ID
   * @return
   */
  override def clear(tbId: Long): Boolean = false

  /**
   * clear current node(server or task) all data in memery and local disk
   * @return
   */
  override def clearAll(): Boolean = ???

  /**
   * check whether the parameter server storage contains specific key/value
   * @param k: key
   * @tparam K: type of key
   * @return
   */
  override def exists[K: ClassTag](k: K): Boolean = keyValues.contains(k.asInstanceOf[String])

  /**
   * iterator all data
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  override def toIterator[K: ClassTag, V: ClassTag](): Iterator[(K, V)] = keyValues.toIterator.asInstanceOf[Iterator[(K, V)]]

  /**
   * apply `agg` and `func` to deltas and original values
   * @param agg: functions applied to deltas
   * @param func: functions allied to original values with specific deltas
   * @tparam K: type of Key
   * @tparam V: type of Value
   */
  override def applyDelta[K: ClassTag, V: ClassTag](agg: ArrayBuffer[V] => V, func: (V, V) => V): Unit = {
    updatedKeyValues.foreach( e => {
      if (keyValues.contains(e._1)) {
        val deltaV = agg(e._2.asInstanceOf[ArrayBuffer[V]])
        println(s"update agg delta ${deltaV.asInstanceOf[Array[Double]].mkString(", ")}")
        keyValues(e._1) = func(keyValues(e._1).asInstanceOf[V], deltaV.asInstanceOf[V]).asInstanceOf[Array[Double]]
        println(s"set ${e._1} to ${keyValues(e._1).mkString(", ")}")
        e._2.clear()
      }
    })
  }
}
