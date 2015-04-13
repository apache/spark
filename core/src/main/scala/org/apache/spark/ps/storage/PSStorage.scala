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

import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer
import scala.reflect.ClassTag

import org.apache.spark.util.Utils
import org.apache.spark.{SparkConf, Logging}

/**
 * local storage interface for ``org.apache.spark.ps.PSServer``
 * and ``PSTask``
 * fetch data from server, cache in local
 */
private[ps] trait PSStorage extends Logging {

  lazy val charset = Charset.forName("UTF-8")

  /**
   * fetch value from parameter server storage.
   * @param k: key
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return value
   */
  def get[K: ClassTag, V: ClassTag](k: K): Option[V]

  /**
   * fetch multi values from parameter server storage.
   * @param ks: multi keys
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return multi values
   */
  def multiGet[K: ClassTag, V: ClassTag](ks: Array[K]): Array[Option[V]]

  /**
   * put value into parameter server storage with specific key
   * @param k: key
   * @param v: value
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  def put[K: ClassTag, V: ClassTag](k: K, v: V): Boolean

  /**
   * put values into parameter server storage with specific key
   * @param ks: keys
   * @param vs: values
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  def multiPut[K: ClassTag, V: ClassTag](ks: Array[K], vs: Array[V]): Boolean

  /**
   * update value into parameter server storage with specific key
   * @param k: key
   * @param v: value
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  def update[K: ClassTag, V: ClassTag](k: K, v: V): Boolean

  /**
   * whether caching data in memory
   * @param bool flag
   */
  def setCacheInMemory(bool: Boolean = true): Unit

  /**
   * clear data of specific table from parameter server storage.
   * @param tbId: table ID
   * @return
   */
  def clear(tbId: Long): Boolean

  /**
   * clear current node(server or task) all data in memery and local disk
   * @return
   */
  def clearAll(): Boolean

  /**
   * check whether the parameter server storage contains specific key/value
   * @param k: key
   * @tparam K: type of key
   * @return
   */
  def exists[K: ClassTag](k: K): Boolean

  /**
   * iterator all data
   * @tparam K: type of Key
   * @tparam V: type of Value
   * @return
   */
  def toIterator[K: ClassTag, V: ClassTag](): Iterator[(K, V)]

  /**
   * apply `agg` and `func` to deltas and original values
   * @param agg: functions applied to deltas
   * @param func: functions allied to original values with specific deltas
   * @tparam K: type of Key
   * @tparam V: type of Value
   */
  def applyDelta[K: ClassTag, V: ClassTag](agg: ArrayBuffer[V] => V, func: (V, V) => V): Unit
}

private[ps] object PSStorage {

  private val configKey = "spark.ps.kv.storage"
  private val FALLBACK_KV_STORAGE = "RocksDB"

  private val psKVStorageNames = Map(
    "DiskStorage" -> classOf[DiskStorage].getName,
    "MemoryStorage" -> classOf[MemoryStorage].getName)

  def getKVStorage(sparkConf: SparkConf): PSStorage = {
    val stName = sparkConf.get(configKey, "MemoryStorage")
    val stClass = psKVStorageNames.getOrElse(stName, stName)
    val st = try {
      val ctor = Class.forName(stClass, true, Utils.getContextOrSparkClassLoader)
        .getConstructor(classOf[SparkConf])
      Some(ctor.newInstance(sparkConf).asInstanceOf[PSStorage])
    } catch {
      case e: ClassNotFoundException => None
      case e: IllegalArgumentException => None
    }
    st.getOrElse(throw new IllegalArgumentException(s"Key/Value Storage [$stClass] is not available. " +
      s"Consider setting $configKey=$FALLBACK_KV_STORAGE"))
  }
}
