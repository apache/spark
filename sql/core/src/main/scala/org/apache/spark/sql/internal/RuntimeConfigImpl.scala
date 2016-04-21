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

package org.apache.spark.sql.internal

import org.apache.spark.sql.RuntimeConfig

/**
 * Implementation for [[RuntimeConfig]].
 */
class RuntimeConfigImpl extends RuntimeConfig {

  private val conf = new SQLConf

  private val hadoopConf = java.util.Collections.synchronizedMap(
    new java.util.HashMap[String, String]())

  override def set(key: String, value: String): RuntimeConfig = {
    conf.setConfString(key, value)
    this
  }

  override def set(key: String, value: Boolean): RuntimeConfig = set(key, value.toString)

  override def set(key: String, value: Long): RuntimeConfig = set(key, value.toString)

  @throws[NoSuchElementException]("if the key is not set")
  override def get(key: String): String = conf.getConfString(key)

  override def getOption(key: String): Option[String] = {
    try Option(get(key)) catch {
      case _: NoSuchElementException => None
    }
  }

  override def unset(key: String): Unit = conf.unsetConf(key)

  override def setHadoop(key: String, value: String): RuntimeConfig = {
    hadoopConf.put(key, value)
    this
  }

  @throws[NoSuchElementException]("if the key is not set")
  override def getHadoop(key: String): String = hadoopConf.synchronized {
    if (hadoopConf.containsKey(key)) {
      hadoopConf.get(key)
    } else {
      throw new NoSuchElementException(key)
    }
  }

  override def getHadoopOption(key: String): Option[String] = {
    try Option(getHadoop(key)) catch {
      case _: NoSuchElementException => None
    }
  }

  override def unsetHadoop(key: String): Unit = hadoopConf.remove(key)
}
