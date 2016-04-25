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

import org.apache.hadoop.conf.Configuration

import org.apache.spark.sql.RuntimeConfig


/**
 * Implementation for [[RuntimeConfig]].
 */
class RuntimeConfigImpl(
    sqlConf: SQLConf = new SQLConf,
    hadoopConf: Configuration = new Configuration)
  extends RuntimeConfig {

  override def set(key: String, value: String): RuntimeConfig = {
    sqlConf.setConfString(key, value)
    this
  }

  override def set(key: String, value: Boolean): RuntimeConfig = set(key, value.toString)

  override def set(key: String, value: Long): RuntimeConfig = set(key, value.toString)

  @throws[NoSuchElementException]("if the key is not set")
  override def get(key: String): String = sqlConf.getConfString(key)

  override def getOption(key: String): Option[String] = {
    try Option(get(key)) catch {
      case _: NoSuchElementException => None
    }
  }

  override def unset(key: String): Unit = sqlConf.unsetConf(key)

  override def setHadoop(key: String, value: String): RuntimeConfig = hadoopConf.synchronized {
    hadoopConf.set(key, value)
    this
  }

  @throws[NoSuchElementException]("if the key is not set")
  override def getHadoop(key: String): String = hadoopConf.synchronized {
    Option(hadoopConf.get(key)).getOrElse {
      throw new NoSuchElementException(key)
    }
  }

  override def getHadoopOption(key: String): Option[String] = hadoopConf.synchronized {
    Option(hadoopConf.get(key))
  }

  override def unsetHadoop(key: String): Unit = hadoopConf.synchronized {
    hadoopConf.unset(key)
  }

}
