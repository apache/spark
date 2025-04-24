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
package org.apache.spark.sql.connect

import org.apache.spark.connect.proto.{ConfigRequest, ConfigResponse, KeyValue}
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigEntry, ConfigReader, OptionalConfigEntry}
import org.apache.spark.sql
import org.apache.spark.sql.connect.client.SparkConnectClient

/**
 * Runtime configuration interface for Spark. To access this, use `SparkSession.conf`.
 *
 * @since 3.4.0
 */
class RuntimeConfig private[sql] (client: SparkConnectClient)
    extends sql.RuntimeConfig
    with Logging { self =>

  /** @inheritdoc */
  def set(key: String, value: String): Unit = {
    executeConfigRequest { builder =>
      builder.getSetBuilder.addPairsBuilder().setKey(key).setValue(value)
    }
  }

  /** @inheritdoc */
  override private[sql] def set[T](entry: ConfigEntry[T], value: T): Unit = {
    require(entry != null, "entry cannot be null")
    require(value != null, s"value cannot be null for key: ${entry.key}")
    set(entry.key, entry.stringConverter(value))
  }

  /** @inheritdoc */
  @throws[NoSuchElementException]("if the key is not set and there is no default value")
  def get(key: String): String = getOption(key).getOrElse {
    throw new NoSuchElementException(key)
  }

  /** @inheritdoc */
  def get(key: String, default: String): String = {
    val kv = executeConfigRequestSinglePair { builder =>
      val pairsBuilder = builder.getGetWithDefaultBuilder
        .addPairsBuilder()
        .setKey(key)
      if (default != null) {
        pairsBuilder.setValue(default)
      }
    }
    if (kv.hasValue) {
      kv.getValue
    } else {
      default
    }
  }

  /** @inheritdoc */
  override private[sql] def get[T](entry: ConfigEntry[T]): T = {
    require(entry != null, "entry cannot be null")
    entry.readFrom(reader)
  }

  /** @inheritdoc */
  override private[sql] def get[T](entry: OptionalConfigEntry[T]): Option[T] = {
    require(entry != null, "entry cannot be null")
    entry.readFrom(reader)
  }

  /** @inheritdoc */
  override private[sql] def get[T](entry: ConfigEntry[T], default: T): T = {
    require(entry != null, "entry cannot be null")
    Option(get(entry.key, null)).map(entry.valueConverter).getOrElse(default)
  }

  /** @inheritdoc */
  override private[sql] def contains(key: String): Boolean = {
    get(key, null) != null
  }

  /** @inheritdoc */
  def getAll: Map[String, String] = {
    val response = executeConfigRequest { builder =>
      builder.getGetAllBuilder
    }
    val builder = Map.newBuilder[String, String]
    response.getPairsList.forEach { kv =>
      require(kv.hasValue)
      builder += ((kv.getKey, kv.getValue))
    }
    builder.result()
  }

  /** @inheritdoc */
  def getOption(key: String): Option[String] = {
    val kv = executeConfigRequestSinglePair { builder =>
      builder.getGetOptionBuilder.addKeys(key)
    }
    if (kv.hasValue) {
      Option(kv.getValue)
    } else {
      None
    }
  }

  /** @inheritdoc */
  def unset(key: String): Unit = {
    executeConfigRequest { builder =>
      builder.getUnsetBuilder.addKeys(key)
    }
  }

  /** @inheritdoc */
  def isModifiable(key: String): Boolean = {
    val kv = executeConfigRequestSinglePair { builder =>
      builder.getIsModifiableBuilder.addKeys(key)
    }
    require(kv.hasValue, "The returned pair does not have a value set")
    java.lang.Boolean.valueOf(kv.getValue)
  }

  private def executeConfigRequestSinglePair(
      f: ConfigRequest.Operation.Builder => Unit): KeyValue = {
    val response = executeConfigRequest(f)
    require(response.getPairsCount == 1, "")
    response.getPairs(0)
  }

  private def executeConfigRequest(f: ConfigRequest.Operation.Builder => Unit): ConfigResponse = {
    val builder = ConfigRequest.Operation.newBuilder()
    f(builder)
    val response = client.config(builder.build())
    response.getWarningsList.forEach { warning =>
      logWarning(warning)
    }
    response
  }

  private val reader = new ConfigReader((key: String) => Option(self.get(key, null)))
}
