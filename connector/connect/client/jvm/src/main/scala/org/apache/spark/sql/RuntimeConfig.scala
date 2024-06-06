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
package org.apache.spark.sql

import org.apache.spark.connect.proto.{ConfigRequest, ConfigResponse, KeyValue}
import org.apache.spark.internal.Logging
import org.apache.spark.sql.connect.client.SparkConnectClient

/**
 * Runtime configuration interface for Spark. To access this, use `SparkSession.conf`.
 *
 * @since 3.4.0
 */
class RuntimeConfig private[sql] (client: SparkConnectClient) extends Logging {

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 3.4.0
   */
  def set(key: String, value: String): Unit = {
    executeConfigRequest { builder =>
      builder.getSetBuilder.addPairsBuilder().setKey(key).setValue(value)
    }
  }

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 3.4.0
   */
  def set(key: String, value: Boolean): Unit = set(key, String.valueOf(value))

  /**
   * Sets the given Spark runtime configuration property.
   *
   * @since 3.4.0
   */
  def set(key: String, value: Long): Unit = set(key, String.valueOf(value))

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @throws java.util.NoSuchElementException
   *   if the key is not set and does not have a default value
   * @since 3.4.0
   */
  @throws[NoSuchElementException]("if the key is not set")
  def get(key: String): String = getOption(key).getOrElse {
    throw new NoSuchElementException(key)
  }

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @since 3.4.0
   */
  def get(key: String, default: String): String = {
    executeConfigRequestSingleValue { builder =>
      builder.getGetWithDefaultBuilder.addPairsBuilder().setKey(key).setValue(default)
    }
  }

  /**
   * Returns all properties set in this conf.
   *
   * @since 3.4.0
   */
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

  /**
   * Returns the value of Spark runtime configuration property for the given key.
   *
   * @since 3.4.0
   */
  def getOption(key: String): Option[String] = {
    val pair = executeConfigRequestSinglePair { builder =>
      builder.getGetOptionBuilder.addKeys(key)
    }
    if (pair.hasValue) {
      Option(pair.getValue)
    } else {
      None
    }
  }

  /**
   * Resets the configuration property for the given key.
   *
   * @since 3.4.0
   */
  def unset(key: String): Unit = {
    executeConfigRequest { builder =>
      builder.getUnsetBuilder.addKeys(key)
    }
  }

  /**
   * Indicates whether the configuration property with the given key is modifiable in the current
   * session.
   *
   * @return
   *   `true` if the configuration property is modifiable. For static SQL, Spark Core, invalid
   *   (not existing) and other non-modifiable configuration properties, the returned value is
   *   `false`.
   * @since 3.4.0
   */
  def isModifiable(key: String): Boolean = {
    val modifiable = executeConfigRequestSingleValue { builder =>
      builder.getIsModifiableBuilder.addKeys(key)
    }
    java.lang.Boolean.valueOf(modifiable)
  }

  private def executeConfigRequestSingleValue(
      f: ConfigRequest.Operation.Builder => Unit): String = {
    val pair = executeConfigRequestSinglePair(f)
    require(pair.hasValue, "The returned pair does not have a value set")
    pair.getValue
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
}
