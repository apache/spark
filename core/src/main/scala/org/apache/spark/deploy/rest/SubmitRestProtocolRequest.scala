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

package org.apache.spark.deploy.rest

import scala.util.Try

import org.apache.spark.util.Utils

/**
 * An abstract request sent from the client in the REST application submission protocol.
 */
private[rest] abstract class SubmitRestProtocolRequest extends SubmitRestProtocolMessage {
  var clientSparkVersion: String = null
  protected override def doValidate(): Unit = {
    super.doValidate()
    assertFieldIsSet(clientSparkVersion, "clientSparkVersion")
  }
}

/**
 * A request to launch a new application in the REST application submission protocol.
 */
private[rest] class CreateSubmissionRequest extends SubmitRestProtocolRequest {
  var appResource: String = null
  var mainClass: String = null
  var appArgs: Array[String] = null
  var sparkProperties: Map[String, String] = null
  var environmentVariables: Map[String, String] = null

  protected override def doValidate(): Unit = {
    super.doValidate()
    assert(sparkProperties != null, "No Spark properties set!")
    assertFieldIsSet(appResource, "appResource")
    assertFieldIsSet(appArgs, "appArgs")
    assertFieldIsSet(environmentVariables, "environmentVariables")
    assertPropertyIsSet("spark.app.name")
    assertPropertyIsBoolean("spark.driver.supervise")
    assertPropertyIsNumeric("spark.driver.cores")
    assertPropertyIsNumeric("spark.cores.max")
    assertPropertyIsMemory("spark.driver.memory")
    assertPropertyIsMemory("spark.executor.memory")
  }

  private def assertPropertyIsSet(key: String): Unit =
    assertFieldIsSet(sparkProperties.getOrElse(key, null), key)

  private def assertPropertyIsBoolean(key: String): Unit =
    assertProperty[Boolean](key, "boolean", _.toBoolean)

  private def assertPropertyIsNumeric(key: String): Unit =
    assertProperty[Double](key, "numeric", _.toDouble)

  private def assertPropertyIsMemory(key: String): Unit =
    assertProperty[Int](key, "memory", Utils.memoryStringToMb)

  /** Assert that a Spark property can be converted to a certain type. */
  private def assertProperty[T](key: String, valueType: String, convert: (String => T)): Unit = {
    sparkProperties.get(key).foreach { value =>
      Try(convert(value)).getOrElse {
        throw new SubmitRestProtocolException(
          s"Property '$key' expected $valueType value: actual was '$value'.")
      }
    }
  }
}
