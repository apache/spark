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

package org.apache.spark.deploy.kubernetes

import org.apache.spark.SparkException

object ConfigurationUtils {
  def parseKeyValuePairs(
    maybeKeyValues: Option[String],
    configKey: String,
    keyValueType: String): Map[String, String] = {

    maybeKeyValues.map(keyValues => {
      keyValues.split(",").map(_.trim).filterNot(_.isEmpty).map(keyValue => {
        keyValue.split("=", 2).toSeq match {
          case Seq(k, v) =>
            (k, v)
          case _ =>
            throw new SparkException(s"Custom $keyValueType set by $configKey must be a" +
              s" comma-separated list of key-value pairs, with format <key>=<value>." +
              s" Got value: $keyValue. All values: $keyValues")
        }
      }).toMap
    }).getOrElse(Map.empty[String, String])
  }
}
