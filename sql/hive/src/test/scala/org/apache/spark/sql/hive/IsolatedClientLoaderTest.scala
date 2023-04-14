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

package org.apache.spark.sql.hive

import org.scalatest.funsuite.AnyFunSuite

import org.apache.spark.sql.hive.client._

class IsolatedClientLoaderTest extends AnyFunSuite {
  test("test hiveVersion") {
    val versions = Array("12", "0.12", "0.12.0",
      "13", "0.13", "0.13.0", "0.13.1",
      "14", "0.14", "0.14.0",
      "1.0", "1.0.0", "1.0.1",
      "1.1", "1.1.0", "1.1.1",
      "1.2", "1.2.0", "1.2.1", "1.2.2",
      "1.2", "1.2.0", "1.2.1", "1.2.2", "1.2.1.spark2",
      "2.0", "2.0.0", "2.0.1",
      "2.1", "2.1.0", "2.1.1",
      "2.2", "2.2.0",
      "2.3", "2.3.0", "2.3.1", "2.3.2", "2.3.3", "2.3.4", "2.3.5", "2.3.6", "2.3.7",
      "2.3.8", "2.3.9",
      "3.0", "3.0.0",
      "3.1", "3.1.0", "3.1.1", "3.1.2"
    )
    for (version <- versions) {
      assert(hiveVersion(version) == IsolatedClientLoader.hiveVersion(version))
    }
  }

  def hiveVersion(version: String): HiveVersion = version match {
    case "12" | "0.12" | "0.12.0" => hive.v12
    case "13" | "0.13" | "0.13.0" | "0.13.1" => hive.v13
    case "14" | "0.14" | "0.14.0" => hive.v14
    case "1.0" | "1.0.0" | "1.0.1" => hive.v1_0
    case "1.1" | "1.1.0" | "1.1.1" => hive.v1_1
    case "1.2" | "1.2.0" | "1.2.1" | "1.2.2" => hive.v1_2
    case "1.2" | "1.2.0" | "1.2.1" | "1.2.2" | "1.2.1.spark2" => hive.v1_2
    case "2.0" | "2.0.0" | "2.0.1" => hive.v2_0
    case "2.1" | "2.1.0" | "2.1.1" => hive.v2_1
    case "2.2" | "2.2.0" => hive.v2_2
    case "2.3" | "2.3.0" | "2.3.1" | "2.3.2" | "2.3.3" | "2.3.4" | "2.3.5" | "2.3.6" | "2.3.7"
         | "2.3.8" | "2.3.9" => hive.v2_3
    case "3.0" | "3.0.0" => hive.v3_0
    case "3.1" | "3.1.0" | "3.1.1" | "3.1.2" => hive.v3_1
    case version =>
      throw new UnsupportedOperationException(s"Unsupported Hive Metastore version ($version). " +
        s"Please set ${HiveUtils.HIVE_METASTORE_VERSION.key} with a valid version.")
  }
}
