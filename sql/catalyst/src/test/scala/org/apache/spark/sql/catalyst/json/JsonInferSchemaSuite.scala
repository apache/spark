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

package org.apache.spark.sql.catalyst.json

import com.fasterxml.jackson.core.JsonFactory

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.TimestampType

class JsonInferSchemaSuite extends SparkFunSuite{
  def checkTimestampType(pattern: String, json: String): Unit = {
    val options = new JSONOptions(Map("timestampFormat" -> pattern), "GMT", "")
    val inferSchema = new JsonInferSchema(options)
    val factory = new JsonFactory()
    options.setJacksonOptions(factory)
    val parser = CreateJacksonParser.string(factory, json)

    assert(inferSchema.inferField(parser) === TimestampType)
  }

  test("Timestamp field types are inferred correctly via custom data format") {
    checkTimestampType("yyyy-MM-dd'T'HH:mm:ss.SSSXXX", "2018-12-02T21:04:00.123")
  }
}
