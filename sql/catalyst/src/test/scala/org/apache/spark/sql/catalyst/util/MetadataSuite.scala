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

package org.apache.spark.sql.catalyst.util

import org.json4s.jackson.JsonMethods.parse

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.types.{MetadataBuilder, Metadata}

class MetadataSuite extends SparkFunSuite {

  val baseMetadata = new MetadataBuilder()
    .putString("purpose", "ml")
    .putBoolean("isBase", true)
    .build()

  val summary = new MetadataBuilder()
    .putLong("numFeatures", 10L)
    .build()

  val age = new MetadataBuilder()
    .putString("name", "age")
    .putLong("index", 1L)
    .putBoolean("categorical", false)
    .putDouble("average", 45.0)
    .build()

  val gender = new MetadataBuilder()
    .putString("name", "gender")
    .putLong("index", 5)
    .putBoolean("categorical", true)
    .putStringArray("categories", Array("male", "female"))
    .build()

  val metadata = new MetadataBuilder()
    .withMetadata(baseMetadata)
    .putBoolean("isBase", false) // overwrite an existing key
    .putMetadata("summary", summary)
    .putLongArray("long[]", Array(0L, 1L))
    .putDoubleArray("double[]", Array(3.0, 4.0))
    .putBooleanArray("boolean[]", Array(true, false))
    .putMetadataArray("features", Array(age, gender))
    .build()

  test("metadata builder and getters") {
    assert(age.contains("summary") === false)
    assert(age.contains("index") === true)
    assert(age.getLong("index") === 1L)
    assert(age.contains("average") === true)
    assert(age.getDouble("average") === 45.0)
    assert(age.contains("categorical") === true)
    assert(age.getBoolean("categorical") === false)
    assert(age.contains("name") === true)
    assert(age.getString("name") === "age")
    assert(metadata.contains("purpose") === true)
    assert(metadata.getString("purpose") === "ml")
    assert(metadata.contains("isBase") === true)
    assert(metadata.getBoolean("isBase") === false)
    assert(metadata.contains("summary") === true)
    assert(metadata.getMetadata("summary") === summary)
    assert(metadata.contains("long[]") === true)
    assert(metadata.getLongArray("long[]").toSeq === Seq(0L, 1L))
    assert(metadata.contains("double[]") === true)
    assert(metadata.getDoubleArray("double[]").toSeq === Seq(3.0, 4.0))
    assert(metadata.contains("boolean[]") === true)
    assert(metadata.getBooleanArray("boolean[]").toSeq === Seq(true, false))
    assert(gender.contains("categories") === true)
    assert(gender.getStringArray("categories").toSeq === Seq("male", "female"))
    assert(metadata.contains("features") === true)
    assert(metadata.getMetadataArray("features").toSeq === Seq(age, gender))
  }

  test("metadata json conversion") {
    val json = metadata.json
    withClue("toJson must produce a valid JSON string") {
      parse(json)
    }
    val parsed = Metadata.fromJson(json)
    assert(parsed === metadata)
    assert(parsed.## === metadata.##)
  }
}
