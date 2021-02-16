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

package org.apache.spark.sql.types

import org.apache.spark.SparkFunSuite

class MetadataSuite extends SparkFunSuite {
  test("String Metadata") {
    val meta = new MetadataBuilder().putString("key", "value").build()
    assert(meta === meta)
    assert(meta.## !== 0)
    assert(meta.getString("key") === "value")
    assert(meta.contains("key"))
    assert(meta === Metadata.fromJson(meta.json))
    intercept[NoSuchElementException](meta.getString("no_such_key"))
    intercept[ClassCastException](meta.getBoolean("key"))
  }

  test("Long Metadata") {
    val meta = new MetadataBuilder().putLong("key", 12).build()
    assert(meta === meta)
    assert(meta.## !== 0)
    assert(meta.getLong("key") === 12)
    assert(meta.contains("key"))
    assert(meta === Metadata.fromJson(meta.json))
    intercept[NoSuchElementException](meta.getLong("no_such_key"))
    intercept[ClassCastException](meta.getBoolean("key"))
  }

  test("Double Metadata") {
    val meta = new MetadataBuilder().putDouble("key", 12).build()
    assert(meta === meta)
    assert(meta.## !== 0)
    assert(meta.getDouble("key") === 12)
    assert(meta.contains("key"))
    assert(meta === Metadata.fromJson(meta.json))
    intercept[NoSuchElementException](meta.getDouble("no_such_key"))
    intercept[ClassCastException](meta.getBoolean("key"))
  }

  test("Boolean Metadata") {
    val meta = new MetadataBuilder().putBoolean("key", true).build()
    assert(meta === meta)
    assert(meta.## !== 0)
    assert(meta.getBoolean("key"))
    assert(meta.contains("key"))
    assert(meta === Metadata.fromJson(meta.json))
    intercept[NoSuchElementException](meta.getBoolean("no_such_key"))
    intercept[ClassCastException](meta.getString("key"))
  }

  test("Null Metadata") {
    val meta = new MetadataBuilder().putNull("key").build()
    assert(meta === meta)
    assert(meta.## !== 0)
    assert(meta.getString("key") === null)
    assert(meta.getDouble("key") === 0)
    assert(meta.getLong("key") === 0)
    assert(meta.getBoolean("key") === false)
    assert(meta.contains("key"))
    assert(meta === Metadata.fromJson(meta.json))
    intercept[NoSuchElementException](meta.getLong("no_such_key"))
  }
}
