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

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.expressions.{Add, Expression, Literal}

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

  test("Kryo serialization for expressions") {
    val conf = new SparkConf()
    val serializer = new KryoSerializer(conf).newInstance()
    checkMetadataExpressions(serializer)
  }

  test("Java serialization for expressions") {
    val conf = new SparkConf()
    val serializer = new JavaSerializer(conf).newInstance()
    checkMetadataExpressions(serializer)
  }

  test("JSON representation with expressions") {
    val meta = new MetadataBuilder()
      .putString("key", "value")
      .putExpression("expr", "1 + 3", Some(Add(Literal(1), Literal(3))))
      .build()
    assert(meta.json == """{"expr":"1 + 3","key":"value"}""")
  }

  test("equals and hashCode with expressions") {
    val meta1 = new MetadataBuilder()
      .putString("key", "value")
      .putExpression("expr", "1 + 2", Some(Add(Literal(1), Literal(2))))
      .build()

    val meta2 = new MetadataBuilder()
      .putString("key", "value")
      .putExpression("expr", "1 + 2", Some(Add(Literal(1), Literal(2))))
      .build()

    val meta3 = new MetadataBuilder()
      .putString("key", "value")
      .putExpression("expr", "2 + 3", Some(Add(Literal(2), Literal(3))))
      .build()

    val meta4 = new MetadataBuilder()
      .putString("key", "value")
      .putExpression("expr", "1 + 2", None)
      .build()

    // meta1 and meta2 are equivalent
    assert(meta1 === meta2)
    assert(meta1.hashCode === meta2.hashCode)

    // meta1 and meta3 are different as they contain different expressions
    assert(meta1 !== meta3)
    assert(meta1.hashCode !== meta3.hashCode)

    // meta1 and meta4 are equivalent even though meta4 only includes the SQL string
    assert(meta1 == meta4)
    assert(meta1.hashCode == meta4.hashCode)
  }

  private def checkMetadataExpressions(serializer: SerializerInstance): Unit = {
    val meta = new MetadataBuilder()
      .putString("key", "value")
      .putExpression("tempKey", "1", Some(Literal(1)))
      .build()
    assert(meta.contains("key"))
    assert(meta.getString("key") == "value")
    assert(meta.contains("tempKey"))
    assert(meta.getExpression[Expression]("tempKey")._1 == "1")
    assert(meta.getExpression[Expression]("tempKey")._2.contains(Literal(1)))

    val deserializedMeta = serializer.deserialize[Metadata](serializer.serialize(meta))
    assert(deserializedMeta == meta)
    assert(deserializedMeta.hashCode == meta.hashCode)
    assert(deserializedMeta.contains("key"))
    assert(deserializedMeta.getString("key") == "value")
    assert(deserializedMeta.contains("tempKey"))
    assert(deserializedMeta.getExpression[Expression]("tempKey")._1 == "1")
    assert(deserializedMeta.getExpression[Expression]("tempKey")._2.isEmpty)
  }
}
