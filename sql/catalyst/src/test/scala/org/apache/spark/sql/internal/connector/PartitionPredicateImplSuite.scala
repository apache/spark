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

package org.apache.spark.sql.internal.connector

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GreaterThan, Literal}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.expressions.PartitionFieldReference
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.unsafe.types.UTF8String

class PartitionPredicateImplSuite extends SparkFunSuite {

  test("Kryo serialization: PartitionPredicateImpl works after round-trip") {
    val conf = new SparkConf()
    val serializer = new KryoSerializer(conf).newInstance()
    checkPartitionPredicateImplAfterSerialization(serializer)
  }

  test("Java serialization: PartitionPredicateImpl works after round-trip") {
    val conf = new SparkConf()
    val serializer = new JavaSerializer(conf).newInstance()
    checkPartitionPredicateImplAfterSerialization(serializer)
  }

  test("Kryo: nested partition path in references survives round-trip") {
    val conf = new SparkConf()
    val serializer = new KryoSerializer(conf).newInstance()
    checkNestedPartitionPathReferencesAfterSerialization(serializer)
  }

  test("Java serialization: nested partition path in references survives round-trip") {
    val conf = new SparkConf()
    val serializer = new JavaSerializer(conf).newInstance()
    checkNestedPartitionPathReferencesAfterSerialization(serializer)
  }

  private def checkPartitionPredicateImplAfterSerialization(
      serializer: SerializerInstance): Unit = {
    val ref = DataTypeUtils.toAttribute(StructField("p", IntegerType, nullable = true))
    val expr = GreaterThan(ref, Literal(5))
    val fields = Seq(PartitionPredicateField(Seq("p"), ref))
    val predicate = PartitionPredicateImpl(expr, fields).get

    val deserialized = serializer.deserialize[PartitionPredicateImpl](
      serializer.serialize(predicate))

    assert(deserialized.eval(InternalRow(10)) === true)
    assert(deserialized.eval(InternalRow(3)) === false)
    assert(deserialized.eval(InternalRow(5)) === false)

    val expectedRefsWithOrdinals = Seq(("p", 0))
    assert(refsWithOrdinals(predicate.references.toSeq) === expectedRefsWithOrdinals)
    assert(refsWithOrdinals(deserialized.references.toSeq) === expectedRefsWithOrdinals)

    assert(deserialized.equals(predicate))
  }

  private def checkNestedPartitionPathReferencesAfterSerialization(
      serializer: SerializerInstance): Unit = {
    val ref = DataTypeUtils.toAttribute(StructField("ts.timezone", StringType, nullable = false))
    val expr = GreaterThan(ref, Literal("x"))
    val fields = Seq(PartitionPredicateField(Seq("ts", "timezone"), ref))
    val predicate = PartitionPredicateImpl(expr, fields).get

    val deserialized = serializer.deserialize[PartitionPredicateImpl](
      serializer.serialize(predicate))

    assert(deserialized.eval(InternalRow(UTF8String.fromString("z"))) === true)
    assert(deserialized.eval(InternalRow(UTF8String.fromString("a"))) === false)

    val expectedRefs = Seq((0, Seq("ts", "timezone")))
    assert(partitionRefDetails(predicate.references.toSeq) === expectedRefs)
    assert(partitionRefDetails(deserialized.references.toSeq) === expectedRefs)

    assert(deserialized.equals(predicate))
  }

  private def partitionRefDetails(refs: Seq[AnyRef]): Seq[(Int, Seq[String])] = refs.map {
    case r: PartitionFieldReference =>
      (r.ordinal(), r.fieldNames().toIndexedSeq)
    case other =>
      fail(s"Expected PartitionFieldReference, got ${other.getClass.getName}: $other")
  }

  private def refsWithOrdinals(refs: Seq[AnyRef]): Seq[(String, Int)] = refs.map {
      case r: PartitionFieldReference =>
        (r.fieldNames().mkString("."), r.ordinal())
      case other =>
        fail(s"Expected PartitionFieldReference, got ${other.getClass.getName}: $other")
    }
}
