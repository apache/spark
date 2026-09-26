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

import org.apache.spark.{SparkConf, SparkException, SparkFunSuite, SparkNumberFormatException}
import org.apache.spark.serializer.{JavaSerializer, KryoSerializer, SerializerInstance}
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{Cast, EqualTo, EvalMode, GreaterThan, Literal}
import org.apache.spark.sql.catalyst.types.DataTypeUtils
import org.apache.spark.sql.connector.expressions.PartitionFieldReference
import org.apache.spark.sql.errors.QueryErrorsBase
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StringType, StructField}
import org.apache.spark.unsafe.types.UTF8String

class PartitionPredicateImplSuite extends SparkFunSuite with QueryErrorsBase {

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

  test("SPARK-59572: eval propagates a failure unless the predicate may keep the partition") {
    val ref = DataTypeUtils.toAttribute(StructField("p", StringType, nullable = true))
    val fields = Seq(PartitionPredicateField(Seq("p"), Some(ref)))
    // An ANSI cast of a non-numeric string throws when evaluated.
    val expr = EqualTo(Cast(ref, IntegerType, None, EvalMode.ANSI), Literal(1))
    val failing = InternalRow(UTF8String.fromString("hr"))
    val matching = InternalRow(UTF8String.fromString("1"))
    val notMatching = InternalRow(UTF8String.fromString("2"))

    // Default: the predicate is Spark's only evaluator, so the failure must surface.
    val strict = PartitionPredicateImpl(expr, fields).get
    checkError(
      exception = intercept[SparkNumberFormatException](strict.eval(failing)),
      condition = "CAST_INVALID_INPUT",
      sqlState = Some("22018"),
      parameters = Map(
        "expression" -> toSQLValue("hr"),
        "sourceType" -> toSQLType(StringType),
        "targetType" -> toSQLType(IntegerType),
        "ansiConfig" -> toSQLConf(SQLConf.ANSI_ENABLED.key)),
      queryContext = Array(ExpectedContext("", -1, -1)))
    assert(strict.eval(matching) === true)
    assert(strict.eval(notMatching) === false)

    // Runtime filters are re-evaluated after the scan, so failing open only prunes less.
    val lenient = PartitionPredicateImpl(expr, fields, keepOnEvalFailure = true).get
    assert(lenient.eval(failing) === true)
    assert(lenient.eval(matching) === true)
    // Keeping on failure must not degrade into always matching: an evaluable key still decides.
    assert(lenient.eval(notMatching) === false)

    // The flag is part of the predicate's identity.
    assert(strict !== lenient)
    assert(strict.hashCode() !== lenient.hashCode())

    // The same split applies to a partition key that does not match the schema.
    val wrongWidth = InternalRow(UTF8String.fromString("1"), UTF8String.fromString("extra"))
    checkError(
      exception = intercept[SparkException](strict.eval(wrongWidth)),
      condition = "INTERNAL_ERROR",
      sqlState = Some("XX000"),
      parameters = Map("message" -> ("Cannot evaluate partition predicate " +
        "(CAST(p AS INT) = 1): partition value field count (2) does not match schema (1).")))
    assert(lenient.eval(wrongWidth) === true)
  }

  test("non-identity partition field: predicate binds by ordinal and never references it") {
    val ref = DataTypeUtils.toAttribute(StructField("p", StringType, nullable = true))
    val fields = Seq(
      PartitionPredicateField(Seq("bucket(4, id)"), None),
      PartitionPredicateField(Seq("p"), Some(ref)))
    val predicate = PartitionPredicateImpl(GreaterThan(ref, Literal("m")), fields).get

    // The partition key carries one value per field; the bucket value at ordinal 0 is skipped.
    assert(predicate.eval(InternalRow(3, UTF8String.fromString("z"))) === true)
    assert(predicate.eval(InternalRow(3, UTF8String.fromString("a"))) === false)
    assert(refsWithOrdinals(predicate.references.toSeq) === Seq(("p", 1)))

    // A filter on the source column of the bucket transform has no field to bind to.
    val id = DataTypeUtils.toAttribute(StructField("id", IntegerType, nullable = true))
    assert(PartitionPredicateImpl(GreaterThan(id, Literal(1)), fields).isEmpty)

    Seq(new JavaSerializer(new SparkConf()), new KryoSerializer(new SparkConf())).foreach { s =>
      val serializer = s.newInstance()
      val deserialized = serializer.deserialize[PartitionPredicateImpl](
        serializer.serialize(predicate))
      assert(deserialized.eval(InternalRow(3, UTF8String.fromString("z"))) === true)
      assert(deserialized.eval(InternalRow(3, UTF8String.fromString("a"))) === false)
      assert(refsWithOrdinals(deserialized.references.toSeq) === Seq(("p", 1)))
      assert(deserialized.equals(predicate))
    }
  }

  private def checkPartitionPredicateImplAfterSerialization(
      serializer: SerializerInstance): Unit = {
    val ref = DataTypeUtils.toAttribute(StructField("p", IntegerType, nullable = true))
    val expr = GreaterThan(ref, Literal(5))
    val fields = Seq(PartitionPredicateField(Seq("p"), Some(ref)))
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
    val fields = Seq(PartitionPredicateField(Seq("ts", "timezone"), Some(ref)))
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
