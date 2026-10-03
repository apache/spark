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
package org.apache.spark.sql.catalyst

import java.math.{BigDecimal => JBigDecimal}
import java.time.{Instant, LocalDate}
import java.util.{Arrays, HashSet => JHashSet, Map => JMap}

import test.org.apache.spark.sql.JavaRecordEncoderTestData._

import org.apache.spark.SparkRuntimeException
import org.apache.spark.sql.{Encoder, Encoders}
import org.apache.spark.sql.catalyst.encoders.{encoderFor, ExpressionEncoder}
import org.apache.spark.sql.catalyst.expressions.CodegenObjectFactoryMode
import org.apache.spark.sql.catalyst.plans.CodegenInterpretedPlanTest
import org.apache.spark.sql.catalyst.types.DataTypeUtils.toAttributes
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.unsafe.types.UTF8String
import org.apache.spark.util.SparkSerDeUtils

class JavaRecordEncoderSuite extends CodegenInterpretedPlanTest {

  private def bound[T](encoder: Encoder[T]): ExpressionEncoder[T] =
    encoderFor(encoder).resolveAndBind()

  private def roundTrip[T](encoder: Encoder[T], value: T): T = {
    val enc = bound(encoder)
    enc.createDeserializer()(enc.createSerializer()(value))
  }

  private def checkRoundTrip[T](recordClass: Class[T], values: T*): Unit = {
    values.foreach(value => assert(roundTrip(Encoders.record(recordClass), value) === value))
  }

  test("round trip primitive, boxed and string components") {
    checkRoundTrip(classOf[SimpleRecord],
      new SimpleRecord(1, "a", 1.5),
      new SimpleRecord(Int.MinValue, "", Double.NaN),
      new SimpleRecord(Int.MaxValue, null, Double.NegativeInfinity))
    checkRoundTrip(classOf[BoxedRecord],
      new BoxedRecord(1, Long.MaxValue, -0.5),
      new BoxedRecord(null, null, null))
  }

  test("round trip leaf type components") {
    // Decimals are stored with scale 18 and instants with microsecond precision, so use values
    // that round-trip exactly.
    checkRoundTrip(classOf[LeafTypesRecord],
      new LeafTypesRecord(true, new JBigDecimal("12.345").setScale(18), LocalDate.of(2024, 1, 2),
        Instant.parse("2024-01-02T03:04:05.123456Z"), Color.GREEN),
      new LeafTypesRecord(false, null, null, null, null))
  }

  test("round trip nested record, JavaBean and generic record components") {
    checkRoundTrip(classOf[Person],
      new Person("Bob", 30, new Address("1 Main St", "Springfield")),
      new Person("Ann", 25, null))
    checkRoundTrip(classOf[RecordWithBean],
      new RecordWithBean("r", new SimpleBean("b", 1)),
      new RecordWithBean("r", null))
    checkRoundTrip(classOf[BoxHolder],
      new BoxHolder(new GenericBox("s"), new GenericBox(1),
        Arrays.asList(new GenericBox("t"), null)))
    checkRoundTrip(classOf[BoundedBoxHolder],
      new BoundedBoxHolder(new NumberBox(Integer.valueOf(1)), new ComparableBox("c")))
    checkRoundTrip(classOf[EmptyRecord], new EmptyRecord())

    val bean = new BeanWithRecord
    bean.setAddress(new Address("2 Oak Ave", "Seattle"))
    assert(roundTrip(Encoders.bean(classOf[BeanWithRecord]), bean).getAddress === bean.getAddress)
  }

  test("round trip collection components") {
    val numbers = new JHashSet[Integer](Arrays.asList[Integer](1, 2))
    val addresses = JMap.of("home", new Address("1 Main St", "Springfield"))
    checkRoundTrip(classOf[CollectionRecord],
      new CollectionRecord(Arrays.asList("a", null), numbers,
        Arrays.asList(new Address("s", "c"), null), addresses),
      new CollectionRecord(null, null, null, null))
  }

  // Interpreted mode can't pass object arrays to a constructor (case classes and JavaBeans are
  // affected too), so only test codegen.
  test("round trip array components") {
    val codegenOnly = CodegenObjectFactoryMode.CODEGEN_ONLY.toString
    withSQLConf(SQLConf.CODEGEN_FACTORY_MODE.key -> codegenOnly) {
      val record = new ArrayRecord(Array(1, 2), Array("a", null), Array(new Address("s", "c")))
      val decoded = roundTrip(Encoders.record(classOf[ArrayRecord]), record)
      assert(decoded.ints().toSeq === record.ints().toSeq)
      assert(decoded.strings().toSeq === record.strings().toSeq)
      assert(decoded.addresses().toSeq === record.addresses().toSeq)
    }
  }

  test("null in a @Nonnull reference component fails to decode") {
    // Decode from a schema where every column is nullable, as when reading arbitrary data.
    val encoder = encoderFor(Encoders.record(classOf[NonnullRecord]))
    val attrs = toAttributes(encoder.schema).map(_.withNullability(true))
    val deserializer = encoder.resolveAndBind(attrs).createDeserializer()
    val note = UTF8String.fromString("n")
    assert(deserializer(InternalRow(UTF8String.fromString("a"), 1, null)) ===
      new NonnullRecord("a", 1, null))
    Seq(InternalRow(null, 1, note), InternalRow(UTF8String.fromString("a"), null, note)).foreach {
      row =>
        val e = intercept[SparkRuntimeException](deserializer(row))
        assert(e.getCondition === "NOT_NULL_ASSERT_VIOLATION")
    }
  }

  test("exceptions from the canonical constructor propagate") {
    val deserializer = bound(Encoders.record(classOf[ValidatedRecord])).createDeserializer()
    val e = intercept[Exception](deserializer(InternalRow(UTF8String.fromString("Bob"), -5)))
    val root = Iterator.iterate[Throwable](e)(_.getCause).takeWhile(_ != null).toSeq.last
    assert(root.isInstanceOf[IllegalArgumentException])
    assert(root.getMessage === "Age cannot be negative: -5")
  }

  // Encoders are sent to executors and embedded in Spark Connect UDF payloads.
  test("record encoders are Java serializable") {
    val encoder = Encoders.record(classOf[Person])
    assert(SparkSerDeUtils.deserialize[Encoder[Person]](SparkSerDeUtils.serialize(encoder)) ===
      encoder)
  }
}
