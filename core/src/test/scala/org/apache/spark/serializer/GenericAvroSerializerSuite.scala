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

package org.apache.spark.serializer

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input, Output}
import org.apache.avro.{Schema, SchemaBuilder}
import org.apache.avro.generic.GenericData.{Array => AvroArray, EnumSymbol, Fixed, Record}

import org.apache.spark.{SharedSparkContext, SparkFunSuite}
import org.apache.spark.internal.config.SERIALIZER

class GenericAvroSerializerSuite extends SparkFunSuite with SharedSparkContext {

  override def beforeAll(): Unit = {
    conf.set(SERIALIZER, "org.apache.spark.serializer.KryoSerializer")
    super.beforeAll()
  }

  val recordSchema : Schema = SchemaBuilder
    .record("testRecord").fields()
    .requiredString("data")
    .endRecord()
  val recordDatum = new Record(recordSchema)
  recordDatum.put("data", "test data")

  val arraySchema = SchemaBuilder.array().items().`type`(recordSchema)
  val arrayDatum = new AvroArray[Record](1, arraySchema)
  arrayDatum.add(recordDatum)

  val enumSchema = SchemaBuilder.enumeration("enum").symbols("A", "B")
  val enumDatum = new EnumSymbol(enumSchema, "A")

  val fixedSchema = SchemaBuilder.fixed("fixed").size(4)
  val fixedDatum = new Fixed(fixedSchema, "ABCD".getBytes)

  test("schema compression and decompression") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    assert(recordSchema ===
      genericSer.decompress(ByteBuffer.wrap(genericSer.compress(recordSchema))))
  }

  test("uses schema fingerprint to decrease message size") {
    val genericSerFull = new GenericAvroSerializer[Record](conf.getAvroSchema)

    val output = new Output(new ByteArrayOutputStream())

    val beginningNormalPosition = output.total()
    genericSerFull.serializeDatum(recordDatum, output)
    output.flush()
    val normalLength = output.total - beginningNormalPosition

    conf.registerAvroSchemas(recordSchema)
    val genericSerFinger = new GenericAvroSerializer[Record](conf.getAvroSchema)
    val beginningFingerprintPosition = output.total()
    genericSerFinger.serializeDatum(recordDatum, output)
    val fingerprintLength = output.total - beginningFingerprintPosition

    assert(fingerprintLength < normalLength)
  }

  test("caches previously seen schemas") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    val compressedSchema = genericSer.compress(recordSchema)
    val decompressedSchema = genericSer.decompress(ByteBuffer.wrap(compressedSchema))

    assert(compressedSchema.eq(genericSer.compress(recordSchema)))
    assert(decompressedSchema.eq(genericSer.decompress(ByteBuffer.wrap(compressedSchema))))
  }

  Seq(
    ("Record", recordDatum),
    ("Array", arrayDatum),
    ("EnumSymbol", enumDatum),
    ("Fixed", fixedDatum)
  ).foreach { case (name, datum) =>
    test(s"SPARK-34477: GenericData.$name serialization and deserialization") {
      val genericSer = new GenericAvroSerializer[datum.type](conf.getAvroSchema)

      val outputStream = new ByteArrayOutputStream()
      val output = new Output(outputStream)
      genericSer.serializeDatum(datum, output)
      output.flush()
      output.close()

      val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
      assert(genericSer.deserializeDatum(input) === datum)
    }

    test(s"SPARK-34477: GenericData.$name serialization and deserialization" +
      " through KryoSerializer ") {
      val rdd = sc.parallelize((0 until 10).map(_ => datum), 2)
      assert(rdd.collect() sameElements Array.fill(10)(datum))
    }
  }

  test("SPARK-39775: Disable validate default values when parsing Avro schemas") {
    val avroTypeStruct = s"""
      |{
      |  "type": "record",
      |  "name": "struct",
      |  "fields": [
      |    {"name": "id", "type": "long", "default": null}
      |  ]
      |}
    """.stripMargin
    val schema = new Schema.Parser().setValidateDefaults(false).parse(avroTypeStruct)

    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    assert(schema === genericSer.decompress(ByteBuffer.wrap(genericSer.compress(schema))))
  }
}
