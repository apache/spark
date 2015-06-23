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

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, OutputStream}

import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.avro.generic.GenericData.Record
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.spark.{SparkFunSuite, SharedSparkContext}

class GenericAvroSerializerSuite extends SparkFunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")

  val schema : Schema = SchemaBuilder
    .record("testRecord").fields()
    .requiredString("data")
    .endRecord()
  val record = new Record(schema)
  record.put("data", "test data")

  test("schema compression and decompression") {
    val genericSer = new GenericAvroSerializer(conf)
    val compressor = genericSer.compressor()
    val decompessor = genericSer.decompressor()

    assert(schema === decompessor.compose(compressor)(schema))
  }

  test("record serialization and deserialization") {
    val genericSer = new GenericAvroSerializer(conf)
    val serializer = genericSer.serialize()
    val deserializer = genericSer.deserialize()

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    serializer(record, output)
    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(deserializer(input) === record)
  }

  test("uses schema fingerprint to decrease message size") {
    val genericSer = new GenericAvroSerializer(conf)
    val serializer = genericSer.serialize()
    val output = new Output(new ByteArrayOutputStream())

    val beginningNormalPosition = output.total()
    serializer(record, output)
    output.flush()
    val normalLength = output.total - beginningNormalPosition

    conf.registerAvroSchema(Array(schema))
    val beginningFingerprintPosition = output.total()
    serializer(record, output)
    val fingerprintLength = output.total - beginningFingerprintPosition

    assert(fingerprintLength < normalLength)
  }

  test("caches previously seen schemas") {
    val genericSer = new GenericAvroSerializer(conf)
    val compressor = genericSer.compressor()
    val decompressor = genericSer.decompressor()
    val compressedSchema = compressor(schema)

    assert(compressedSchema.eq(compressor(schema)))
    assert(decompressor(compressedSchema).eq(decompressor(compressedSchema)))
  }
}
