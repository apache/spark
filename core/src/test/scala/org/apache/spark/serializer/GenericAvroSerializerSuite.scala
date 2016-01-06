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

import org.apache.spark.serializer.GenericAvroSerializerSuite._
import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Output, Input}
import org.apache.avro.generic.GenericRecord
import org.apache.avro.{SchemaBuilder, Schema}
import org.apache.avro.generic.GenericData.Record
import org.apache.spark.serializer.avro.{GenericAvroSerializer, SchemaRepo}

import org.apache.spark.{SparkConf, SparkFunSuite, SharedSparkContext}

class GenericAvroSerializerSuite extends SparkFunSuite with SharedSparkContext {
  conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")


  test("schema compression and decompression") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    assert(schema === genericSer.decompress(ByteBuffer.wrap(genericSer.compress(schema))))
  }

  test("record serialization and deserialization") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSer.serializeDatum(record, output)
    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSer.deserializeDatum(input) === record)
  }

  test("uses schema fingerprint to decrease message size") {
    val genericSerFull = new GenericAvroSerializer(conf.getAvroSchema)

    val output = new Output(new ByteArrayOutputStream())

    val beginningNormalPosition = output.total()
    genericSerFull.serializeDatum(record, output)
    output.flush()
    val normalLength = output.total - beginningNormalPosition

    conf.registerAvroSchemas(schema)
    val genericSerFinger = new GenericAvroSerializer(conf.getAvroSchema)
    val beginningFingerprintPosition = output.total()
    genericSerFinger.serializeDatum(record, output)
    val fingerprintLength = output.total - beginningFingerprintPosition

    assert(fingerprintLength < normalLength)
  }

  test("caches previously seen schemas") {
    val genericSer = new GenericAvroSerializer(conf.getAvroSchema)
    val compressedSchema = genericSer.compress(schema)
    val decompressedSchema = genericSer.decompress(ByteBuffer.wrap(compressedSchema))

    assert(compressedSchema.eq(genericSer.compress(schema)))
    assert(decompressedSchema.eq(genericSer.decompress(ByteBuffer.wrap(compressedSchema))))
  }

  test("found in schema repository") {
    val schemaRepo = new TestSchemaRepo(conf)
    val genericSerFingerWithRepo = new GenericAvroSerializer(conf.getAvroSchema, schemaRepo)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSerFingerWithRepo.serializeDatum(record, output)

    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSerFingerWithRepo.deserializeDatum(input) === record)

  }

  test("extracted schemaId which is missing from schema repository") {
    val schemaRepo = new TestSchemaRepo(conf)
    val genericSerFingerWithRepo = new GenericAvroSerializer(conf.getAvroSchema, schemaRepo)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSerFingerWithRepo.serializeDatum(record2, output)

    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSerFingerWithRepo.deserializeDatum(input) === record2)
  }

  test("no schemaId extracted from record") {
    val schemaRepo = new TestSchemaRepo(conf)
    val genericSerFingerWithRepo = new GenericAvroSerializer(conf.getAvroSchema, schemaRepo)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSerFingerWithRepo.serializeDatum(record3, output)

    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSerFingerWithRepo.deserializeDatum(input) === record3)
  }

  test("registered schemas takes precedence over schema repository") {
    conf.registerAvroSchemas(schema)
    val schemaRepo = new TestSchemaRepo(conf)
    val genericSerFingerWithRepo = new GenericAvroSerializer(conf.getAvroSchema, schemaRepo)

    val outputStream = new ByteArrayOutputStream()
    val output = new Output(outputStream)
    genericSerFingerWithRepo.serializeDatum(record2, output)

    output.flush()
    output.close()

    val input = new Input(new ByteArrayInputStream(outputStream.toByteArray))
    assert(genericSerFingerWithRepo.deserializeDatum(input) === record2)
  }

  class TestSchemaRepo(conf: SparkConf) extends SchemaRepo(conf) {
    val repo = Map[Long,String](1L -> schema.toString)
    /**
     * Receive from repo an avro schema as string by its ID
     * @param schemaId - the schemaId
     * @return schema if found, none otherwise
     */
    override def getRawSchema(schemaId: Long): Option[String] = {
      repo.get(schemaId)
    }

    /**
     * Extract schemaId from record.
     * @param r current avro record
     * @return schemaId if managed to extract, none otherwise
     */
    override def extractSchemaId(r: GenericRecord): Option[Long] = {
      if(r equals record) Some(1L)
      else if(r equals record2) Some(2L)
      else None
    }

    /**
     * Checks whether the schema repository contains the following schemaId
     * @param schemaId - the schemaId
     * @return true if found in repo, false otherwise.
     */
    override def contains(schemaId: Long): Boolean = repo.contains(schemaId)
  }
}

object GenericAvroSerializerSuite {
  val schema : Schema = SchemaBuilder
    .record("testRecord").fields()
    .requiredString("data")
    .endRecord()
  val record = new Record(schema)
  record.put("data", "test data")

  val record2 = new Record(schema)
  record2.put("data", "test data2")

  val record3 = new Record(schema)
  record3.put("data", "test data3")

}
