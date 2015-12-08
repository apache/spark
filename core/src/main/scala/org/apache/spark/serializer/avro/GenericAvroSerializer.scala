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

package org.apache.spark.serializer.avro

import java.io.{ByteArrayInputStream, ByteArrayOutputStream}
import java.nio.ByteBuffer

import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import org.apache.avro.Schema.Parser
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.{Schema, SchemaNormalization}
import org.apache.commons.io.IOUtils
import org.apache.spark.io.CompressionCodec
import org.apache.spark.{SparkEnv, SparkException}

import scala.collection.mutable

/**
 * Custom serializer used for generic Avro records. If the user registers the schemas
 * ahead of time, then the schema's fingerprint will be sent with each message instead of the actual
 * schema, as to reduce network IO.
 * Actions like parsing or compressing schemas are computationally expensive so the serializer
 * caches all previously seen values as to reduce the amount of work needed to do.
 * @param schemas a map where the keys are unique IDs for Avro schemas and the values are the
 *                string representation of the Avro schema, used to decrease the amount of data
 *                that needs to be serialized.
 */
private[serializer] class GenericAvroSerializer(schemas: Map[Long, String],
                                                schemaRepo: SchemaRepo = EmptySchemaRepo)
  extends KSerializer[GenericRecord] {

  /** Used to reduce the amount of effort to compress the schema */
  private val compressCache = new mutable.HashMap[Schema, Array[Byte]]()
  private val decompressCache = new mutable.HashMap[ByteBuffer, Schema]()

  /** Reuses the same datum reader/writer since the same schema will be used many times */
  private val writerCache = new mutable.HashMap[Schema, DatumWriter[_]]()
  private val readerCache = new mutable.HashMap[Schema, DatumReader[_]]()

  /** Fingerprinting is very expensive so this alleviates most of the work */
  private val fingerprintCache = new mutable.HashMap[Schema, Long]()
  private val schemaCache = new mutable.HashMap[Long, Schema]()

  // GenericAvroSerializer can't take a SparkConf in the constructor b/c then it would become
  // a member of KryoSerializer, which would make KryoSerializer not Serializable.  We make
  // the codec lazy here just b/c in some unit tests, we use a KryoSerializer w/out having
  // the SparkEnv set (note those tests would fail if they tried to serialize avro data).
  private lazy val codec = CompressionCodec.createCodec(SparkEnv.get.conf)

  /**
   * Used to compress Schemas when they are being sent over the wire.
   * The compression results are memoized to reduce the compression time since the
   * same schema is compressed many times over
   */
  def compress(schema: Schema): Array[Byte] = compressCache.getOrElseUpdate(schema, {
    val bos = new ByteArrayOutputStream()
    val out = codec.compressedOutputStream(bos)
    out.write(schema.toString.getBytes("UTF-8"))
    out.close()
    bos.toByteArray
  })

  /**
   * Decompresses the schema into the actual in-memory object. Keeps an internal cache of already
   * seen values so to limit the number of times that decompression has to be done.
   */
  def decompress(schemaBytes: ByteBuffer): Schema = decompressCache.getOrElseUpdate(schemaBytes, {
    val bis = new ByteArrayInputStream(schemaBytes.array())
    val bytes = IOUtils.toByteArray(codec.compressedInputStream(bis))
    new Schema.Parser().parse(new String(bytes, "UTF-8"))
  })

  /**
   * Serializes a record to the given output stream. It caches a lot of the internal data as
   * to not redo work
   */
  def serializeDatum[R <: GenericRecord](datum: R, output: KryoOutput): Unit = {
    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    val schema = datum.getSchema

    serializeSchema(datum, schema, output)

    writerCache.getOrElseUpdate(schema, GenericData.get.createDatumWriter(schema))
      .asInstanceOf[DatumWriter[R]]
      .write(datum, encoder)
    encoder.flush()
  }

  /**
   * Deserializes generic records into their in-memory form. There is internal
   * state to keep a cache of already seen schemas and datum readers.
   */
  def deserializeDatum(input: KryoInput): GenericRecord = {
    val schema: Schema = deserializeSchema(input)

    val decoder = DecoderFactory.get.directBinaryDecoder(input, null)
    readerCache.getOrElseUpdate(schema, GenericData.get.createDatumReader(schema))
      .asInstanceOf[DatumReader[GenericRecord]]
      .read(null, decoder)
  }


  /**
   * Serialize schema
   * Step 1: Calculate the schema's finger print using Avro's SchemaNormilization mechanism.
   * Step 2: Use fingerprint to look for the schema in the pre-registered schemas, if found serialize the fingerprint, else step 3
   * Step 3: Use SchemaRepo to find the schemaId of record, if found serialize the schemaId as fingerprint, else step 4
   * Step 4: Serialize the entire schema with indicator of this behavior.
   * @param datum - datum to extract id from
   * @param schema - schema to serialize
   * @param output - kryo output
   */
  private def serializeSchema[R <: GenericRecord](datum: R, schema: Schema, output: KryoOutput) = {
    val fingerprint = fingerprintCache.getOrElseUpdate(schema, {
      SchemaNormalization.parsingFingerprint64(schema)
    })
    schemas.get(fingerprint) match {
      case Some(_) =>
        output.writeBoolean(true)
        output.writeLong(fingerprint)
      case None =>
        schemaRepo.extractSchemaId(datum) match {
          case Some(schemaId) if schemaRepo.contains(schemaId) =>
            output.writeBoolean(true)
            output.writeLong(schemaId)
          case _ =>
            output.writeBoolean(false)
            val compressedSchema = compress(schema)
            output.writeInt(compressedSchema.length)
            output.writeBytes(compressedSchema)
        }
    }
  }


  /**
   * Deserialize schema
   * If the indicator boolean of finger using is false:
   * 1: Deserialize the schema itself from bytes.
   * If the indicator boolean of fingerprint using is true:
   * Step 1: Search for the schema in the explicitly registered schemas using the fingerprint. If schema was not found, move to step 2.
   * Step 2: Search in the schema repository using the fingerprint. At that point if the schema was not found - throw exception.
   * @param input KryoInput
   * @return the deserialized schema
   */
  private def deserializeSchema(input: KryoInput): Schema = {
    val schema = {
      if (input.readBoolean()) {
        val fingerprint = input.readLong()
        schemaCache.getOrElseUpdate(fingerprint, {
          schemas.get(fingerprint) match {
            case Some(s) => new Parser().parse(s)
            case None =>
              schemaRepo.getSchema(fingerprint) match {
                case Some(res_schema) => res_schema
                case None =>
                  throw new SparkException(
                    s"""Error reading attempting to read avro data --
                       |encountered an unknown fingerprint: $fingerprint, not sure what schema to use.
                       |This could happen if you registered additional schemas after starting your
                       |spark context.""".stripMargin)
              }

          }
        })
      } else {
        val length = input.readInt()
        decompress(ByteBuffer.wrap(input.readBytes(length)))
      }
    }
    schema
  }

  override def write(kryo: Kryo, output: KryoOutput, datum: GenericRecord): Unit =
    serializeDatum(datum, output)

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[GenericRecord]): GenericRecord =
    deserializeDatum(input)
}
