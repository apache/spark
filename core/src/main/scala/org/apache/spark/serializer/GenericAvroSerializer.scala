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

import java.io.ByteArrayOutputStream
import java.nio.ByteBuffer
import java.util.zip.{Inflater, Deflater}

import scala.collection.mutable

import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.{Schema, SchemaNormalization}

object GenericAvroSerializer {
  val avroSchemaNamespace = "avro.schema."
  def avroSchemaKey(fingerprint: Long): String = avroSchemaNamespace + fingerprint
}

/**
 * Custom serializer used for generic Avro records. If the user registers the schemas
 * ahead of time, then the schema's fingerprint will be sent with each message instead of the actual
 * schema, as to reduce network IO.
 * Actions like parsing or compressing schemas are computationally expensive so the serializer
 * caches all previously seen values as to reduce the amount of work needed to do.
 */
class GenericAvroSerializer(schemas: Map[Long, String]) extends KSerializer[GenericRecord] {

  /** Used to reduce the amount of effort to compress the schema */
  private val compressCache = new mutable.HashMap[Schema, Array[Byte]]()
  private val decompressCache = new mutable.HashMap[ByteBuffer, Schema]()

  /** Reuses the same datum reader/writer since the same schema will be used many times */
  private val writerCache = new mutable.HashMap[Schema, DatumWriter[_]]()
  private val readerCache = new mutable.HashMap[Schema, DatumReader[_]]()

  /** Fingerprinting is very expensive to this alleviates most of the work */
  private val fingerprintCache = new mutable.HashMap[Schema, Long]()
  private val schemaCache = new mutable.HashMap[Long, Schema]()

  private def getSchema(fingerprint: Long): Option[String] = schemas.get(fingerprint)

  /**
   * Used to compress Schemas when they are being sent over the wire.
   * The compression results are memoized to reduce the compression time since the
   * same schema is compressed many times over
   */
  def compress(schema: Schema): Array[Byte] = compressCache.getOrElseUpdate(schema, {
    val deflater = new Deflater(Deflater.BEST_COMPRESSION)
    val schemaBytes = schema.toString.getBytes("UTF-8")
    deflater.setInput(schemaBytes)
    deflater.finish()
    val buffer = Array.ofDim[Byte](schemaBytes.length)
    val outputStream = new ByteArrayOutputStream(schemaBytes.length)
    while(!deflater.finished()) {
      val count = deflater.deflate(buffer)
      outputStream.write(buffer, 0, count)
    }
    outputStream.close()
    outputStream.toByteArray
  })


  /**
   * Decompresses the schema into the actual in-memory object. Keeps an internal cache of already
   * seen values so to limit the number of times that decompression has to be done.
   */
  def decompress(schemaBytes: ByteBuffer): Schema = decompressCache.getOrElseUpdate(schemaBytes, {
    val inflater = new Inflater()
    val bytes = schemaBytes.array()
    inflater.setInput(bytes)
    val outputStream = new ByteArrayOutputStream(bytes.length)
    val tmpBuffer = Array.ofDim[Byte](1024)
    while (!inflater.finished()) {
      val count = inflater.inflate(tmpBuffer)
      outputStream.write(tmpBuffer, 0, count)
    }
    inflater.end()
    outputStream.close()
    new Schema.Parser().parse(new String(outputStream.toByteArray, "UTF-8"))
  })

  /**
   * Serializes a record to the given output stream. It caches a lot of the internal data as
   * to not redo work
   */
  def serializeDatum[R <: GenericRecord](datum: R, output: KryoOutput): Unit = {
    val encoder = EncoderFactory.get.binaryEncoder(output, null)
    val schema = datum.getSchema
    val fingerprint = fingerprintCache.getOrElseUpdate(schema, {
      SchemaNormalization.parsingFingerprint64(schema)
    })
    getSchema(fingerprint) match {
      case Some(_) => {
        output.writeBoolean(true)
        output.writeLong(fingerprint)
      }
      case None => {
        output.writeBoolean(false)
        val compressedSchema = compress(schema)
        output.writeInt(compressedSchema.length)
        output.writeBytes(compressedSchema)

      }
    }

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
    val schema = {
      if (input.readBoolean()) {
        val fingerprint = input.readLong()
        schemaCache.getOrElseUpdate(fingerprint, {
          getSchema(fingerprint) match {
            case Some(s) => new Schema.Parser().parse(s)
            case None => throw new RuntimeException(s"Unknown fingerprint: $fingerprint")
          }
        })
      } else {
        val length = input.readInt()
        decompress(ByteBuffer.wrap(input.readBytes(length)))
      }
    }
    val decoder = DecoderFactory.get.directBinaryDecoder(input, null)
    readerCache.getOrElseUpdate(schema, GenericData.get.createDatumReader(schema))
               .asInstanceOf[DatumReader[GenericRecord]]
               .read(null.asInstanceOf[GenericRecord], decoder)
  }

  override def write(kryo: Kryo, output: KryoOutput, datum: GenericRecord): Unit =
    serializeDatum(datum, output)

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[GenericRecord]): GenericRecord =
    deserializeDatum(input)
}

