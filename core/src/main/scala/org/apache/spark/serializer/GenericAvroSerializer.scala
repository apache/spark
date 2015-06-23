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
import java.util.zip.{Inflater, Deflater}

import scala.collection.mutable

import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}
import com.esotericsoftware.kryo.io.{Input => KryoInput, Output => KryoOutput}
import org.apache.avro.generic.{GenericData, GenericRecord}
import org.apache.avro.io._
import org.apache.avro.{Schema, SchemaNormalization}

import org.apache.spark.SparkConf

import GenericAvroSerializer._

object GenericAvroSerializer {
  def avroSchemaKey(implicit fingerprint: Long): String = s"avro.schema.$fingerprint"
}

/**
 * Custom serializer used for generic Avro records. If the user registers the schemas
 * ahead of time, then the schema's fingerprint will be sent with each message instead of the actual
 * schema, as to reduce network IO.
 * Actions like parsing or compressing schemas are computationally expensive so the serializer
 * caches all previously seen values as to reduce the amount of work needed to do.
 */
class GenericAvroSerializer(conf: SparkConf) extends KSerializer[GenericRecord] {

  private val serializer = serialize()
  private val deserializer = deserialize()

  private def confSchema(implicit fingerprint: Long) = conf.getOption(avroSchemaKey)

  /**
   * Used to compress Schemas when they are being sent over the wire.
   * The compression results are memoized to reduce the compression time since the
   * same schema is compressed many times over
   */
  def compressor(): Schema => Array[Byte] = {
    val cache = new mutable.HashMap[Schema, Array[Byte]]()

    def compress(schema: Schema): Array[Byte] = cache.getOrElseUpdate(schema, {
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

    compress
  }

  /**
   * Decompresses the schema into the actual in-memory object. Keeps an internal cache of already
   * seen values so to limit the number of times that decompression has to be done.
   */
  def decompressor(): Array[Byte] => Schema = {
    val cache = new mutable.HashMap[Array[Byte], Schema]()

    def decompress(schemaBytes: Array[Byte]): Schema = cache.getOrElseUpdate(schemaBytes, {
      val inflater = new Inflater()
      inflater.setInput(schemaBytes)
      val outputStream = new ByteArrayOutputStream(schemaBytes.length)
      val tmpBuffer = Array.ofDim[Byte](1024)
      while (!inflater.finished()) {
        val count = inflater.inflate(tmpBuffer)
        outputStream.write(tmpBuffer, 0, count)
      }
      inflater.end()
      outputStream.close()
      new Schema.Parser().parse(new String(outputStream.toByteArray, "UTF-8"))
    })

    decompress
  }

  /**
   * Serializes generic records into byte buffers. It keeps an internal cache of already seen
   * schema as to reduce the amount of required work.
   */
  def serialize(): (GenericRecord, KryoOutput) => Unit = {
    val writerCache = new mutable.HashMap[Schema, DatumWriter[_]]()
    val schemaCache = new mutable.HashMap[Schema, Long]()
    val compress = compressor()

    def serialize[R <: GenericRecord](datum: R, schema: Schema, output: KryoOutput): Unit = {
      val encoder = EncoderFactory.get.binaryEncoder(output, null)
      writerCache.getOrElseUpdate(schema, GenericData.get.createDatumWriter(schema))
                 .asInstanceOf[DatumWriter[R]]
                 .write(datum, encoder)
      encoder.flush()
    }

    def wrapDatum(datum: GenericRecord, output: KryoOutput): Unit = {
      val schema = datum.getSchema
      val fingerprint = schemaCache.getOrElseUpdate(schema, {
        SchemaNormalization.parsingFingerprint64(schema)
      })
      confSchema(fingerprint) match {
        case Some(_) => {
          output.writeBoolean(true)
          output.writeLong(fingerprint)
        }
        case None => {
          output.writeBoolean(false)
          val compressedSchema = compress(schema)
          output.writeInt(compressedSchema.array.length)
          output.writeBytes(compressedSchema.array)
        }
      }
      serialize(datum, schema, output)
    }
    wrapDatum
  }

  /**
   * Deserializes generic records into their in-memory form. There is internal
   * state to keep a cache of already seen schemas and datum readers.
   * @return
   */
  def deserialize(): KryoInput => GenericRecord = {
    val readerCache = new mutable.HashMap[Schema, DatumReader[_]]()
    val schemaCache = new mutable.HashMap[Long, Schema]()
    val decompress = decompressor()

    def deserialize(input: KryoInput, schema: Schema): GenericRecord = {
      val decoder = DecoderFactory.get.directBinaryDecoder(input, null)
      readerCache.getOrElseUpdate(schema, GenericData.get.createDatumReader(schema))
        .asInstanceOf[DatumReader[GenericRecord]]
        .read(null.asInstanceOf[GenericRecord], decoder)
    }

    def unwrapDatum(input: KryoInput): GenericRecord = {
      val schema = {
        if (input.readBoolean()) {
          val fingerprint = input.readLong()
          schemaCache.getOrElseUpdate(fingerprint, {
            confSchema(fingerprint) match {
              case Some(s) => new Schema.Parser().parse(s)
              case None => throw new RuntimeException(s"Unknown fingerprint: $fingerprint")
            }
          })
        } else {
          val length = input.readInt()
          decompress(input.readBytes(length))
        }
      }
      deserialize(input, schema)
    }
    unwrapDatum
  }

  override def write(kryo: Kryo, output: KryoOutput, datum: GenericRecord): Unit =
    serializer(datum, output)

  override def read(kryo: Kryo, input: KryoInput, datumClass: Class[GenericRecord]): GenericRecord =
    deserializer(input)
}

