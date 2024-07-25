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

package org.apache.spark.sql.execution.streaming.state

import java.io.StringReader

import scala.util.Try

import org.apache.hadoop.fs.{FSDataInputStream, FSDataOutputStream}
import org.json4s.DefaultFormats
import org.json4s.jackson.JsonMethods

import org.apache.spark.sql.execution.streaming.MetadataVersionUtil
import org.apache.spark.sql.types.StructType
import org.apache.spark.util.Utils

/**
 * Helper classes for reading/writing state schema.
 */
case class StateSchemaFormatV3(
    stateStoreColFamilySchema: List[StateStoreColFamilySchema]
)

object SchemaHelper {

  sealed trait SchemaReader {
    def version: Int

    def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema]

    protected def readJsonSchema(inputStream: FSDataInputStream): String = {
      val buf = new StringBuilder
      val numChunks = inputStream.readInt()
      (0 until numChunks).foreach(_ => buf.append(inputStream.readUTF()))
      buf.toString()
    }
  }

  object SchemaReader {
    def createSchemaReader(versionStr: String): SchemaReader = {
      val version = MetadataVersionUtil.validateVersion(versionStr,
        3)
      version match {
        case 1 => new SchemaV1Reader
        case 2 => new SchemaV2Reader
        case 3 => new SchemaV3Reader
      }
    }
  }

  class SchemaV1Reader extends SchemaReader {
    override def version: Int = 1

    override def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
      val keySchemaStr = inputStream.readUTF()
      val valueSchemaStr = inputStream.readUTF()
      List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
        StructType.fromString(keySchemaStr),
        StructType.fromString(valueSchemaStr)))
    }
  }

  class SchemaV2Reader extends SchemaReader {
    override def version: Int = 2

    override def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
      val keySchemaStr = readJsonSchema(inputStream)
      val valueSchemaStr = readJsonSchema(inputStream)

      List(StateStoreColFamilySchema(StateStore.DEFAULT_COL_FAMILY_NAME,
        StructType.fromString(keySchemaStr),
        StructType.fromString(valueSchemaStr)))
    }
  }

  class SchemaV3Reader extends SchemaReader {
    override def version: Int = 3

    override def read(inputStream: FSDataInputStream): List[StateStoreColFamilySchema] = {
      implicit val formats: DefaultFormats.type = DefaultFormats
      val numEntries = inputStream.readInt()
      (0 until numEntries).map { _ =>
        // read the col family name and the key and value schema
        val colFamilyName = inputStream.readUTF()
        val keySchemaStr = readJsonSchema(inputStream)
        val valueSchemaStr = readJsonSchema(inputStream)
        val keySchema = StructType.fromString(keySchemaStr)

        // use the key schema to also populate the encoder spec
        val keyEncoderSpecStr = readJsonSchema(inputStream)
        val colFamilyMap = JsonMethods.parse(keyEncoderSpecStr).extract[Map[String, Any]]
        val encoderSpec = KeyStateEncoderSpec.fromJson(keySchema, colFamilyMap)

        // read the user key encoder spec if provided
        val userKeyEncoderSchemaStr = readJsonSchema(inputStream)
        val userKeyEncoderSchema = Try(StructType.fromString(userKeyEncoderSchemaStr)).toOption

        StateStoreColFamilySchema(colFamilyName,
          keySchema,
          StructType.fromString(valueSchemaStr),
          Some(encoderSpec),
          userKeyEncoderSchema)
      }.toList
    }
  }

  trait SchemaWriter {
    // 2^16 - 1 bytes
    final val MAX_UTF_CHUNK_SIZE = 65535

    def version: Int

    final def write(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      writeVersion(outputStream)
      writeSchema(stateStoreColFamilySchema, outputStream)
    }

    private def writeVersion(outputStream: FSDataOutputStream): Unit = {
      outputStream.writeUTF(s"v${version}")
    }

    protected def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit

    protected def writeJsonSchema(
        outputStream: FSDataOutputStream,
        jsonSchema: String): Unit = {
      // DataOutputStream.writeUTF can't write a string at once
      // if the size exceeds 65535 (2^16 - 1) bytes.
      // So a key as well as a value consist of multiple chunks in schema version 2 and above.
      val buf = new Array[Char](MAX_UTF_CHUNK_SIZE)
      val numChunks = (jsonSchema.length - 1) / MAX_UTF_CHUNK_SIZE + 1
      val stringReader = new StringReader(jsonSchema)
      outputStream.writeInt(numChunks)
      (0 until numChunks).foreach { _ =>
        val numRead = stringReader.read(buf, 0, MAX_UTF_CHUNK_SIZE)
        outputStream.writeUTF(new String(buf, 0, numRead))
      }
    }
  }

  object SchemaWriter {
    def createSchemaWriter(version: Int): SchemaWriter = {
      version match {
        case 1 if Utils.isTesting => new SchemaV1Writer
        case 2 => new SchemaV2Writer
        case 3 => new SchemaV3Writer
      }
    }
  }

  class SchemaV1Writer extends SchemaWriter {
    override def version: Int = 1

    def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      assert(stateStoreColFamilySchema.length == 1)
      val stateSchema = stateStoreColFamilySchema.head
      outputStream.writeUTF(stateSchema.keySchema.json)
      outputStream.writeUTF(stateSchema.valueSchema.json)
    }
  }

  class SchemaV2Writer extends SchemaWriter {
    override def version: Int = 2

    def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      assert(stateStoreColFamilySchema.length == 1)
      val stateSchema = stateStoreColFamilySchema.head

      writeJsonSchema(outputStream, stateSchema.keySchema.json)
      writeJsonSchema(outputStream, stateSchema.valueSchema.json)
    }
  }

  class SchemaV3Writer extends SchemaWriter {
    override def version: Int = 3

    private val emptyJsonStr = """{    }"""

    def writeSchema(
        stateStoreColFamilySchema: List[StateStoreColFamilySchema],
        outputStream: FSDataOutputStream): Unit = {
      outputStream.writeInt(stateStoreColFamilySchema.size)
      stateStoreColFamilySchema.foreach { colFamilySchema =>
        assert(colFamilySchema.keyStateEncoderSpec.isDefined)
        outputStream.writeUTF(colFamilySchema.colFamilyName)
        writeJsonSchema(outputStream, colFamilySchema.keySchema.json)
        writeJsonSchema(outputStream, colFamilySchema.valueSchema.json)
        writeJsonSchema(outputStream, colFamilySchema.keyStateEncoderSpec.get.json)
        // write user key encoder schema if provided and empty json otherwise
        val userKeyEncoderStr = if (colFamilySchema.userKeyEncoderSchema.isDefined) {
          colFamilySchema.userKeyEncoderSchema.get.json
        } else {
          emptyJsonStr
        }
        writeJsonSchema(outputStream, userKeyEncoderStr)
      }
    }
  }
}
