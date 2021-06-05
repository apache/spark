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

import org.apache.hadoop.fs.FSDataOutputStream

import org.apache.spark.sql.types.StructType

class SchemaV2Writer extends SchemaWriter {

  val version = SchemaV2Writer.VERSION

  def writeSchema(
      keySchema: StructType,
      valueSchema: StructType,
      outputStream: FSDataOutputStream): Unit = {
    val buf = new Array[Char](SchemaV2Writer.MAX_UTF_CHUNK_SIZE)

    // DataOutputStream.writeUTF can't write a string at once
    // if the size exceeds 65535 (2^16 - 1) bytes.
    // So a key as well as a value consist of multiple chunks in schema version 2.
    val keySchemaJson = keySchema.json
    val numKeyChunks =
      (keySchemaJson.length -1) / SchemaV2Writer.MAX_UTF_CHUNK_SIZE + 1
    val keyStringReader = new StringReader(keySchemaJson)
    outputStream.writeInt(numKeyChunks)
    (0 until numKeyChunks).foreach { _ =>
      val numRead =
        keyStringReader.read(buf, 0, SchemaV2Writer.MAX_UTF_CHUNK_SIZE)
      outputStream.writeUTF(new String(buf, 0, numRead))
    }

    val valueSchemaJson = valueSchema.json
    val numValueChunks =
      (valueSchemaJson.length - 1) / SchemaV2Writer.MAX_UTF_CHUNK_SIZE + 1
    val valueStringReader = new StringReader(valueSchemaJson)
    outputStream.writeInt(numValueChunks)
    (0 until numValueChunks).foreach { _ =>
      val numRead =
        valueStringReader.read(buf, 0, SchemaV2Writer.MAX_UTF_CHUNK_SIZE)
      outputStream.writeUTF(new String(buf, 0, numRead))
    }
  }
}

object SchemaV2Writer {
  val VERSION = 2

  // 2^16 - 1 bytes
  val MAX_UTF_CHUNK_SIZE = 65535
}
