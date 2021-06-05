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

import org.apache.hadoop.fs.FSDataInputStream

import org.apache.spark.sql.types.StructType

sealed trait SchemaReader {
  final def read(inputStream: FSDataInputStream): (StructType, StructType) =
    readSchema(inputStream)
  protected def readSchema(inputStream: FSDataInputStream): (StructType, StructType)
}

class SchemaV1Reader extends SchemaReader {
  def readSchema(inputStream: FSDataInputStream): (StructType, StructType) = {
    val keySchemaStr = inputStream.readUTF()
    val valueSchemaStr = inputStream.readUTF()
    (StructType.fromString(keySchemaStr), StructType.fromString(valueSchemaStr))
  }
}

class SchemaV2Reader extends SchemaReader {
  def readSchema(inputStream: FSDataInputStream): (StructType, StructType) = {
    val buf = new StringBuilder
    val numKeyChunks = inputStream.readInt()
    (0 until numKeyChunks).foreach(_ => buf.append(inputStream.readUTF()))
    val keySchemaStr = buf.toString()

    buf.clear()
    val numValueChunks = inputStream.readInt()
    (0 until numValueChunks).foreach(_ => buf.append(inputStream.readUTF()))
    val valueSchemaStr = buf.toString()
    (StructType.fromString(keySchemaStr), StructType.fromString(valueSchemaStr))
  }
}
