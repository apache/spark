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

package org.apache.spark.sql.execution.datasources.parquet

import scala.collection.JavaConverters.{collectionAsScalaIterableConverter, mapAsJavaMapConverter, seqAsJavaListConverter}

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.parquet.io.api.RecordConsumer
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.sql.QueryTest

/**
 * Helper class for testing Parquet compatibility.
 */
private[sql] abstract class ParquetCompatibilityTest extends QueryTest with ParquetTest {
  protected def readParquetSchema(path: String): MessageType = {
    readParquetSchema(path, { path => !path.getName.startsWith("_") })
  }

  protected def readParquetSchema(path: String, pathFilter: Path => Boolean): MessageType = {
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(configuration)
    val parquetFiles = fs.listStatus(fsPath, new PathFilter {
      override def accept(path: Path): Boolean = pathFilter(path)
    }).toSeq.asJava

    val footers = ParquetFileReader.readAllFootersInParallel(configuration, parquetFiles, true)
    footers.asScala.head.getParquetMetadata.getFileMetaData.getSchema
  }

  protected def logParquetSchema(path: String): Unit = {
    logInfo(
      s"""Schema of the Parquet file written by parquet-avro:
         |${readParquetSchema(path)}
       """.stripMargin)
  }
}

private[sql] object ParquetCompatibilityTest {
  private class DirectWriteSupport(
      schema: MessageType, writer: RecordConsumer => Unit, metadata: Map[String, String])
    extends WriteSupport[Void] {

    private var recordConsumer: RecordConsumer = _

    override def init(configuration: Configuration): WriteContext = {
      new WriteContext(schema, metadata.asJava)
    }

    override def write(record: Void): Unit = {
      writer(recordConsumer)
    }

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
      this.recordConsumer = recordConsumer
    }
  }

  def writeDirect
      (path: String, schema: String, metadata: Map[String, String] = Map.empty[String, String])
      (writer: RecordConsumer => Unit): Unit = {
    writeDirect(path, MessageTypeParser.parseMessageType(schema), metadata)(writer)
  }

  def writeDirect
      (path: String, schema: MessageType, metadata: Map[String, String])
      (writer: RecordConsumer => Unit): Unit = {
    val writeSupport = new DirectWriteSupport(schema, writer, metadata)
    val parquetWriter = new ParquetWriter[Void](new Path(path), writeSupport)
    try parquetWriter.write(null) finally parquetWriter.close()
  }

  def message(f: => Unit)(implicit consumer: RecordConsumer): Unit = {
    consumer.startMessage()
    f
    consumer.endMessage()
  }

  def group(f: => Unit)(implicit consumer: RecordConsumer): Unit = {
    consumer.startGroup()
    f
    consumer.endGroup()
  }

  def field
      (name: String, index: Int)
      (f: => Unit)
      (implicit consumer: RecordConsumer): Unit = {
    consumer.startField(name, index)
    f
    consumer.endField(name, index)
  }
}
