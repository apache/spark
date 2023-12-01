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

import scala.jdk.CollectionConverters._

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{Path, PathFilter}
import org.apache.parquet.hadoop.{ParquetFileReader, ParquetWriter}
import org.apache.parquet.hadoop.api.WriteSupport
import org.apache.parquet.hadoop.api.WriteSupport.WriteContext
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
    val hadoopConf = spark.sessionState.newHadoopConf()
    val fsPath = new Path(path)
    val fs = fsPath.getFileSystem(hadoopConf)
    val parquetFiles = fs.listStatus(fsPath, new PathFilter {
      override def accept(path: Path): Boolean = pathFilter(path)
    }).toSeq.asJava

    val footers =
      ParquetFileReader.readAllFootersInParallel(hadoopConf, parquetFiles, true)
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
  implicit class RecordConsumerDSL(consumer: RecordConsumer) {
    def message(f: => Unit): Unit = {
      consumer.startMessage()
      f
      consumer.endMessage()
    }

    def group(f: => Unit): Unit = {
      consumer.startGroup()
      f
      consumer.endGroup()
    }

    def field(name: String, index: Int)(f: => Unit): Unit = {
      consumer.startField(name, index)
      f
      consumer.endField(name, index)
    }
  }

  /**
   * A testing Parquet [[WriteSupport]] implementation used to write manually constructed Parquet
   * records with arbitrary structures.
   */
  private class DirectWriteSupport(schema: MessageType, metadata: Map[String, String])
    extends WriteSupport[RecordConsumer => Unit] {

    private var recordConsumer: RecordConsumer = _

    override def init(configuration: Configuration): WriteContext = {
      new WriteContext(schema, metadata.asJava)
    }

    override def write(recordWriter: RecordConsumer => Unit): Unit = {
      recordWriter.apply(recordConsumer)
    }

    override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
      this.recordConsumer = recordConsumer
    }
  }

  /**
   * Writes arbitrary messages conforming to a given `schema` to a Parquet file located by `path`.
   * Records are produced by `recordWriters`.
   */
  def writeDirect(path: String, schema: String, recordWriters: (RecordConsumer => Unit)*): Unit = {
    writeDirect(path, schema, Map.empty[String, String], recordWriters: _*)
  }

  /**
   * Writes arbitrary messages conforming to a given `schema` to a Parquet file located by `path`
   * with given user-defined key-value `metadata`. Records are produced by `recordWriters`.
   */
  def writeDirect(
      path: String,
      schema: String,
      metadata: Map[String, String],
      recordWriters: (RecordConsumer => Unit)*): Unit = {
    val messageType = MessageTypeParser.parseMessageType(schema)
    val testWriteSupport = new DirectWriteSupport(messageType, metadata)
    /**
     * Provide a builder for constructing a parquet writer - after PARQUET-248 directly constructing
     * the writer is deprecated and should be done through a builder. The default builders include
     * Avro - but for raw Parquet writing we must create our own builder.
     */
    class ParquetWriterBuilder() extends
        ParquetWriter.Builder[RecordConsumer => Unit, ParquetWriterBuilder](new Path(path)) {
      override def getWriteSupport(conf: Configuration) = testWriteSupport
      override def self() = this
    }
    val parquetWriter = new ParquetWriterBuilder().build()
    try recordWriters.foreach(parquetWriter.write) finally parquetWriter.close()
  }
}
