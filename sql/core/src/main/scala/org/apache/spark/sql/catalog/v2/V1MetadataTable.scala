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

package org.apache.spark.sql.catalog.v2

import java.util
import scala.collection.JavaConverters._

import org.apache.spark.sql.SaveMode
import org.apache.spark.sql.catalog.v2.PartitionTransforms.{bucket, identity}
import org.apache.spark.sql.catalyst.catalog.{CatalogTable, CatalogUtils}
import org.apache.spark.sql.sources.v2.{DataSourceOptions, DataSourceV2, ReadSupport, WriteSupport}
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

/**
 * An implementation of catalog v2 [[Table]] to expose v1 table metadata.
 */
private[sql] class V1MetadataTable(
    v1Table: CatalogTable,
    v2Source: Option[DataSourceV2]) extends Table {

  def readDelegate: ReadSupport = v2Source match {
    case r: ReadSupport => r
    case _ => throw new UnsupportedOperationException(s"Source does not support reads: $v2Source")
  }

  def writeDelegate: WriteSupport = v2Source match {
    case w: WriteSupport => w
    case _ => throw new UnsupportedOperationException(s"Source does not support writes: $v2Source")
  }

  lazy val options: Map[String, String] = {
    v1Table.storage.locationUri match {
      case Some(uri) =>
        v1Table.storage.properties + ("path" -> CatalogUtils.URIToString(uri))
      case _ =>
        v1Table.storage.properties
    }
  }

  override lazy val properties: util.Map[String, String] = {
    val allProperties = new util.HashMap[String, String]()

    allProperties.putAll(v1Table.properties.asJava)
    allProperties.putAll(options.asJava)

    allProperties
  }

  override lazy val schema: StructType = {
    v1Table.schema
  }

  override lazy val partitioning: util.List[PartitionTransform] = {
    val partitions = new util.ArrayList[PartitionTransform]()

    v1Table.partitionColumnNames.foreach(col => partitions.add(identity(col)))
    v1Table.bucketSpec.map(spec =>
      partitions.add(bucket(spec.numBuckets, spec.bucketColumnNames: _*)))

    partitions
  }

  def mergeOptions(readOptions: DataSourceOptions): DataSourceOptions = {
    val newMap = new util.HashMap[String, String]()
    newMap.putAll(options.asJava)
    newMap.putAll(readOptions.asMap)
    new DataSourceOptions(newMap)
  }
}

private[sql] trait DelegateReadSupport extends ReadSupport {
  def readDelegate: ReadSupport

  def mergeOptions(options: DataSourceOptions): DataSourceOptions

  override def createReader(options: DataSourceOptions): DataSourceReader = {
    readDelegate.createReader(mergeOptions(options))
  }

  override def createReader(
      schema: StructType,
      options: DataSourceOptions): DataSourceReader = {
    readDelegate.createReader(schema, mergeOptions(options))
  }
}

private[sql] trait DelegateWriteSupport extends WriteSupport {
  def writeDelegate: WriteSupport

  def mergeOptions(options: DataSourceOptions): DataSourceOptions

  override def createWriter(
      writeUUID: String,
      schema: StructType,
      mode: SaveMode,
      options: DataSourceOptions): util.Optional[DataSourceWriter] = {
    assert(mode == SaveMode.Append, "Only append mode is supported")
    writeDelegate.createWriter(writeUUID, schema, mode, mergeOptions(options))
  }
}
