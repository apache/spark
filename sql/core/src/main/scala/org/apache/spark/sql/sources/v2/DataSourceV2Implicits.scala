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

package org.apache.spark.sql.sources.v2

import java.util.UUID

import scala.collection.JavaConverters._

import org.apache.spark.sql.{AnalysisException, SaveMode}
import org.apache.spark.sql.catalog.v2.{CaseInsensitiveStringMap, CatalogProvider, Table, TableCatalog}
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.sources.v2.reader.DataSourceReader
import org.apache.spark.sql.sources.v2.writer.DataSourceWriter
import org.apache.spark.sql.types.StructType

/**
 * Implicit helper classes to make working with the v2 API in Scala easier.
 */
private[sql] object DataSourceV2Implicits {
  implicit class CatalogHelper(catalog: CatalogProvider) {
    def asTableCatalog: TableCatalog = catalog match {
      case tableCatalog: TableCatalog =>
        tableCatalog
      case _ =>
        throw new UnsupportedOperationException(s"Catalog $catalog does not support tables")
    }
  }

  implicit class TableHelper(table: Table) {
    def asReadSupport: ReadSupport = {
      table match {
        case support: ReadSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Table is not readable: $table")
      }
    }

    def asWriteSupport: WriteSupport = {
      table match {
        case support: WriteSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Table is not writable: $table")
      }
    }

    def createReader(
        options: Map[String, String],
        userSpecifiedSchema: Option[StructType]): DataSourceReader = {
      userSpecifiedSchema match {
        case Some(schema) =>
          asReadSupport.createReader(schema, options.asDataSourceOptions)
        case None =>
          asReadSupport.createReader(options.asDataSourceOptions)
      }
    }

    def createWriter(
        options: Map[String, String],
        schema: StructType): DataSourceWriter = {
      asWriteSupport.createWriter(
        UUID.randomUUID.toString, schema, SaveMode.Append, options.asDataSourceOptions).get
    }
  }

  implicit class SourceHelper(source: DataSourceV2) {
    def asReadSupport: ReadSupport = {
      source match {
        case support: ReadSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Data source is not readable: $name")
      }
    }

    def asWriteSupport: WriteSupport = {
      source match {
        case support: WriteSupport =>
          support
        case _ =>
          throw new AnalysisException(s"Data source is not writable: $name")
      }
    }

    def name: String = {
      source match {
        case registered: DataSourceRegister =>
          registered.shortName()
        case _ =>
          source.getClass.getSimpleName
      }
    }

    def createReader(
        options: Map[String, String],
        userSpecifiedSchema: Option[StructType]): DataSourceReader = {
      userSpecifiedSchema match {
        case Some(schema) =>
          asReadSupport.createReader(schema, options.asDataSourceOptions)
        case None =>
          asReadSupport.createReader(options.asDataSourceOptions)
      }
    }

    def createWriter(
        options: Map[String, String],
        schema: StructType): DataSourceWriter = {
      val v2Options = new DataSourceOptions(options.asJava)
      asWriteSupport.createWriter(UUID.randomUUID.toString, schema, SaveMode.Append, v2Options).get
    }
  }

  implicit class OptionsHelper(options: Map[String, String]) {
    def asDataSourceOptions: DataSourceOptions = {
      new DataSourceOptions(options.asJava)
    }

    def asCaseInsensitiveMap: CaseInsensitiveStringMap = {
      val map = CaseInsensitiveStringMap.empty()
      map.putAll(options.asJava)
      map
    }

    def table: Option[TableIdentifier] = {
      val map = asCaseInsensitiveMap
      Option(map.get(DataSourceOptions.TABLE_KEY))
          .map(TableIdentifier(_, Option(map.get(DataSourceOptions.DATABASE_KEY))))
    }

    def paths: Array[String] = {
      asDataSourceOptions.paths()
    }
  }
}
