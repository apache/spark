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

package org.apache.spark.sql.connect

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Evolving
import org.apache.spark.connect.proto.Read.DataSource
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.errors.DataTypeErrors
import org.apache.spark.sql.streaming
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a streaming `Dataset` from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.readStream` to access this.
 *
 * @since 3.5.0
 */
@Evolving
final class DataStreamReader private[sql] (sparkSession: SparkSession)
    extends streaming.DataStreamReader {

  private val sourceBuilder = DataSource.newBuilder()

  /** @inheritdoc */
  def format(source: String): this.type = {
    sourceBuilder.setFormat(source)
    this
  }

  /** @inheritdoc */
  def schema(schema: StructType): this.type = {
    if (schema != null) {
      sourceBuilder.setSchema(schema.json) // Use json. DDL does not retail all the attributes.
    }
    this
  }

  /** @inheritdoc */
  override def schema(schemaString: String): this.type = {
    sourceBuilder.setSchema(schemaString)
    this
  }

  /** @inheritdoc */
  def option(key: String, value: String): this.type = {
    sourceBuilder.putOptions(key, value)
    this
  }

  /** @inheritdoc */
  def options(options: scala.collection.Map[String, String]): this.type = {
    this.options(options.asJava)
  }

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type = {
    sourceBuilder.putAllOptions(options)
    this
  }

  /** @inheritdoc */
  def load(): DataFrame = {
    sparkSession.newDataFrame { relationBuilder =>
      relationBuilder.getReadBuilder
        .setIsStreaming(true)
        .setDataSource(sourceBuilder.build())
    }
  }

  /** @inheritdoc */
  def load(path: String): DataFrame = {
    sourceBuilder.clearPaths()
    sourceBuilder.addPaths(path)
    load()
  }

  /** @inheritdoc */
  def table(tableName: String): DataFrame = {
    require(tableName != null, "The table name can't be null")
    sparkSession.newDataFrame { builder =>
      builder.getReadBuilder
        .setIsStreaming(true)
        .getNamedTableBuilder
        .setUnparsedIdentifier(tableName)
        .putAllOptions(sourceBuilder.getOptionsMap)
    }
  }

  override protected def assertNoSpecifiedSchema(operation: String): Unit = {
    if (sourceBuilder.hasSchema) {
      throw DataTypeErrors.userSpecifiedSchemaUnsupportedError(operation)
    }
  }

  ///////////////////////////////////////////////////////////////////////////////////////
  // Covariant overrides.
  ///////////////////////////////////////////////////////////////////////////////////////

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def json(path: String): DataFrame = super.json(path)

  /** @inheritdoc */
  override def csv(path: String): DataFrame = super.csv(path)

  /** @inheritdoc */
  override def xml(path: String): DataFrame = super.xml(path)

  /** @inheritdoc */
  override def orc(path: String): DataFrame = super.orc(path)

  /** @inheritdoc */
  override def parquet(path: String): DataFrame = super.parquet(path)

  /** @inheritdoc */
  override def text(path: String): DataFrame = super.text(path)

  /** @inheritdoc */
  override def textFile(path: String): Dataset[String] = super.textFile(path)

}
