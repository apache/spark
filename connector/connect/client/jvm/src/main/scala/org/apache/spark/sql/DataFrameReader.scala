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

package org.apache.spark.sql

import java.util.Properties

import scala.jdk.CollectionConverters._

import org.apache.spark.annotation.Stable
import org.apache.spark.connect.proto.Parse.ParseFormat
import org.apache.spark.sql.connect.ConnectConversions._
import org.apache.spark.sql.connect.common.DataTypeProtoConverter
import org.apache.spark.sql.types.StructType

/**
 * Interface used to load a [[Dataset]] from external storage systems (e.g. file systems,
 * key-value stores, etc). Use `SparkSession.read` to access this.
 *
 * @since 3.4.0
 */
@Stable
class DataFrameReader private[sql] (sparkSession: SparkSession) extends api.DataFrameReader {
  type DS[U] = Dataset[U]

  /** @inheritdoc */
  override def format(source: String): this.type = super.format(source)

  /** @inheritdoc */
  override def schema(schema: StructType): this.type = super.schema(schema)

  /** @inheritdoc */
  override def schema(schemaString: String): this.type = super.schema(schemaString)

  /** @inheritdoc */
  override def option(key: String, value: String): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Boolean): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Long): this.type = super.option(key, value)

  /** @inheritdoc */
  override def option(key: String, value: Double): this.type = super.option(key, value)

  /** @inheritdoc */
  override def options(options: scala.collection.Map[String, String]): this.type =
    super.options(options)

  /** @inheritdoc */
  override def options(options: java.util.Map[String, String]): this.type = super.options(options)

  /** @inheritdoc */
  override def load(): DataFrame = load(Nil: _*)

  /** @inheritdoc */
  def load(path: String): DataFrame = load(Seq(path): _*)

  /** @inheritdoc */
  @scala.annotation.varargs
  def load(paths: String*): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val dataSourceBuilder = builder.getReadBuilder.getDataSourceBuilder
      assertSourceFormatSpecified()
      dataSourceBuilder.setFormat(source)
      userSpecifiedSchema.foreach(schema => dataSourceBuilder.setSchema(schema.toDDL))
      extraOptions.foreach { case (k, v) =>
        dataSourceBuilder.putOptions(k, v)
      }
      paths.foreach(path => dataSourceBuilder.addPaths(path))
      builder.build()
    }
  }

  /** @inheritdoc */
  override def jdbc(url: String, table: String, properties: Properties): DataFrame =
    super.jdbc(url, table, properties)

  /** @inheritdoc */
  override def jdbc(
      url: String,
      table: String,
      columnName: String,
      lowerBound: Long,
      upperBound: Long,
      numPartitions: Int,
      connectionProperties: Properties): DataFrame =
    super.jdbc(
      url,
      table,
      columnName,
      lowerBound,
      upperBound,
      numPartitions,
      connectionProperties)

  /** @inheritdoc */
  def jdbc(
      url: String,
      table: String,
      predicates: Array[String],
      connectionProperties: Properties): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val dataSourceBuilder = builder.getReadBuilder.getDataSourceBuilder
      format("jdbc")
      dataSourceBuilder.setFormat(source)
      predicates.foreach(predicate => dataSourceBuilder.addPredicates(predicate))
      this.extraOptions ++= Seq("url" -> url, "dbtable" -> table)
      val params = extraOptions ++ connectionProperties.asScala
      params.foreach { case (k, v) =>
        dataSourceBuilder.putOptions(k, v)
      }
      builder.build()
    }
  }

  /** @inheritdoc */
  override def json(path: String): DataFrame = super.json(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def json(paths: String*): DataFrame = super.json(paths: _*)

  /** @inheritdoc */
  def json(jsonDataset: Dataset[String]): DataFrame =
    parse(jsonDataset, ParseFormat.PARSE_FORMAT_JSON)

  /** @inheritdoc */
  override def csv(path: String): DataFrame = super.csv(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def csv(paths: String*): DataFrame = super.csv(paths: _*)

  /** @inheritdoc */
  def csv(csvDataset: Dataset[String]): DataFrame =
    parse(csvDataset, ParseFormat.PARSE_FORMAT_CSV)

  /** @inheritdoc */
  override def xml(path: String): DataFrame = super.xml(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def xml(paths: String*): DataFrame = super.xml(paths: _*)

  /** @inheritdoc */
  def xml(xmlDataset: Dataset[String]): DataFrame =
    parse(xmlDataset, ParseFormat.PARSE_FORMAT_UNSPECIFIED)

  /** @inheritdoc */
  override def parquet(path: String): DataFrame = super.parquet(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def parquet(paths: String*): DataFrame = super.parquet(paths: _*)

  /** @inheritdoc */
  override def orc(path: String): DataFrame = super.orc(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def orc(paths: String*): DataFrame = super.orc(paths: _*)

  /** @inheritdoc */
  def table(tableName: String): DataFrame = {
    assertNoSpecifiedSchema("table")
    sparkSession.newDataFrame { builder =>
      builder.getReadBuilder.getNamedTableBuilder
        .setUnparsedIdentifier(tableName)
        .putAllOptions(extraOptions.toMap.asJava)
    }
  }

  /** @inheritdoc */
  override def text(path: String): DataFrame = super.text(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def text(paths: String*): DataFrame = super.text(paths: _*)

  /** @inheritdoc */
  override def textFile(path: String): Dataset[String] = super.textFile(path)

  /** @inheritdoc */
  @scala.annotation.varargs
  override def textFile(paths: String*): Dataset[String] = super.textFile(paths: _*)

  private def assertSourceFormatSpecified(): Unit = {
    if (source == null) {
      throw new IllegalArgumentException("The source format must be specified.")
    }
  }

  private def parse(ds: Dataset[String], format: ParseFormat): DataFrame = {
    sparkSession.newDataFrame { builder =>
      val parseBuilder = builder.getParseBuilder
        .setInput(ds.plan.getRoot)
        .setFormat(format)
      userSpecifiedSchema.foreach(schema =>
        parseBuilder.setSchema(DataTypeProtoConverter.toConnectProtoType(schema)))
      extraOptions.foreach { case (k, v) =>
        parseBuilder.putOptions(k, v)
      }
    }
  }
}
