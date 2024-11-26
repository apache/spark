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

import scala.jdk.CollectionConverters.PropertiesHasAsScala

import org.apache.spark.annotation.Stable
import org.apache.spark.sql.api.{DataStreamReader, StreamingQueryManager}
import org.apache.spark.sql.catalog.Table
import org.apache.spark.sql.util.ExecutionListenerManager

@Stable
class SQLContext private[sql](sparkSession: SparkSession)
  extends api.SQLContext(sparkSession) {

  /** @inheritdoc */
  override def newSession(): api.SQLContext = {
    sparkSession.newSession().sqlContext
  }

  /** @inheritdoc */
  override def listenerManager: ExecutionListenerManager = {
    sparkSession.listenerManager
  }

  /** @inheritdoc */
  override def setConf(props: Properties): Unit = sparkSession.conf.synchronized {
    props.asScala.foreach { case (k, v) => sparkSession.conf.set(k, v) }
  }

  /** @inheritdoc */
  override def experimental: ExperimentalMethods = {
    sparkSession.experimental
  }

  /** @inheritdoc */
  override def udf: api.UDFRegistration = {
    sparkSession.udf
  }

  /** @inheritdoc */
  override def read: api.DataFrameReader = {
    sparkSession.read
  }

  /** @inheritdoc */
  override def readStream: DataStreamReader = {
    sparkSession.readStream
  }

  /** @inheritdoc */
  override def tables(): api.Dataset[Row] = {
    sparkSession.catalog.listTables()
      .map(ListTableRow.fromTable)(Encoders.product[ListTableRow])
      .toDF()
  }

  /** @inheritdoc */
  override def tables(databaseName: String): api.Dataset[Row] = {
    sparkSession.catalog.listTables(databaseName)
      .map(ListTableRow.fromTable)(Encoders.product[ListTableRow])
      .toDF()
  }

  /** @inheritdoc */
  override def streams: StreamingQueryManager = {
    sparkSession.streams
  }

  /** @inheritdoc */
  override def tableNames(databaseName: String): Array[String] = {
    sparkSession.catalog.listTables(databaseName).map(_.name)(Encoders.STRING).collect()
  }
}

protected sealed case class ListTableRow(database: String, tableName: String, isTemporary: Boolean)

protected object ListTableRow {
  def fromTable(table: Table): ListTableRow = {
    ListTableRow(table.database, table.name, table.isTemporary)
  }
}
