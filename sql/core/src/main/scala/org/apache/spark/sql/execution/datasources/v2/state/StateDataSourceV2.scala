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
package org.apache.spark.sql.execution.datasources.v2.state

import java.util
import java.util.Map

import org.apache.spark.sql.{AnalysisException, SparkSession}
import org.apache.spark.sql.connector.catalog.{Table, TableProvider}
import org.apache.spark.sql.connector.expressions.Transform
import org.apache.spark.sql.execution.streaming.state.StateStoreId
import org.apache.spark.sql.sources.DataSourceRegister
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap

class StateDataSourceV2 extends TableProvider with DataSourceRegister {

  import StateDataSourceV2._

  lazy val session = SparkSession.active

  override def shortName(): String = "state"

  override def getTable(
      schema: StructType,
      partitioning: Array[Transform],
      properties: util.Map[String, String]): Table = {
    val checkpointLocation = Option(properties.get(PARAM_CHECKPOINT_LOCATION)).orElse {
      throw new AnalysisException(s"'$PARAM_CHECKPOINT_LOCATION' must be specified.")
    }.get

    val version = Option(properties.get(PARAM_VERSION)).map(_.toInt).orElse {
      throw new AnalysisException(s"'$PARAM_VERSION' must be specified.")
    }.get

    val operatorId = Option(properties.get(PARAM_OPERATOR_ID)).map(_.toInt).orElse {
      throw new AnalysisException(s"'$PARAM_OPERATOR_ID' must be specified.")
    }.get

    val storeName = Option(properties.get(PARAM_STORE_NAME))
      .orElse(Some(StateStoreId.DEFAULT_STORE_NAME)).get

    new StateTable(session, schema, checkpointLocation, version, operatorId, storeName)
  }

  override def inferSchema(options: CaseInsensitiveStringMap): StructType =
    throw new UnsupportedOperationException("Schema should be explicitly specified.")

  override def supportsExternalMetadata(): Boolean = true
}

object StateDataSourceV2 {
  val PARAM_CHECKPOINT_LOCATION = "checkpointLocation"
  val PARAM_VERSION = "version"
  val PARAM_OPERATOR_ID = "operatorId"
  val PARAM_STORE_NAME = "storeName"
}
