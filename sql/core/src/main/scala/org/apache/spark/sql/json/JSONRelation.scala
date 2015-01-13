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

package org.apache.spark.sql.json

import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.catalyst.types.StructType
import org.apache.spark.sql.sources._

private[sql] class DefaultSource extends RelationProvider with SchemaRelationProvider {

  /** Returns a new base relation with the parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    val fileName = parameters.getOrElse("path", sys.error("Option 'path' not specified"))
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    JSONRelation(fileName, samplingRatio, None)(sqlContext)
  }

  /** Returns a new base relation with the given schema and parameters. */
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      schema: StructType): BaseRelation = {
    val fileName = parameters.getOrElse("path", sys.error("Option 'path' not specified"))
    val samplingRatio = parameters.get("samplingRatio").map(_.toDouble).getOrElse(1.0)

    JSONRelation(fileName, samplingRatio, Some(schema))(sqlContext)
  }
}

private[sql] case class JSONRelation(
    fileName: String,
    samplingRatio: Double,
    userSpecifiedSchema: Option[StructType])(
    @transient val sqlContext: SQLContext)
  extends TableScan {

  private def baseRDD = sqlContext.sparkContext.textFile(fileName)

  override val schema = userSpecifiedSchema.getOrElse(
    JsonRDD.nullTypeToStringType(
      JsonRDD.inferSchema(
        baseRDD,
        samplingRatio,
        sqlContext.conf.columnNameOfCorruptRecord)))

  override def buildScan() =
    JsonRDD.jsonStringToRow(baseRDD, schema, sqlContext.conf.columnNameOfCorruptRecord)
}
