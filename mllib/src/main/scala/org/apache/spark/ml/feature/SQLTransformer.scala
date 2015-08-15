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

package org.apache.spark.ml.feature

import org.apache.spark.SparkContext
import org.apache.spark.annotation.Experimental
import org.apache.spark.ml.param.{ParamMap, Param}
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.util.Identifiable
import org.apache.spark.sql.{SQLContext, DataFrame, Row}
import org.apache.spark.sql.types.StructType

/**
 * :: Experimental ::
 * Implements the transforms which are defined by SQL statement.
 * Currently we only support SQL syntax like 'SELECT ... FROM __THIS__'
 * where '__THIS__' represents the underlying table of the input dataset.
 */
@Experimental
class SQLTransformer (override val uid: String) extends Transformer {

  def this() = this(Identifiable.randomUID("sql"))

  /**
   * SQL statement parameter. The statement is provided in string form.
   * @group param
   */
  final val statement: Param[String] = new Param[String](this, "statement", "SQL statement")

  /** @group setParam */
  def setStatement(value: String): this.type = set(statement, value)

  /** @group getParam */
  def getStatement: String = $(statement)

  private val tableIdentifier: String = "__THIS__"

  override def transform(dataset: DataFrame): DataFrame = {
    val tableName = Identifiable.randomUID(uid)
    dataset.registerTempTable(tableName)
    val realStatement = $(statement).replace(tableIdentifier, tableName)
    val outputDF = dataset.sqlContext.sql(realStatement)
    outputDF
  }

  override def transformSchema(schema: StructType): StructType = {
    val sc = SparkContext.getOrCreate()
    val sqlContext = SQLContext.getOrCreate(sc)
    val dummyRDD = sc.parallelize(Seq(Row.empty))
    val dummyDF = sqlContext.createDataFrame(dummyRDD, schema)
    dummyDF.registerTempTable(tableIdentifier)
    val outputSchema = sqlContext.sql($(statement)).schema
    outputSchema
  }

  override def copy(extra: ParamMap): SQLTransformer = defaultCopy(extra)
}
