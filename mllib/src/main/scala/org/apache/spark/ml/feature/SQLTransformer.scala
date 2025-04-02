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

import org.apache.spark.annotation.Since
import org.apache.spark.ml.Transformer
import org.apache.spark.ml.param.{Param, ParamMap}
import org.apache.spark.ml.util._
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import org.apache.spark.sql.types.StructType

/**
 * Implements the transformations which are defined by SQL statement.
 * Currently we only support SQL syntax like 'SELECT ... FROM __THIS__ ...'
 * where '__THIS__' represents the underlying table of the input dataset.
 * The select clause specifies the fields, constants, and expressions to display in
 * the output, it can be any select clause that Spark SQL supports. Users can also
 * use Spark SQL built-in function and UDFs to operate on these selected columns.
 * For example, [[SQLTransformer]] supports statements like:
 * {{{
 *  SELECT a, a + b AS a_b FROM __THIS__
 *  SELECT a, SQRT(b) AS b_sqrt FROM __THIS__ where a > 5
 *  SELECT a, b, SUM(c) AS c_sum FROM __THIS__ GROUP BY a, b
 * }}}
 */
@Since("1.6.0")
class SQLTransformer @Since("1.6.0") (@Since("1.6.0") override val uid: String) extends Transformer
  with DefaultParamsWritable {

  @Since("1.6.0")
  def this() = this(Identifiable.randomUID("sql"))

  /**
   * SQL statement parameter. The statement is provided in string form.
   *
   * @group param
   */
  @Since("1.6.0")
  final val statement: Param[String] = new Param[String](this, "statement", "SQL statement")

  /** @group setParam */
  @Since("1.6.0")
  def setStatement(value: String): this.type = set(statement, value)

  /** @group getParam */
  @Since("1.6.0")
  def getStatement: String = $(statement)

  private val tableIdentifier: String = "__THIS__"

  @Since("2.0.0")
  override def transform(dataset: Dataset[_]): DataFrame = {
    transformSchema(dataset.schema, logging = true)
    val tableName = Identifiable.randomUID(uid)
    dataset.createOrReplaceTempView(tableName)
    val realStatement = $(statement).replace(tableIdentifier, tableName)
    val result = dataset.sparkSession.sql(realStatement)
    // Call SessionCatalog.dropTempView to avoid unpersisting the possibly cached dataset.
    dataset.sparkSession.sessionState.catalog.dropTempView(tableName)
    result
  }

  @Since("1.6.0")
  override def transformSchema(schema: StructType): StructType = {
    val spark = SparkSession.builder().getOrCreate()
    val dummyRDD = spark.sparkContext.parallelize(Seq(Row.empty))
    val dummyDF = spark.createDataFrame(dummyRDD, schema)
    val tableName = Identifiable.randomUID(uid)
    val realStatement = $(statement).replace(tableIdentifier, tableName)
    dummyDF.createOrReplaceTempView(tableName)
    val outputSchema = spark.sql(realStatement).schema
    spark.catalog.dropTempView(tableName)
    outputSchema
  }

  @Since("1.6.0")
  override def copy(extra: ParamMap): SQLTransformer = defaultCopy(extra)

  @Since("3.0.0")
  override def toString: String = {
    s"SQLTransformer: uid=$uid" +
      get(statement).map(i => s", statement=$i").getOrElse("")
  }
}

@Since("1.6.0")
object SQLTransformer extends DefaultParamsReadable[SQLTransformer] {

  @Since("1.6.0")
  override def load(path: String): SQLTransformer = super.load(path)
}
