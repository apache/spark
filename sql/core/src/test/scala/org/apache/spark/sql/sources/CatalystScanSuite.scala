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

package org.apache.spark.sql.sources

import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.expressions.{Attribute, Expression, PredicateHelper}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types._

class CatalystScanSource extends RelationProvider {
  override def createRelation(
      sqlContext: SQLContext,
      parameters: Map[String, String]): BaseRelation = {
    SimpleCatalystScan(parameters("from").toInt, parameters("to").toInt)(sqlContext.sparkSession)
  }
}

case class SimpleCatalystScan(from: Int, to: Int)(@transient val sparkSession: SparkSession)
  extends BaseRelation
  with CatalystScan {

  override def sqlContext: SQLContext = sparkSession.sqlContext

  override def schema: StructType =
    StructType(StructField("a", LongType, nullable = false) :: Nil)

  // Overriding `unhandledFilters` would be rare in actual implementations but it tests
  // as it is exposed and developers can override this.
  override def unhandledFilters(filters: Array[Filter]): Array[Filter] = {
    // This source does not handle other filters except for `LessThan`.
    filters.filterNot(_.isInstanceOf[LessThan])
  }

  override def buildScan(requiredColumns: Seq[Attribute], filters: Seq[Expression]): RDD[Row] = {
    sqlContext.range(from, to).toDF("a").rdd
  }
}

class CatalystScanSuite extends DataSourceTest with SharedSQLContext with PredicateHelper {
  test("Metadata should keep the pushed filters in PushedCatalystFilters") {
    val df = spark.read
      .format(classOf[CatalystScanSource].getCanonicalName)
      .option("from", 0).option("to", 10).load()
      .filter("cast(a as string) == '1' and a < 3")
    val maybeMetadata = df.queryExecution.executedPlan.collectFirst {
      case p: execution.DataSourceScanExec => p.metadata
    }
    assert(maybeMetadata.isDefined)
    val metadata = maybeMetadata.get
    assert(metadata.contains("PushedCatalystFilters"))
    assert(
      metadata("PushedCatalystFilters").contains("cast") &&
      metadata("PushedCatalystFilters").contains("= 1") &&
      metadata("PushedCatalystFilters").contains("< 3"))
    assert(!metadata.contains("PushedFilters"))
  }
}
