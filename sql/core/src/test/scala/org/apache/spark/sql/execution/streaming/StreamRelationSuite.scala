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

package org.apache.spark.sql.execution.streaming

import org.apache.spark.sql.catalyst.{AliasIdentifier, TableIdentifier}
import org.apache.spark.sql.catalyst.analysis.AnalysisTest
import org.apache.spark.sql.catalyst.expressions.AttributeReference
import org.apache.spark.sql.catalyst.plans.logical.{Project, SubqueryAlias}
import org.apache.spark.sql.execution.datasources.DataSource
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.IntegerType

class StreamRelationSuite extends SharedSparkSession with AnalysisTest {

  test("STREAM with options is correctly propagated to datasource in V1") {
    sql("CREATE TABLE t (id INT) USING PARQUET")
    sql("INSERT INTO t VALUES (1), (2)")

    val actualAnalyzedPlan = sql(
      """
        |SELECT id FROM STREAM t
        |WITH ('readOptionKey'='readOptionValue')""".stripMargin
    ).queryExecution.analyzed

    val catalogTable = spark.sessionState.catalog.getTableMetadata(
      TableIdentifier("t")
    )
    val idAttr = AttributeReference(name = "id", dataType = IntegerType)()

    val expectedAnalyzedPlan = Project(
      projectList = Seq(idAttr),
      child = SubqueryAlias(
        identifier = AliasIdentifier(name = "t", qualifier = Seq("spark_catalog", "default")),
        child = StreamingRelation(
          dataSource = DataSource(
            sparkSession = spark,
            className = catalogTable.provider.get,
            options = Map(
              "readOptionKey" -> "readOptionValue",
              "path" -> catalogTable.location.toString
            ),
            userSpecifiedSchema = Option(catalogTable.schema),
            catalogTable = Option(catalogTable)
          ),
          sourceName = s"FileSource[${catalogTable.location.toString}]",
          output = Seq(idAttr)
        )
      )
    )

    comparePlans(
      expectedAnalyzedPlan,
      actualAnalyzedPlan,
      checkAnalysis = true
    )
  }
}
