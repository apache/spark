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

package org.apache.spark.sql.execution.datasources

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.plans.logical.Project
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{LongType, StringType, StructType}

class FileResolverSuite extends QueryTest with SharedSparkSession {
  private val tableSchema = new StructType().add("id", LongType)
  private val csvTableSchema = new StructType().add("_c0", StringType)

  test("JSON file format") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.json(f.getCanonicalPath)
      checkResolveOperator(
        sqlText = s"select id from json.`${f.getCanonicalPath}`",
        expectedTablePath = s"file:${f.getCanonicalPath}",
        expectedTableSchema = tableSchema
      )
    })
  }

  test("PARQUET file format") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.parquet(f.getCanonicalPath)
      checkResolveOperator(
        sqlText = s"select id from parquet.`${f.getCanonicalPath}`",
        expectedTablePath = s"file:${f.getCanonicalPath}",
        expectedTableSchema = tableSchema
      )
    })
  }

  test("ORC file format") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.orc(f.getCanonicalPath)
      checkResolveOperator(
        sqlText = s"select id from ORC.`${f.getCanonicalPath}`",
        expectedTablePath = s"file:${f.getCanonicalPath}",
        expectedTableSchema = tableSchema
      )
    })
  }

  test("CSV file format") {
    val df = spark.range(100).toDF()
    withTempPath(f => {
      df.write.csv(f.getCanonicalPath)
      checkResolveOperator(
        sqlText = s"select _c0 from csv.`${f.getCanonicalPath}`",
        expectedTablePath = s"file:${f.getCanonicalPath}",
        expectedTableSchema = csvTableSchema
      )
    })
  }

  private def checkResolveOperator(
      sqlText: String,
      expectedTablePath: String,
      expectedTableSchema: StructType) = {
    val fileResolver = new FileResolver(spark)

    val unresolvedPlan = spark.sql(sqlText).queryExecution.logical

    val result = fileResolver.resolveOperator(
      unresolvedPlan.asInstanceOf[Project].child.asInstanceOf[UnresolvedRelation]
    )

    val logicalRelation = result.asInstanceOf[LogicalRelation]
    assert(
      logicalRelation.relation.asInstanceOf[HadoopFsRelation].location.rootPaths.mkString(",") ==
      expectedTablePath
    )
    assert(logicalRelation.relation.schema == expectedTableSchema)
  }
}
