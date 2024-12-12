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

import org.apache.spark.SparkContext
import org.apache.spark.sql.catalyst.optimizer.{ConvertToLocalRelation, PropagateEmptyRelation}
import org.apache.spark.sql.catalyst.util.resourceToString
import org.apache.spark.sql.connector.catalog.InMemoryTableWithV2FilterCatalog
import org.apache.spark.sql.execution.{LeafExecNode, SparkPlan, UnionExec}
import org.apache.spark.sql.execution.adaptive.{AdaptiveSparkPlanExec, AQEPropagateEmptyRelation, QueryStageExec}
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.TestSparkSession


class TPCDSV2RelationLimitedTest extends QueryTest with TPCDSBase with SQLQueryTestHelper {

  protected override def createSparkSession: TestSparkSession = {
    val session = new TestSparkSession(new SparkContext("local[1]",
      this.getClass.getSimpleName, sparkConf))
    session.conf.set("spark.sql.catalog.testcat", classOf[InMemoryTableWithV2FilterCatalog].getName)
    session.conf.set("spark.sql.defaultCatalog", "testcat")
    session.conf.set(SQLConf.OPTIMIZER_EXCLUDED_RULES.key,
      s"${PropagateEmptyRelation.ruleName},${ConvertToLocalRelation.ruleName}")
    session.conf.set(SQLConf.ADAPTIVE_OPTIMIZER_EXCLUDED_RULES.key,
      s"${PropagateEmptyRelation.ruleName},${ConvertToLocalRelation.ruleName}," +
        s"${AQEPropagateEmptyRelation.ruleName}")

    session
  }

  protected val baseResourcePath = {
    // use the same way as `SQLQueryTestSuite` to get the resource path
    getWorkspaceFilePath("sql", "core", "src", "test", "resources", "test-data", "spark-45866")
      .toFile.getAbsolutePath
  }

  override def createTable(
                            spark: SparkSession,
                            tableName: String,
                            format: String = "parquet",
                            options: Seq[String] = Nil): Unit = {

    // first read the parquet to create dataframe and store the table as InMemory table
    val dfwTemp = spark.read.parquet(s"$baseResourcePath/$tableName").writeTo(tableName)
    val dfw = tablePartitionColumns.get(tableName) match {
      case Some(partitionClause) => val cols = partitionClause.map(Column(_))
        dfwTemp.partitionedBy(cols.head, cols.drop(1): _*)
      case _ => dfwTemp
    }
    dfw.createOrReplace()
  }

 test("q14b") {
   // TODO . Fix the assertion failure in DataSourceV2Relation.computeStats which gets exposed
   // by this test
   System.setProperty("SPARK-45943", "1")
   try {
     def findUnionExecPlans(plan: SparkPlan): Seq[UnionExec] = {
       val unionExecs1 = plan collectWithSubqueries {
         case u: UnionExec => u
       }
       val collectLeavesIncludeHiddenInPlanExprs = plan.collectWithSubqueries {
         case leaf: LeafExecNode => leaf
       }

       // find leaves which can internally contain plan and extract Unions from it too.
       val unionExecs2 = collectLeavesIncludeHiddenInPlanExprs.flatMap(pl => pl match {
         case adp: AdaptiveSparkPlanExec => findUnionExecPlans(adp.finalPhysicalPlan)
         case qs: QueryStageExec => findUnionExecPlans(qs.plan)
         // ignore reused exchange exec etc
         case _ => Seq.empty
       })

       unionExecs2 ++ unionExecs1
     }

     tpcdsQueries.filter(_ == "q14b").foreach { name =>
       val queryString = resourceToString(s"tpcds/$name.sql")
       val df = spark.sql(queryString)
       // execute the query
       df.collect()
       val execPlan = df.queryExecution.executedPlan
       // collect the total UnionExec nodes.
       val allUnions = findUnionExecPlans(execPlan)
       assert(allUnions.size == 1)
     }
   } finally {
     System.clearProperty("SPARK-45943")
   }
 }
}
