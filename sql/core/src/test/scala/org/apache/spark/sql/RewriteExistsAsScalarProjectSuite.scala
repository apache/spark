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

import org.apache.spark.sql.execution.datasources.v2.DataSourceV2ScanRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class RewriteExistsAsScalarProjectSuite extends QueryTest with SharedSparkSession {
  test("SPARK-50873: Prune column after RewriteSubquery for DSV2") {
    import org.apache.spark.sql.functions._
    withTempPath { dir =>
      spark.range(10)
        .withColumn("userid", col("id") + 1)
        .withColumn("price", col("id") + 2)
        .write
        .mode("overwrite")
        .parquet(dir.getCanonicalPath + "/sales")
      spark.range(5)
        .withColumn("age", col("id") + 1)
        .withColumn("address", col("id") + 2)
        .write.mode("overwrite").parquet(dir.getCanonicalPath + "/customer")
      withSQLConf(SQLConf.USE_V1_SOURCE_LIST.key-> "") {
        spark.read.parquet(dir.getCanonicalPath + "/sales").createOrReplaceTempView("sales")
        spark.read.parquet(dir.getCanonicalPath + "/customer").createOrReplaceTempView("customer")

        val df = sql(
          """
            |select * from sales
            |where exists (select * from customer where sales.userid == customer.id)
            |""".stripMargin)

        withClue(df.queryExecution) {
          val plan = df.queryExecution.optimizedPlan
          val allRelationV2 = plan.collectWithSubqueries { case b: DataSourceV2ScanRelation => b }
          val customerRelation = allRelationV2.find(_.relation.name.endsWith("customer"))
          assert(customerRelation.isDefined, "Customer relation not found in the plan")

          val columns = customerRelation.get.output
          val columnNames = columns.map(_.name)
          assert(columnNames == Seq("id"),
            s"Expected only 'id' column in customer relation, " +
              s"but found: ${columnNames.mkString(", ")}")
        }
      }
    }
  }
}
