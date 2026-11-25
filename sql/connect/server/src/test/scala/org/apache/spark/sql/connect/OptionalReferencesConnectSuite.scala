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

package org.apache.spark.sql.connect

import org.apache.spark.SparkConf
import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.connector.catalog.InMemoryRowLevelOperationTableCatalog
import org.apache.spark.sql.functions.{col, expr}
import org.apache.spark.sql.internal.SQLConf

/** Optional references (`x?`) in the plans that Spark Connect builds. */
class OptionalReferencesConnectSuite extends SparkConnectServerTest {

  override def sparkConf: SparkConf = super.sparkConf
    .set("spark.sql.catalog.cat", classOf[InMemoryRowLevelOperationTableCatalog].getName)

  test("SPARK-61494: MERGE rejects an optional reference in a source relation analyzed early") {
    withSession { session =>
      session.conf.set(SQLConf.OPTIONAL_REFERENCES_ENABLED.key, "true")
      session.sql("CREATE TABLE cat.ns1.target (pk INT NOT NULL, dep STRING)").collect()
      try {
        session.sql("INSERT INTO cat.ns1.target VALUES (1, 'hr')").collect()
        session.sql("CREATE OR REPLACE TEMPORARY VIEW source AS SELECT 1 AS pk").collect()
        // The planner analyzes a `withColumns` relation when it plans it, so the optional
        // reference is already the NULL that it resolved to when the MERGE is analyzed, or a NULL
        // that the rest of that analysis folded, as for an extraction from it.
        Seq("archived?", "attrs?['archived']").foreach { column =>
          val exception = intercept[AnalysisException] {
            session
              .table("source")
              .withColumn("archived", expr(column))
              .mergeInto("cat.ns1.target", col("source.pk") === col("cat.ns1.target.pk"))
              .whenMatched(expr("archived IS NULL"))
              .delete()
              .merge()
          }
          assert(exception.getCondition == "OPTIONAL_REFERENCE_NOT_SUPPORTED.IN_DML")
          assert(exception.getMessageParameters.get("context") == "MERGE")
        }
        assert(session.table("cat.ns1.target").collect().length == 1)
      } finally {
        session.sql("DROP TABLE IF EXISTS cat.ns1.target").collect()
        session.conf.unset(SQLConf.OPTIONAL_REFERENCES_ENABLED.key)
      }
    }
  }
}
