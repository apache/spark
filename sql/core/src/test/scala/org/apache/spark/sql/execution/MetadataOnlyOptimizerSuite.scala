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

package org.apache.spark.sql.execution

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.plans.logical.LocalRelation
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSQLContext

class MetadataOnlyOptimizerSuite extends QueryTest with SharedSQLContext {
  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    val data = (1 to 10).map(i => (i, s"data-$i", i % 2, if ((i % 2) == 0) "even" else "odd"))
      .toDF("id", "data", "partId", "part")
    data.write.partitionBy("partId", "part").mode("append").saveAsTable("srcpart_15752")
  }

  override protected def afterAll(): Unit = {
    try {
      sql("DROP TABLE IF EXISTS srcpart_15752")
    } finally {
      super.afterAll()
    }
  }

  private def checkWithMetadataOnly(df: DataFrame): Unit = {
    val localRelations = df.queryExecution.optimizedPlan.collect {
      case l @ LocalRelation(_, _) => l
    }
    assert(localRelations.size == 1)
  }

  private def checkWithoutMetadataOnly(df: DataFrame): Unit = {
    val localRelations = df.queryExecution.optimizedPlan.collect{
      case l @ LocalRelation(_, _) => l
    }
    assert(localRelations.size == 0)
  }

  test("spark-15752 metadata only optimizer for partition table") {
    withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "true") {
      checkWithMetadataOnly(sql("select part from srcpart_15752 where part = 0 group by part"))
      checkWithMetadataOnly(sql("select max(part) from srcpart_15752"))
      checkWithMetadataOnly(sql("select max(part) from srcpart_15752 where part = 0"))
      checkWithMetadataOnly(
        sql("select part, min(partId) from srcpart_15752 where part = 0 group by part"))
      checkWithMetadataOnly(
        sql("select max(x) from (select part + 1 as x from srcpart_15752 where part = 1) t"))
      checkWithMetadataOnly(sql("select distinct part from srcpart_15752"))
      checkWithMetadataOnly(sql("select distinct part, partId from srcpart_15752"))
      checkWithMetadataOnly(
        sql("select distinct x from (select part + 1 as x from srcpart_15752 where part = 0) t"))

      // Now donot support metadata only optimizer
      checkWithoutMetadataOnly(sql("select part, max(id) from srcpart_15752 group by part"))
      checkWithoutMetadataOnly(sql("select distinct part, id from srcpart_15752"))
      checkWithoutMetadataOnly(sql("select part, sum(partId) from srcpart_15752 group by part"))
      checkWithoutMetadataOnly(
        sql("select part from srcpart_15752 where part = 1 group by rollup(part)"))
      checkWithoutMetadataOnly(
        sql("select part from (select part from srcpart_15752 where part = 0 union all " +
          "select part from srcpart_15752 where part= 1)t group by part"))
    }
  }

  test("spark-15752 without metadata only optimizer for partition table") {
    withSQLConf(SQLConf.OPTIMIZER_METADATA_ONLY.key -> "false") {
      checkWithoutMetadataOnly(sql("select part from srcpart_15752 where part = 0 group by part"))
      checkWithoutMetadataOnly(sql("select max(part) from srcpart_15752"))
      checkWithoutMetadataOnly(sql("select max(part) from srcpart_15752 where part = 0"))
      checkWithoutMetadataOnly(
        sql("select max(x) from (select part + 1 as x from srcpart_15752 where part = 1) t"))
      checkWithoutMetadataOnly(sql("select distinct part from srcpart_15752"))
      checkWithoutMetadataOnly(
        sql("select distinct x from (select part + 1 as x from srcpart_15752 where part = 1) t"))
    }
  }
}
