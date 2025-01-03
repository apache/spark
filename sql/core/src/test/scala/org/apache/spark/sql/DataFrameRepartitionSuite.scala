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

import org.apache.spark.sql.functions.{col, lit, spark_partition_id, typedLit, when}
import org.apache.spark.sql.test.SharedSparkSession

class DataFrameRepartitionSuite extends QueryTest with SharedSparkSession {

  test("repartition by MapType") {
    Seq("int", "long", "float", "double", "decimal(10, 2)", "string", "varchar(6)").foreach { dt =>
      val df = spark.range(20)
        .withColumn("c1",
          when(col("id") % 3 === 1, typedLit(Map(1 -> 2)))
            .when(col("id") % 3 === 2, typedLit(Map(1 -> 1, 2 -> 2)))
            .otherwise(typedLit(Map(2 -> 2, 1 -> 1))).cast(s"map<$dt, $dt>"))
        .withColumn("c2", typedLit(Map(1 -> null)).cast(s"map<$dt, $dt>"))
        .withColumn("c3", lit(null).cast(s"map<$dt, $dt>"))

      assertRepartitionNumber(df.repartition(4, col("c1")), 2)
      assertRepartitionNumber(df.repartition(4, col("c2")), 1)
      assertRepartitionNumber(df.repartition(4, col("c3")), 1)
      assertRepartitionNumber(df.repartition(4, col("c1"), col("c2")), 2)
      assertRepartitionNumber(df.repartition(4, col("c1"), col("c3")), 2)
      assertRepartitionNumber(df.repartition(4, col("c1"), col("c2"), col("c3")), 2)
      assertRepartitionNumber(df.repartition(4, col("c2"), col("c3")), 2)
    }
  }

  def assertRepartitionNumber(df: => DataFrame, max: Int): Unit = {
    val dfGrouped = df.groupBy(spark_partition_id()).count()
    // Result number of partition can be lower or equal to max,
    // but no more than that.
    assert(dfGrouped.count() <= max, dfGrouped.queryExecution.simpleString)
  }
}
