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

package org.apache.spark.sql.hive.execution.command

import org.apache.spark.sql.{Row, SaveMode}
import org.apache.spark.sql.execution.command.v1

class ShowPartitionsSuite extends v1.ShowPartitionsSuiteBase with CommandSuiteBase {
  test("null and empty string as partition values") {
    import testImplicits._
    withSQLConf("hive.exec.dynamic.partition.mode" -> "nonstrict") {
      withTable("t") {
        val df = Seq((0, ""), (1, null)).toDF("a", "part")
        df.write
          .partitionBy("part")
          .format("hive")
          .mode(SaveMode.Overwrite)
          .saveAsTable("t")

        runShowPartitionsSql(
          "SHOW PARTITIONS t",
          Row("part=__HIVE_DEFAULT_PARTITION__") :: Nil)
        checkAnswer(spark.table("t"),
          Row(0, "__HIVE_DEFAULT_PARTITION__") ::
          Row(1, "__HIVE_DEFAULT_PARTITION__") :: Nil)
      }
    }
  }
}
