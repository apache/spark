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

package org.apache.spark.sql.classic

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.catalyst.analysis.UnresolvedRelation
import org.apache.spark.sql.catalyst.parser.ParseException
import org.apache.spark.sql.catalyst.plans.logical.MergeIntoTable
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession

class MergeIntoWriterSuite extends QueryTest with SharedSparkSession {
  test("SPARK-54418: mergeInto supports raw path targets") {
    withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
      val writer = spark.range(1).toDF("id")
        .mergeInto("/tmp/target", lit(true))
        .whenNotMatched()
        .insertAll()
        .asInstanceOf[MergeIntoWriter[Row]]
      val command = writer.mergeCommand().asInstanceOf[MergeIntoTable]
      val target = command.targetTable.asInstanceOf[UnresolvedRelation]

      assert(target.multipartIdentifier === Seq("json", "/tmp/target"))
    }
  }

  test("mergeInto supports URI path targets") {
    val path = "abfss://container@account.dfs.core.windows.net/layer/table_name"
    withSQLConf(SQLConf.DEFAULT_DATA_SOURCE_NAME.key -> "json") {
      val writer = spark.range(1).toDF("id")
        .mergeInto(path, lit(true))
        .whenNotMatched()
        .insertAll()
        .asInstanceOf[MergeIntoWriter[Row]]
      val command = writer.mergeCommand().asInstanceOf[MergeIntoTable]
      val target = command.targetTable.asInstanceOf[UnresolvedRelation]

      assert(target.multipartIdentifier === Seq("json", path))
    }
  }

  test("mergeInto keeps explicit SQL-on-file targets unchanged") {
    val writer = spark.range(1).toDF("id")
      .mergeInto("json.`/tmp/target`", lit(true))
      .whenNotMatched()
      .insertAll()
      .asInstanceOf[MergeIntoWriter[Row]]
    val command = writer.mergeCommand().asInstanceOf[MergeIntoTable]
    val target = command.targetTable.asInstanceOf[UnresolvedRelation]

    assert(target.multipartIdentifier === Seq("json", "/tmp/target"))
  }

  test("mergeInto still reports parse errors for invalid non-path targets") {
    intercept[ParseException] {
      spark.range(1).toDF("id").mergeInto("invalid target", lit(true))
    }
  }
}
