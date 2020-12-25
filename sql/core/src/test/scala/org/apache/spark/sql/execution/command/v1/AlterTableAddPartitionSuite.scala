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

package org.apache.spark.sql.execution.command.v1

import org.apache.spark.sql.AnalysisException
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.command

trait AlterTableAddPartitionSuiteBase extends command.AlterTableAddPartitionSuiteBase {
  override protected def checkLocation(
      t: String,
      spec: TablePartitionSpec,
      expected: String): Unit = {
    val tablePath = t.split('.')
    val tableName = tablePath.last
    val ns = tablePath.init.mkString(".")
    val partSpec = spec.map { case (key, value) => s"$key = $value"}.mkString(", ")
    val information = sql(s"SHOW TABLE EXTENDED IN $ns LIKE '$tableName' PARTITION($partSpec)")
      .select("information")
      .first().getString(0)
    val location = information.split("\\r?\\n").filter(_.startsWith("Location:")).head
    assert(location.endsWith(expected))
  }

  test("empty string as partition value") {
    withNamespaceAndTable("ns", "tbl") { t =>
      sql(s"CREATE TABLE $t (col1 INT, p1 STRING) $defaultUsing PARTITIONED BY (p1)")
      val errMsg = intercept[AnalysisException] {
        sql(s"ALTER TABLE $t ADD PARTITION (p1 = '')")
      }.getMessage
      assert(errMsg.contains("Partition spec is invalid. " +
        "The spec ([p1=]) contains an empty partition column value"))
    }
  }
}

class AlterTableAddPartitionSuite extends AlterTableAddPartitionSuiteBase with CommandSuiteBase
