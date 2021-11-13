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

package org.apache.spark.sql.execution.command.v2

import org.apache.spark.sql.Row
import org.apache.spark.sql.connector.catalog.SupportsNamespaces
import org.apache.spark.sql.execution.command
import org.apache.spark.util.Utils

/**
 * The class contains tests for the `ALTER NAMESPACE ... SET LOCATION` command to check V2 table
 * catalogs.
 */
class AlterNamespaceSetLocationSuite extends command.AlterNamespaceSetLocationSuiteBase
    with CommandSuiteBase {
  override def notFoundMsgPrefix: String = "Namespace"

  test("basic v2 test") {
    val ns = "ns1.ns2"
    withNamespace(s"$catalog.$ns") {
      sql(s"CREATE NAMESPACE IF NOT EXISTS $catalog.$ns COMMENT " +
        "'test namespace' LOCATION '/tmp/ns_test_1'")
      sql(s"ALTER NAMESPACE $catalog.$ns SET LOCATION '/tmp/ns_test_2'")
      val descriptionDf = sql(s"DESCRIBE NAMESPACE EXTENDED $catalog.$ns")
      assert(descriptionDf.collect() === Seq(
        Row("Namespace Name", "ns2"),
        Row(SupportsNamespaces.PROP_COMMENT.capitalize, "test namespace"),
        Row(SupportsNamespaces.PROP_LOCATION.capitalize, "/tmp/ns_test_2"),
        Row(SupportsNamespaces.PROP_OWNER.capitalize, Utils.getCurrentUserName()),
        Row("Properties", ""))
      )
    }
  }
}
