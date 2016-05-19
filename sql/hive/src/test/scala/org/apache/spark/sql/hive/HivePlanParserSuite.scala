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

package org.apache.spark.sql.hive

import org.apache.hadoop.hive.serde.serdeConstants

import org.apache.spark.sql.catalyst.catalog.{CatalogColumn, CatalogTable, CatalogTableType}
import org.apache.spark.sql.catalyst.expressions.JsonTuple
import org.apache.spark.sql.catalyst.plans.PlanTest
import org.apache.spark.sql.catalyst.plans.logical.Generate
import org.apache.spark.sql.execution.command.{CreateTable, CreateTableAsSelectLogicalPlan, CreateViewCommand}
import org.apache.spark.sql.hive.test.TestHive

class HivePlanParserSuite extends PlanTest {
  val parser = TestHive.sessionState.sqlParser

  private def extractTableDesc(sql: String): (CatalogTable, Boolean) = {
    parser.parsePlan(sql).collect {
      case CreateTable(desc, allowExisting) => (desc, allowExisting)
      case CreateTableAsSelectLogicalPlan(desc, _, allowExisting) => (desc, allowExisting)
      case CreateViewCommand(desc, _, allowExisting, _, _, _) => (desc, allowExisting)
    }.head
  }

  test("Test CTAS #1") {
    val s1 =
      """CREATE EXTERNAL TABLE IF NOT EXISTS mydb.page_view
        |(viewTime INT,
        |userid BIGINT,
        |page_url STRING,
        |referrer_url STRING,
        |ip STRING COMMENT 'IP Address of the User',
        |country STRING COMMENT 'country of origination')
        |COMMENT 'This is the staging page view table'
        |PARTITIONED BY (dt STRING COMMENT 'date type', hour STRING COMMENT 'hour of the day')
        |ROW FORMAT DELIMITED FIELDS TERMINATED BY '\054' STORED AS RCFILE
        |LOCATION '/user/external/page_view'
        |TBLPROPERTIES ('p1'='v1', 'p2'='v2')
        |AS SELECT * FROM src""".stripMargin

    val (desc, exists) = extractTableDesc(s1)
    assert(exists)
    assert(desc.identifier.database == Some("mydb"))
    assert(desc.identifier.table == "page_view")
    assert(desc.tableType == CatalogTableType.EXTERNAL)
    assert(desc.storage.locationUri == Some("/user/external/page_view"))
    assert(desc.schema ==
      CatalogColumn("viewtime", "int") ::
      CatalogColumn("userid", "bigint") ::
      CatalogColumn("page_url", "string") ::
      CatalogColumn("referrer_url", "string") ::
      CatalogColumn("ip", "string", comment = Some("IP Address of the User")) ::
      CatalogColumn("country", "string", comment = Some("country of origination")) ::
      CatalogColumn("dt", "string", comment = Some("date type")) ::
      CatalogColumn("hour", "string", comment = Some("hour of the day")) :: Nil)
    assert(desc.comment == Some("This is the staging page view table"))
    // TODO will be SQLText
    assert(desc.viewText.isEmpty)
    assert(desc.viewOriginalText.isEmpty)
    assert(desc.partitionColumns ==
      CatalogColumn("dt", "string", comment = Some("date type")) ::
      CatalogColumn("hour", "string", comment = Some("hour of the day")) :: Nil)
    assert(desc.storage.serdeProperties ==
      Map((serdeConstants.SERIALIZATION_FORMAT, "\u002C"), (serdeConstants.FIELD_DELIM, "\u002C")))
    assert(desc.storage.inputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileInputFormat"))
    assert(desc.storage.outputFormat == Some("org.apache.hadoop.hive.ql.io.RCFileOutputFormat"))
    assert(desc.storage.serde ==
      Some("org.apache.hadoop.hive.serde2.columnar.LazyBinaryColumnarSerDe"))
    assert(desc.properties == Map(("p1", "v1"), ("p2", "v2")))
  }

  test("use native json_tuple instead of hive's UDTF in LATERAL VIEW") {
    val analyzer = TestHive.sparkSession.sessionState.analyzer
    val plan = analyzer.execute(parser.parsePlan(
      """
        |SELECT *
        |FROM (SELECT '{"f1": "value1", "f2": 12}' json) test
        |LATERAL VIEW json_tuple(json, 'f1', 'f2') jt AS a, b
      """.stripMargin))

    assert(plan.children.head.asInstanceOf[Generate].generator.isInstanceOf[JsonTuple])
  }

}
