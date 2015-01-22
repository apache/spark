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

package org.apache.spark.sql.hbase

/**
 * Test insert / query against the table created by HBaseMainTest
 */

class HBaseBasicOperationSuite extends HBaseIntegrationTestBase {

  HBaseMainTest.main(null)

  import org.apache.spark.sql.hbase.TestHbase._

  override def afterAll() = {
    if (TestHbase.hbaseAdmin.tableExists("ht0")) {
      TestHbase.hbaseAdmin.disableTable("ht0")
      TestHbase.hbaseAdmin.deleteTable("ht0")
    }
    if (TestHbase.hbaseAdmin.tableExists("ht1")) {
      TestHbase.hbaseAdmin.disableTable("ht1")
      TestHbase.hbaseAdmin.deleteTable("ht1")
    }
    super.afterAll()
  }

  test("Insert Into table0") {
    sql( """CREATE TABLE tb0 (column2 INTEGER, column1 INTEGER, column4 FLOAT,
          column3 SHORT, PRIMARY KEY(column1, column2))
          MAPPED BY (testNamespace.ht0, COLS=[column3=family1.qualifier1,
          column4=family2.qualifier2])"""
    )

    assert(sql( """SELECT * FROM tb0""").count() == 0)
    sql( """INSERT INTO tb0 SELECT col4,col4,col6,col3 FROM ta""")
    assert(sql( """SELECT * FROM tb0""").count() == 14)

    sql( """DROP TABLE tb0""")
  }

  test("Insert Into table 1") {
    sql( """CREATE TABLE tb1 (column1 INTEGER, column2 STRING,
          PRIMARY KEY(column2))
          MAPPED BY (ht1, COLS=[column1=cf.cq])"""
    )

    assert(sql( """SELECT * FROM tb1""").count() == 0)
    sql( """INSERT INTO tb1 VALUES (1024, "abc")""")
    assert(sql( """SELECT * FROM tb1""").count() == 1)

    sql( """DROP TABLE tb1""")
  }

  test("Select test 0") {
    assert(sql( """SELECT * FROM ta""").count() == 14)
  }

  test("Select test 1 (AND, OR)") {
    assert(sql( """SELECT * FROM ta WHERE col7 = 255 OR col7 = 127""").count == 2)
    assert(sql( """SELECT * FROM ta WHERE col7 < 0 AND col4 < -255""").count == 4)
  }

  test("Select test 2 (WHERE)") {
    assert(sql( """SELECT * FROM ta WHERE col7 > 128""").count() == 3)
    assert(sql( """SELECT * FROM ta WHERE (col7 - 10 > 128) AND col1 = ' p255 '""").count() == 1)
  }

  test("Select test 3 (ORDER BY)") {
    val result = sql( """SELECT col1, col7 FROM ta ORDER BY col7 DESC""").collect()
    val sortedResult = result.sortWith(
      (r1, r2) => r1(1).asInstanceOf[Int] > r2(1).asInstanceOf[Int])
    for ((r1, r2) <- result zip sortedResult) {
      assert(r1.equals(r2))
    }
  }

  test("Select test 4 (join)") {
    assert(sql( """SELECT ta.col2 FROM ta join tb on ta.col4=tb.col7""").count == 2)
    assert(sql( """SELECT * FROM ta FULL OUTER JOIN tb WHERE tb.col7 = 1""").count == 14)
    assert(sql( """SELECT * FROM ta LEFT JOIN tb WHERE tb.col7 = 1""").count == 14)
    assert(sql( """SELECT * FROM ta RIGHT JOIN tb WHERE tb.col7 = 1""").count == 14)
  }

  test("Alter Add column and Alter Drop column") {
    assert(sql( """SELECT * FROM ta""").collect()(0).size == 7)
    sql( """ALTER TABLE ta ADD col8 STRING MAPPED BY (col8 = cf1.cf13)""")
    assert(sql( """SELECT * FROM ta""").collect()(0).size == 8)
    sql( """ALTER TABLE ta DROP col8""")
    assert(sql( """SELECT * FROM ta""").collect()(0).size == 7)
  }
}
