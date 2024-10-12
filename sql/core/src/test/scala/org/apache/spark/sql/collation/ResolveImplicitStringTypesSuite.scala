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


package org.apache.spark.sql.collation

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.util.Utils

class ResolveImplicitStringTypesSuite extends QueryTest with SharedSparkSession {

  def withSessionCollation(collation: String)(f: => Unit): Unit = {
    sql(s"SET COLLATION $collation")
    Utils.tryWithSafeFinally(f) {
      sql(s"SET COLLATION UTF8_BINARY")
    }
  }

  test("initial") {
    withTable("tbl") {
      sql(s"CREATE TABLE tbl (a STRING) USING parquet")
    }
    sql(s"SET COLLATION UTF8_LCASE")
//    checkAnswer(
//      sql(s"SELECT COLLATION('a')"),
//      Seq(Row("UTF8_LCASE"))
//    )

    checkAnswer(
      sql(s"SELECT 1 WHERE 'a' = cast('A' as STRING)"),
      Seq(Row(1))
    )
  }
}
