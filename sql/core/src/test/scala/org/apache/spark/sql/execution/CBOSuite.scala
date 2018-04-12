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

import org.apache.spark.sql.{QueryTest, SaveMode}
import org.apache.spark.sql.test.SharedSparkSession

class CBOSuite extends QueryTest with SharedSparkSession {

  import testImplicits._

  test("Simple queries must be working, if CBO is turned on") {
    withSQLConf(("spark.sql.cbo.enabled", "true")) {
      withTable("TBL1", "TBL") {
        import org.apache.spark.sql.functions._
        val df = spark.range(1000L).select('id,
          'id * 2 as "FLD1",
          'id * 12 as "FLD2",
          lit("aaa") + 'id as "fld3")
        df.write
          .mode(SaveMode.Overwrite)
          .bucketBy(10, "id", "FLD1", "FLD2")
          .sortBy("id", "FLD1", "FLD2")
          .saveAsTable("TBL")
        spark.sql("ANALYZE TABLE TBL COMPUTE STATISTICS ")
        spark.sql("ANALYZE TABLE TBL COMPUTE STATISTICS FOR COLUMNS ID, FLD1, FLD2, FLD3")
        val df2 = spark.sql(
          """
             SELECT t1.id, t1.fld1, t1.fld2, t1.fld3
             FROM tbl t1
             JOIN tbl t2 on t1.id=t2.id
             WHERE  t1.fld3 IN (-123.23,321.23)
          """.stripMargin)
        df2.createTempView("TBL2")
        val df3 = spark.sql("SELECT * FROM tbl2 WHERE fld3 IN ('qqq', 'qwe')  ")
        assertResult(0, "") {
          df3.count()
        }
      }
    }

  }

}
