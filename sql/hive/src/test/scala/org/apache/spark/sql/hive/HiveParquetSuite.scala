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

import org.apache.spark.sql.{QueryTest, Row}
import org.apache.spark.sql.hive.test.HiveParquetTest

case class Cases(lower: String, UPPER: String)

class HiveParquetSuite extends QueryTest with HiveParquetTest {

  test("Case insensitive attribute names") {
    withParquetTable((1 to 4).map(i => Cases(i.toString, i.toString)), "cases") {
      val expected = (1 to 4).map(i => Row(i.toString))
      checkAnswer(ctx.sql("SELECT upper FROM cases"), expected)
      checkAnswer(ctx.sql("SELECT LOWER FROM cases"), expected)
    }
  }

  test("SELECT on Parquet table") {
    val data = (1 to 4).map(i => (i, s"val_$i"))
    withParquetTable(data, "t") {
      checkAnswer(ctx.sql("SELECT * FROM t"), data.map(Row.fromTuple))
    }
  }

  test("Simple column projection + filter on Parquet table") {
    withParquetTable((1 to 4).map(i => (i % 2 == 0, i, s"val_$i")), "t") {
      checkAnswer(
        ctx.sql("SELECT `_1`, `_3` FROM t WHERE `_1` = true"),
        Seq(Row(true, "val_2"), Row(true, "val_4")))
    }
  }

  test("Converting Hive to Parquet Table via saveAsParquetFile") {
    withTempPath { dir =>
      ctx.sql("SELECT * FROM src").write.parquet(dir.getCanonicalPath)
      ctx.read.parquet(dir.getCanonicalPath).registerTempTable("p")
      withTempTable("p") {
        checkAnswer(
          ctx.sql("SELECT * FROM src ORDER BY key"),
          ctx.sql("SELECT * from p ORDER BY key").collect().toSeq)
      }
    }
  }

  test("INSERT OVERWRITE TABLE Parquet table") {
    withParquetTable((1 to 10).map(i => (i, s"val_$i")), "t") {
      withTempPath { file =>
        ctx.sql("SELECT * FROM t LIMIT 1").write.parquet(file.getCanonicalPath)
        ctx.read.parquet(file.getCanonicalPath).registerTempTable("p")
        withTempTable("p") {
          // let's do three overwrites for good measure
          ctx.sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          ctx.sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          ctx.sql("INSERT OVERWRITE TABLE p SELECT * FROM t")
          checkAnswer(ctx.sql("SELECT * FROM p"), ctx.sql("SELECT * FROM t").collect().toSeq)
        }
      }
    }
  }
}
