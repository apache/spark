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

package org.apache.spark.sql.hive.orc

import java.io.File

import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.spark.sql.hive.HiveContext
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.test.SQLTestUtils
import org.apache.spark.sql._

private[sql] trait OrcTest extends SQLTestUtils {
  protected def hiveContext = sqlContext.asInstanceOf[HiveContext]

  import sqlContext.sparkContext
  import sqlContext.implicits._

  /**
   * Writes `data` to a Orc file, which is then passed to `f` and will be deleted after `f`
   * returns.
   */
  protected def withOrcFile[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: String => Unit): Unit = {
    withTempPath { file =>
      sparkContext.parallelize(data).toDF().saveAsOrcFile(file.getCanonicalPath)
      f(file.getCanonicalPath)
    }
  }

  /**
   * Writes `data` to a Orc file and reads it back as a [[DataFrame]],
   * which is then passed to `f`. The Orc file will be deleted after `f` returns.
   */
  protected def withOrcDataFrame[T <: Product: ClassTag: TypeTag]
      (data: Seq[T])
      (f: DataFrame => Unit): Unit = {
    import org.apache.spark.sql.hive.orc.OrcContext
    withOrcFile(data)(path => f(hiveContext.orcFile(path)))
  }

  /**
   * Writes `data` to a Orc file, reads it back as a [[DataFrame]] and registers it as a
   * temporary table named `tableName`, then call `f`. The temporary table together with the
   * Orc file will be dropped/deleted after `f` returns.
   */
  protected def withOrcTable[T <: Product: ClassTag: TypeTag]
      (data: Seq[T], tableName: String)
      (f: => Unit): Unit = {
    withOrcDataFrame(data) { df =>
      hiveContext.registerDataFrameAsTable(df, tableName)
      withTempTable(tableName)(f)
    }
  }

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      data: Seq[T], path: File): Unit = {
    data.toDF().save(path.getCanonicalPath, "org.apache.spark.sql.orc", SaveMode.Overwrite)
  }

  protected def makeOrcFile[T <: Product: ClassTag: TypeTag](
      df: DataFrame, path: File): Unit = {
    df.save(path.getCanonicalPath, "org.apache.spark.sql.orc", SaveMode.Overwrite)
  }
}

class NewOrcQuerySuite extends QueryTest with OrcTest {
  override val sqlContext: SQLContext = TestHive

  import sqlContext._

  test("simple select queries") {
    withOrcTable((0 until 10).map(i => (i, i.toString)), "t") {
      checkAnswer(
        sql("SELECT `_1` FROM t where t.`_1` > 5"),
        (6 until 10).map(Row.apply(_)))

      checkAnswer(
        sql("SELECT `_1` FROM t as tmp where tmp.`_1` < 5"),
        (0 until 5).map(Row.apply(_)))
    }
  }

  test("appending") {
    val data = (0 until 10).map(i => (i, i.toString))
    createDataFrame(data).toDF("c1", "c2").registerTempTable("tmp")
    withOrcTable(data, "t") {
      sql("INSERT INTO TABLE t SELECT * FROM tmp")
      checkAnswer(table("t"), (data ++ data).map(Row.fromTuple))
    }
    catalog.unregisterTable(Seq("tmp"))
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    createDataFrame(data).toDF("c1", "c2").registerTempTable("tmp")
    withOrcTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(table("t"), data.map(Row.fromTuple))
    }
    catalog.unregisterTable(Seq("tmp"))
  }

  test("self-join") {
    // 4 rows, cells of column 1 of row 2 and row 4 are null
    val data = (1 to 4).map { i =>
      val maybeInt = if (i % 2 == 0) None else Some(i)
      (maybeInt, i.toString)
    }

    withOrcTable(data, "t") {
      val selfJoin = sql("SELECT * FROM t x JOIN t y WHERE x.`_1` = y.`_1`")
      val queryOutput = selfJoin.queryExecution.analyzed.output

      assertResult(4, "Field count mismatches")(queryOutput.size)
      assertResult(2, "Duplicated expression ID in query plan:\n $selfJoin") {
        queryOutput.filter(_.name == "_1").map(_.exprId).size
      }

      checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
    }
  }

  test("nested data - struct with array field") {
    val data = (1 to 10).map(i => Tuple1((i, Seq("val_$i"))))
    withOrcTable(data, "t") {
      checkAnswer(sql("SELECT `_1`.`_2`[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }

  test("nested data - array of struct") {
    val data = (1 to 10).map(i => Tuple1(Seq(i -> "val_$i")))
    withOrcTable(data, "t") {
      checkAnswer(sql("SELECT `_1`[0].`_2` FROM t"), data.map {
        case Tuple1(Seq((_, string))) => Row(string)
      })
    }
  }

  test("columns only referenced by pushed down filters should remain") {
    withOrcTable((1 to 10).map(Tuple1.apply), "t") {
      checkAnswer(sql("SELECT `_1` FROM t WHERE `_1` < 10"), (1 to 9).map(Row.apply(_)))
    }
  }

  test("SPARK-5309 strings stored using dictionary compression in orc") {
    withOrcTable((0 until 1000).map(i => ("same", "run_" + i / 100, 1)), "t") {
      checkAnswer(
        sql("SELECT `_1`, `_2`, SUM(`_3`) FROM t GROUP BY `_1`, `_2`"),
        (0 until 10).map(i => Row("same", "run_" + i, 100)))

      checkAnswer(
        sql("SELECT `_1`, `_2`, SUM(`_3`) FROM t WHERE `_2` = 'run_5' GROUP BY `_1`, `_2`"),
        List(Row("same", "run_5", 100)))
    }
  }
}
