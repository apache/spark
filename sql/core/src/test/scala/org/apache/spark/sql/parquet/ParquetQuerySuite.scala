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

package org.apache.spark.sql.parquet

import org.apache.hadoop.fs.Path
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.types._
import org.apache.spark.sql.{SQLConf, QueryTest}
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._

/**
 * A test suite that tests various Parquet queries.
 */
class ParquetQuerySuiteBase extends QueryTest with ParquetTest {
  val sqlContext = TestSQLContext

  test("simple select queries") {
    withParquetTable((0 until 10).map(i => (i, i.toString)), "t") {
      checkAnswer(sql("SELECT _1 FROM t where t._1 > 5"), (6 until 10).map(Row.apply(_)))
      checkAnswer(sql("SELECT _1 FROM t as tmp where tmp._1 < 5"), (0 until 5).map(Row.apply(_)))
    }
  }

  test("appending") {
    val data = (0 until 10).map(i => (i, i.toString))
    createDataFrame(data).toDF("c1", "c2").registerTempTable("tmp")
    withParquetTable(data, "t") {
      sql("INSERT INTO TABLE t SELECT * FROM tmp")
      checkAnswer(table("t"), (data ++ data).map(Row.fromTuple))
    }
    catalog.unregisterTable(Seq("tmp"))
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    createDataFrame(data).toDF("c1", "c2").registerTempTable("tmp")
    withParquetTable(data, "t") {
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

    withParquetTable(data, "t") {
      val selfJoin = sql("SELECT * FROM t x JOIN t y WHERE x._1 = y._1")
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
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1._2[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }

  test("nested data - array of struct") {
    val data = (1 to 10).map(i => Tuple1(Seq(i -> "val_$i")))
    withParquetTable(data, "t") {
      checkAnswer(sql("SELECT _1[0]._2 FROM t"), data.map {
        case Tuple1(Seq((_, string))) => Row(string)
      })
    }
  }

  test("SPARK-1913 regression: columns only referenced by pushed down filters should remain") {
    withParquetTable((1 to 10).map(Tuple1.apply), "t") {
      checkAnswer(sql("SELECT _1 FROM t WHERE _1 < 10"), (1 to 9).map(Row.apply(_)))
    }
  }

  test("SPARK-5309 strings stored using dictionary compression in parquet") {
    withParquetTable((0 until 1000).map(i => ("same", "run_" + i /100, 1)), "t") {

      checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t GROUP BY _1, _2"),
        (0 until 10).map(i => Row("same", "run_" + i, 100)))

      checkAnswer(sql("SELECT _1, _2, SUM(_3) FROM t WHERE _2 = 'run_5' GROUP BY _1, _2"),
        List(Row("same", "run_5", 100)))
    }
  }

  test("SPARK-6917 DecimalType should work with non-native types") {
    val data = (1 to 10).map(i => Row(Decimal(i, 18, 0), new java.sql.Timestamp(i)))
    val schema = StructType(List(StructField("d", DecimalType(18, 0), false),
      StructField("time", TimestampType, false)).toArray)
    withTempPath { file =>
      val df = sqlContext.createDataFrame(sparkContext.parallelize(data), schema)
      df.write.parquet(file.getCanonicalPath)
      val df2 = sqlContext.read.parquet(file.getCanonicalPath)
      checkAnswer(df2, df.collect().toSeq)
    }
  }

  test("SPARK-8990 DataFrameReader.parquet() should respect user specified options") {
    withTempPath { dir =>
      val basePath = dir.getCanonicalPath
      sqlContext.range(0, 10).toDF("a").write.parquet(new Path(basePath, "foo=1").toString)
      sqlContext.range(0, 10).toDF("b").write.parquet(new Path(basePath, "foo=a").toString)

      assertResult(2) {
        // Disables schema merging via data source option
        sqlContext.read.option("mergeSchema", "false").parquet(basePath).columns.length
      }

      assertResult(3) {
        // Enables schema merging via data source option
        sqlContext.read.option("mergeSchema", "true").parquet(basePath).columns.length
      }
    }
  }
}

class ParquetDataSourceOnQuerySuite extends ParquetQuerySuiteBase with BeforeAndAfterAll {
  val originalConf = sqlContext.conf.parquetUseDataSourceApi

  override protected def beforeAll(): Unit = {
    sqlContext.conf.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "true")
  }

  override protected def afterAll(): Unit = {
    sqlContext.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, originalConf.toString)
  }
}

class ParquetDataSourceOffQuerySuite extends ParquetQuerySuiteBase with BeforeAndAfterAll {
  val originalConf = sqlContext.conf.parquetUseDataSourceApi

  override protected def beforeAll(): Unit = {
    sqlContext.conf.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "false")
  }

  override protected def afterAll(): Unit = {
    sqlContext.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, originalConf.toString)
  }
}
