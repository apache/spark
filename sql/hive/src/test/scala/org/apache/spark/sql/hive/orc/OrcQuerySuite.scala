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

import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.ql.io.orc.CompressionKind
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.hive.test.TestHive
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._

case class AllDataTypesWithNonPrimitiveType(
    stringField: String,
    intField: Int,
    longField: Long,
    floatField: Float,
    doubleField: Double,
    shortField: Short,
    byteField: Byte,
    booleanField: Boolean,
    array: Seq[Int],
    arrayContainsNull: Seq[Option[Int]],
    map: Map[Int, Long],
    mapValueContainsNull: Map[Int, Option[Long]],
    data: (Seq[Int], (Int, String)))

case class BinaryData(binaryData: Array[Byte])

case class Contact(name: String, phone: String)

case class Person(name: String, age: Int, contacts: Seq[Contact])

class OrcQuerySuite extends QueryTest with BeforeAndAfterAll with OrcTest {
  override val sqlContext = TestHive

  def getTempFilePath(prefix: String, suffix: String = ""): File = {
    val tempFile = File.createTempFile(prefix, suffix)
    tempFile.delete()
    tempFile
  }

  test("Read/write All Types") {
    val data = (0 to 255).map { i =>
      (s"$i", i, i.toLong, i.toFloat, i.toDouble, i.toShort, i.toByte, i % 2 == 0)
    }

    withOrcFile(data) { file =>
      checkAnswer(
        read.format("orc").load(file),
        data.toDF().collect())
    }
  }

  test("Read/write binary data") {
    withOrcFile(BinaryData("test".getBytes("utf8")) :: Nil) { file =>
      val bytes = read.format("orc").load(file).head().getAs[Array[Byte]](0)
      assert(new String(bytes, "utf8") === "test")
    }
  }

  test("Read/write all types with non-primitive type") {
    val data = (0 to 255).map { i =>
      AllDataTypesWithNonPrimitiveType(
        s"$i", i, i.toLong, i.toFloat, i.toDouble, i.toShort, i.toByte, i % 2 == 0,
        0 until i,
        (0 until i).map(Option(_).filter(_ % 3 == 0)),
        (0 until i).map(i => i -> i.toLong).toMap,
        (0 until i).map(i => i -> Option(i.toLong)).toMap + (i -> None),
        (0 until i, (i, s"$i")))
    }

    withOrcFile(data) { file =>
      checkAnswer(
        read.format("orc").load(file),
        data.toDF().collect())
    }
  }

  test("Creating case class RDD table") {
    val data = (1 to 100).map(i => (i, s"val_$i"))
    sparkContext.parallelize(data).toDF().registerTempTable("t")
    withTempTable("t") {
      checkAnswer(sql("SELECT * FROM t"), data.toDF().collect())
    }
  }

  test("Simple selection form ORC table") {
    val data = (1 to 10).map { i =>
      Person(s"name_$i", i, (0 to 1).map { m => Contact(s"contact_$m", s"phone_$m") })
    }

    withOrcTable(data, "t") {
      // ppd:
      // leaf-0 = (LESS_THAN_EQUALS age 5)
      // expr = leaf-0
      assert(sql("SELECT name FROM t WHERE age <= 5").count() === 5)

      // ppd:
      // leaf-0 = (LESS_THAN_EQUALS age 5)
      // expr = (not leaf-0)
      assertResult(10) {
        sql("SELECT name, contacts FROM t where age > 5")
          .flatMap(_.getAs[Seq[_]]("contacts"))
          .count()
      }

      // ppd:
      // leaf-0 = (LESS_THAN_EQUALS age 5)
      // leaf-1 = (LESS_THAN age 8)
      // expr = (and (not leaf-0) leaf-1)
      {
        val df = sql("SELECT name, contacts FROM t WHERE age > 5 AND age < 8")
        assert(df.count() === 2)
        assertResult(4) {
          df.flatMap(_.getAs[Seq[_]]("contacts")).count()
        }
      }

      // ppd:
      // leaf-0 = (LESS_THAN age 2)
      // leaf-1 = (LESS_THAN_EQUALS age 8)
      // expr = (or leaf-0 (not leaf-1))
      {
        val df = sql("SELECT name, contacts FROM t WHERE age < 2 OR age > 8")
        assert(df.count() === 3)
        assertResult(6) {
          df.flatMap(_.getAs[Seq[_]]("contacts")).count()
        }
      }
    }
  }

  test("save and load case class RDD with `None`s as orc") {
    val data = (
      None: Option[Int],
      None: Option[Long],
      None: Option[Float],
      None: Option[Double],
      None: Option[Boolean]
    ) :: Nil

    withOrcFile(data) { file =>
      checkAnswer(
        read.format("orc").load(file),
        Row(Seq.fill(5)(null): _*))
    }
  }

  // We only support zlib in Hive 0.12.0 now
  test("Default compression options for writing to an ORC file") {
    withOrcFile((1 to 100).map(i => (i, s"val_$i"))) { file =>
      assertResult(CompressionKind.ZLIB) {
        OrcFileOperator.getFileReader(file).get.getCompression
      }
    }
  }

  // Following codec is supported in hive-0.13.1, ignore it now
  ignore("Other compression options for writing to an ORC file - 0.13.1 and above") {
    val data = (1 to 100).map(i => (i, s"val_$i"))
    val conf = sparkContext.hadoopConfiguration

    conf.set(ConfVars.HIVE_ORC_DEFAULT_COMPRESS.varname, "SNAPPY")
    withOrcFile(data) { file =>
      assertResult(CompressionKind.SNAPPY) {
        OrcFileOperator.getFileReader(file).get.getCompression
      }
    }

    conf.set(ConfVars.HIVE_ORC_DEFAULT_COMPRESS.varname, "NONE")
    withOrcFile(data) { file =>
      assertResult(CompressionKind.NONE) {
        OrcFileOperator.getFileReader(file).get.getCompression
      }
    }

    conf.set(ConfVars.HIVE_ORC_DEFAULT_COMPRESS.varname, "LZO")
    withOrcFile(data) { file =>
      assertResult(CompressionKind.LZO) {
        OrcFileOperator.getFileReader(file).get.getCompression
      }
    }
  }

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

  test("SPARK-8501: Avoids discovery schema from empty ORC files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("empty_orc") {
        withTempTable("empty", "single") {
          sqlContext.sql(
            s"""CREATE TABLE empty_orc(key INT, value STRING)
               |STORED AS ORC
               |LOCATION '$path'
             """.stripMargin)

          val emptyDF = Seq.empty[(Int, String)].toDF("key", "value").coalesce(1)
          emptyDF.registerTempTable("empty")

          // This creates 1 empty ORC file with Hive ORC SerDe.  We are using this trick because
          // Spark SQL ORC data source always avoids write empty ORC files.
          sqlContext.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM empty
             """.stripMargin)

          val errorMessage = intercept[AnalysisException] {
            sqlContext.read.format("orc").load(path)
          }.getMessage

          assert(errorMessage.contains("Failed to discover schema from ORC files"))

          val singleRowDF = Seq((0, "foo")).toDF("key", "value").coalesce(1)
          singleRowDF.registerTempTable("single")

          sqlContext.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM single
             """.stripMargin)

          val df = sqlContext.read.format("orc").load(path)
          assert(df.schema === singleRowDF.schema.asNullable)
          checkAnswer(df, singleRowDF)
        }
      }
    }
  }
}
