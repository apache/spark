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

import java.nio.charset.StandardCharsets
import java.sql.Timestamp

import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.execution.datasources.LogicalRelation
import org.apache.spark.sql.hive.{HiveUtils, MetastoreRelation}
import org.apache.spark.sql.hive.test.TestHive._
import org.apache.spark.sql.hive.test.TestHive.implicits._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.types.{IntegerType, StructType}

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

  test("Read/write All Types") {
    val data = (0 to 255).map { i =>
      (s"$i", i, i.toLong, i.toFloat, i.toDouble, i.toShort, i.toByte, i % 2 == 0)
    }

    withOrcFile(data) { file =>
      checkAnswer(
        spark.read.orc(file),
        data.toDF().collect())
    }
  }

  test("Read/write binary data") {
    withOrcFile(BinaryData("test".getBytes(StandardCharsets.UTF_8)) :: Nil) { file =>
      val bytes = read.orc(file).head().getAs[Array[Byte]](0)
      assert(new String(bytes, StandardCharsets.UTF_8) === "test")
    }
  }

  test("Read/write all types with non-primitive type") {
    val data: Seq[AllDataTypesWithNonPrimitiveType] = (0 to 255).map { i =>
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
        read.orc(file),
        data.toDF().collect())
    }
  }

  test("Creating case class RDD table") {
    val data = (1 to 100).map(i => (i, s"val_$i"))
    sparkContext.parallelize(data).toDF().createOrReplaceTempView("t")
    withTempView("t") {
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
          .rdd
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
          df.rdd.flatMap(_.getAs[Seq[_]]("contacts")).count()
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
          df.rdd.flatMap(_.getAs[Seq[_]]("contacts")).count()
        }
      }
    }
  }

  test("save and load case class RDD with `None`s as orc") {
    val data = (
      Option.empty[Int],
      Option.empty[Long],
      Option.empty[Float],
      Option.empty[Double],
      Option.empty[Boolean]
    ) :: Nil

    withOrcFile(data) { file =>
      checkAnswer(
        read.orc(file),
        Row(Seq.fill(5)(null): _*))
    }
  }

  test("SPARK-16610: Respect orc.compress option when compression is unset") {
    // Respect `orc.compress`.
    withTempPath { file =>
      spark.range(0, 10).write
        .option("orc.compress", "ZLIB")
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("ZLIB" === expectedCompressionKind.name())
    }

    // `compression` overrides `orc.compress`.
    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", "ZLIB")
        .option("orc.compress", "SNAPPY")
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("ZLIB" === expectedCompressionKind.name())
    }
  }

  // Hive supports zlib, snappy and none for Hive 1.2.1.
  test("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)") {
    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", "ZLIB")
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("ZLIB" === expectedCompressionKind.name())
    }

    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", "SNAPPY")
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("SNAPPY" === expectedCompressionKind.name())
    }

    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", "NONE")
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("NONE" === expectedCompressionKind.name())
    }
  }

  // Following codec is not supported in Hive 1.2.1, ignore it now
  ignore("LZO compression options for writing to an ORC file not supported in Hive 1.2.1") {
    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", "LZO")
        .orc(file.getCanonicalPath)
      val expectedCompressionKind =
        OrcFileOperator.getFileReader(file.getCanonicalPath).get.getCompression
      assert("LZO" === expectedCompressionKind.name())
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
    createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withOrcTable(data, "t") {
      sql("INSERT INTO TABLE t SELECT * FROM tmp")
      checkAnswer(table("t"), (data ++ data).map(Row.fromTuple))
    }
    sessionState.catalog.dropTable(TableIdentifier("tmp"), ignoreIfNotExists = true, purge = false)
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withOrcTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(table("t"), data.map(Row.fromTuple))
    }
    sessionState.catalog.dropTable(TableIdentifier("tmp"), ignoreIfNotExists = true, purge = false)
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

  test("SPARK-9170: Don't implicitly lowercase of user-provided columns") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      spark.range(0, 10).select('id as "Acol").write.format("orc").save(path)
      spark.read.format("orc").load(path).schema("Acol")
      intercept[IllegalArgumentException] {
        spark.read.format("orc").load(path).schema("acol")
      }
      checkAnswer(spark.read.format("orc").load(path).select("acol").sort("acol"),
        (0 until 10).map(Row(_)))
    }
  }

  test("SPARK-8501: Avoids discovery schema from empty ORC files") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("empty_orc") {
        withTempView("empty", "single") {
          spark.sql(
            s"""CREATE TABLE empty_orc(key INT, value STRING)
               |STORED AS ORC
               |LOCATION '$path'
             """.stripMargin)

          val emptyDF = Seq.empty[(Int, String)].toDF("key", "value").coalesce(1)
          emptyDF.createOrReplaceTempView("empty")

          // This creates 1 empty ORC file with Hive ORC SerDe.  We are using this trick because
          // Spark SQL ORC data source always avoids write empty ORC files.
          spark.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM empty
             """.stripMargin)

          val errorMessage = intercept[AnalysisException] {
            spark.read.orc(path)
          }.getMessage

          assert(errorMessage.contains("Unable to infer schema for ORC"))

          val singleRowDF = Seq((0, "foo")).toDF("key", "value").coalesce(1)
          singleRowDF.createOrReplaceTempView("single")

          spark.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM single
             """.stripMargin)

          val df = spark.read.orc(path)
          assert(df.schema === singleRowDF.schema.asNullable)
          checkAnswer(df, singleRowDF)
        }
      }
    }
  }

  test("SPARK-10623 Enable ORC PPD") {
    withTempPath { dir =>
      withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        import testImplicits._
        val path = dir.getCanonicalPath

        // For field "a", the first column has odds integers. This is to check the filtered count
        // when `isNull` is performed. For Field "b", `isNotNull` of ORC file filters rows
        // only when all the values are null (maybe this works differently when the data
        // or query is complicated). So, simply here a column only having `null` is added.
        val data = (0 until 10).map { i =>
          val maybeInt = if (i % 2 == 0) None else Some(i)
          val nullValue: Option[String] = None
          (maybeInt, nullValue)
        }
        // It needs to repartition data so that we can have several ORC files
        // in order to skip stripes in ORC.
        createDataFrame(data).toDF("a", "b").repartition(10).write.orc(path)
        val df = spark.read.orc(path)

        def checkPredicate(pred: Column, answer: Seq[Row]): Unit = {
          val sourceDf = stripSparkFilter(df.where(pred))
          val data = sourceDf.collect().toSet
          val expectedData = answer.toSet

          // When a filter is pushed to ORC, ORC can apply it to rows. So, we can check
          // the number of rows returned from the ORC to make sure our filter pushdown work.
          // A tricky part is, ORC does not process filter rows fully but return some possible
          // results. So, this checks if the number of result is less than the original count
          // of data, and then checks if it contains the expected data.
          assert(
            sourceDf.count < 10 && expectedData.subsetOf(data),
            s"No data was filtered for predicate: $pred")
        }

        checkPredicate('a === 5, List(5).map(Row(_, null)))
        checkPredicate('a <=> 5, List(5).map(Row(_, null)))
        checkPredicate('a < 5, List(1, 3).map(Row(_, null)))
        checkPredicate('a <= 5, List(1, 3, 5).map(Row(_, null)))
        checkPredicate('a > 5, List(7, 9).map(Row(_, null)))
        checkPredicate('a >= 5, List(5, 7, 9).map(Row(_, null)))
        checkPredicate('a.isNull, List(null).map(Row(_, null)))
        checkPredicate('b.isNotNull, List())
        checkPredicate('a.isin(3, 5, 7), List(3, 5, 7).map(Row(_, null)))
        checkPredicate('a > 0 && 'a < 3, List(1).map(Row(_, null)))
        checkPredicate('a < 1 || 'a > 8, List(9).map(Row(_, null)))
        checkPredicate(!('a > 3), List(1, 3).map(Row(_, null)))
        checkPredicate(!('a > 0 && 'a < 3), List(3, 5, 7, 9).map(Row(_, null)))
      }
    }
  }

  test("Verify the ORC conversion parameter: CONVERT_METASTORE_ORC") {
    withTempView("single") {
      val singleRowDF = Seq((0, "foo")).toDF("key", "value")
      singleRowDF.createOrReplaceTempView("single")

      Seq("true", "false").foreach { orcConversion =>
        withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> orcConversion) {
          withTable("dummy_orc") {
            withTempPath { dir =>
              val path = dir.getCanonicalPath
              spark.sql(
                s"""
                   |CREATE TABLE dummy_orc(key INT, value STRING)
                   |STORED AS ORC
                   |LOCATION '$path'
                 """.stripMargin)

              spark.sql(
                s"""
                   |INSERT INTO TABLE dummy_orc
                   |SELECT key, value FROM single
                 """.stripMargin)

              val df = spark.sql("SELECT * FROM dummy_orc WHERE key=0")
              checkAnswer(df, singleRowDF)

              val queryExecution = df.queryExecution
              if (orcConversion == "true") {
                queryExecution.analyzed.collectFirst {
                  case _: LogicalRelation => ()
                }.getOrElse {
                  fail(s"Expecting the query plan to convert orc to data sources, " +
                    s"but got:\n$queryExecution")
                }
              } else {
                queryExecution.analyzed.collectFirst {
                  case _: MetastoreRelation => ()
                }.getOrElse {
                  fail(s"Expecting no conversion from orc to data sources, " +
                    s"but got:\n$queryExecution")
                }
              }
            }
          }
        }
      }
    }
  }

  test("converted ORC table supports resolving mixed case field") {
    withSQLConf(HiveUtils.CONVERT_METASTORE_ORC.key -> "true") {
      withTable("dummy_orc") {
        withTempPath { dir =>
          val df = spark.range(5).selectExpr("id", "id as valueField", "id as partitionValue")
          df.write
            .partitionBy("partitionValue")
            .mode("overwrite")
            .orc(dir.getAbsolutePath)

          spark.sql(s"""
            |create external table dummy_orc (id long, valueField long)
            |partitioned by (partitionValue int)
            |stored as orc
            |location "${dir.getAbsolutePath}"""".stripMargin)
          spark.sql(s"msck repair table dummy_orc")
          println(spark.sql("select * from dummy_orc"))
          checkAnswer(spark.sql("select * from dummy_orc"), df)
        }
      }
    }
  }

  test("SPARK-14962 Produce correct results on array type with isnotnull") {
    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val data = (0 until 10).map(i => Tuple1(Array(i)))
      withOrcFile(data) { file =>
        val actual = spark
          .read
          .orc(file)
          .where("_1 is not null")
        val expected = data.toDF()
        checkAnswer(actual, expected)
      }
    }
  }

  test("SPARK-15198 Support for pushing down filters for boolean types") {
    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val data = (0 until 10).map(_ => (true, false))
      withOrcFile(data) { file =>
        val df = spark.read.orc(file).where("_2 == true")
        val actual = stripSparkFilter(df).count()

        // ORC filter should be applied and the total count should be 0.
        assert(actual === 0)
      }
    }
  }

  test("Support for pushing down filters for decimal types") {
    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val data = (0 until 10).map(i => Tuple1(BigDecimal.valueOf(i)))
      withTempPath { file =>
        // It needs to repartition data so that we can have several ORC files
        // in order to skip stripes in ORC.
        createDataFrame(data).toDF("a").repartition(10).write.orc(file.getCanonicalPath)
        val df = spark.read.orc(file.getCanonicalPath).where("a == 2")
        val actual = stripSparkFilter(df).count()

        assert(actual < 10)
      }
    }
  }

  test("Support for pushing down filters for timestamp types") {
    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val timeString = "2015-08-20 14:57:00"
      val data = (0 until 10).map { i =>
        val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
        Tuple1(new Timestamp(milliseconds))
      }
      withTempPath { file =>
        // It needs to repartition data so that we can have several ORC files
        // in order to skip stripes in ORC.
        createDataFrame(data).toDF("a").repartition(10).write.orc(file.getCanonicalPath)
        val df = spark.read.orc(file.getCanonicalPath).where(s"a == '$timeString'")
        val actual = stripSparkFilter(df).count()

        assert(actual < 10)
      }
    }
  }

  test("column nullability and comment - write and then read") {
    val schema = (new StructType)
      .add("cl1", IntegerType, nullable = false, comment = "test")
      .add("cl2", IntegerType, nullable = true)
      .add("cl3", IntegerType, nullable = true)
    val row = Row(3, null, 4)
    val df = spark.createDataFrame(sparkContext.parallelize(row :: Nil), schema)

    val tableName = "tab"
    withTable(tableName) {
      df.write.format("orc").mode("overwrite").saveAsTable(tableName)
      // Verify the DDL command result: DESCRIBE TABLE
      checkAnswer(
        sql(s"desc $tableName").select("col_name", "comment").where($"comment" === "test"),
        Row("cl1", "test") :: Nil)
      // Verify the schema
      val expectedFields = schema.fields.map(f => f.copy(nullable = true))
      assert(spark.table(tableName).schema == schema.copy(fields = expectedFields))
    }
  }
}
