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

package org.apache.spark.sql.execution.datasources.orc

import java.io.File
import java.nio.charset.StandardCharsets
import java.sql.Timestamp
import java.time.LocalDateTime

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, OrcFile}
import org.apache.orc.OrcConf.COMPRESS
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.TableIdentifier
import org.apache.spark.sql.catalyst.util.DateTimeTestUtils
import org.apache.spark.sql.execution.FileSourceScanExec
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation, RecordReaderIterator}
import org.apache.spark.sql.execution.datasources.v2.BatchScanExec
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.util.Utils

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

abstract class OrcQueryTest extends OrcTest {
  import testImplicits._

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
      val bytes = spark.read.orc(file).head().getAs[Array[Byte]](0)
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
        spark.read.orc(file),
        data.toDF().collect())
    }
  }

  test("Read/write UserDefinedType") {
    withTempPath { path =>
      val data = Seq((1, new TestUDT.MyDenseVector(Array(0.25, 2.25, 4.25))))
      val udtDF = data.toDF("id", "vectors")
      udtDF.write.orc(path.getAbsolutePath)
      val readBack = spark.read.schema(udtDF.schema).orc(path.getAbsolutePath)
      checkAnswer(udtDF, readBack)
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
          .flatMap(_.getAs[scala.collection.Seq[_]]("contacts"))
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
          df.rdd.flatMap(_.getAs[scala.collection.Seq[_]]("contacts")).count()
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
          df.rdd.flatMap(_.getAs[scala.collection.Seq[_]]("contacts")).count()
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
        spark.read.orc(file),
        Row(Seq.fill(5)(null): _*))
    }
  }

  test("SPARK-16610: Respect orc.compress (i.e., OrcConf.COMPRESS) when compression is unset") {
    // Respect `orc.compress` (i.e., OrcConf.COMPRESS).
    withTempPath { file =>
      spark.range(0, 10).write
        .option(COMPRESS.getAttribute, OrcCompressionCodec.ZLIB.name())
        .orc(file.getCanonicalPath)

      val maybeOrcFile = file.listFiles().find(_.getName.endsWith(".zlib.orc"))
      assert(maybeOrcFile.isDefined)

      val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
      val conf = OrcFile.readerOptions(new Configuration())
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, conf)) { reader =>
        assert(OrcCompressionCodec.ZLIB.name() === reader.getCompressionKind.name)
      }
    }

    // `compression` overrides `orc.compress`.
    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", OrcCompressionCodec.ZLIB.name())
        .option(COMPRESS.getAttribute, OrcCompressionCodec.SNAPPY.name())
        .orc(file.getCanonicalPath)

      val maybeOrcFile = file.listFiles().find(_.getName.endsWith(".zlib.orc"))
      assert(maybeOrcFile.isDefined)

      val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
      val conf = OrcFile.readerOptions(new Configuration())
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, conf)) { reader =>
        assert(OrcCompressionCodec.ZLIB.name() === reader.getCompressionKind.name)
      }
    }
  }

  test("Compression options for writing to an ORC file (SNAPPY, ZLIB and NONE)") {
    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", OrcCompressionCodec.ZLIB.name())
        .orc(file.getCanonicalPath)

      val maybeOrcFile = file.listFiles().find(_.getName.endsWith(".zlib.orc"))
      assert(maybeOrcFile.isDefined)

      val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
      val conf = OrcFile.readerOptions(new Configuration())
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, conf)) { reader =>
        assert(OrcCompressionCodec.ZLIB.name() === reader.getCompressionKind.name)
      }
    }

    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", OrcCompressionCodec.SNAPPY.name())
        .orc(file.getCanonicalPath)

      val maybeOrcFile = file.listFiles().find(_.getName.endsWith(".snappy.orc"))
      assert(maybeOrcFile.isDefined)

      val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
      val conf = OrcFile.readerOptions(new Configuration())
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, conf)) { reader =>
        assert(OrcCompressionCodec.SNAPPY.name() === reader.getCompressionKind.name)
      }
    }

    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", OrcCompressionCodec.NONE.name())
        .orc(file.getCanonicalPath)

      val maybeOrcFile = file.listFiles().find(_.getName.endsWith(".orc"))
      assert(maybeOrcFile.isDefined)

      val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
      val conf = OrcFile.readerOptions(new Configuration())
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, conf)) { reader =>
        assert(OrcCompressionCodec.NONE.name() === reader.getCompressionKind.name)
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
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")

    withOrcFile(data) { file =>
      withTempView("t") {
        spark.read.orc(file).createOrReplaceTempView("t")
        checkAnswer(spark.table("t"), data.map(Row.fromTuple))
        sql("INSERT INTO TABLE t SELECT * FROM tmp")
        checkAnswer(spark.table("t"), (data ++ data).map(Row.fromTuple))
      }
    }

    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"),
      ignoreIfNotExists = true,
      purge = false)
  }

  test("overwriting") {
    val data = (0 until 10).map(i => (i, i.toString))
    spark.createDataFrame(data).toDF("c1", "c2").createOrReplaceTempView("tmp")
    withOrcTable(data, "t") {
      sql("INSERT OVERWRITE TABLE t SELECT * FROM tmp")
      checkAnswer(spark.table("t"), data.map(Row.fromTuple))
    }
    spark.sessionState.catalog.dropTable(
      TableIdentifier("tmp"),
      ignoreIfNotExists = true,
      purge = false)
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
      assertResult(2, s"Duplicated expression ID in query plan:\n $selfJoin") {
        queryOutput.filter(_.name == "_1").map(_.exprId).size
      }

      checkAnswer(selfJoin, List(Row(1, "1", 1, "1"), Row(3, "3", 3, "3")))
    }
  }

  test("nested data - struct with array field") {
    val data = (1 to 10).map(i => Tuple1((i, Seq(s"val_$i"))))
    withOrcTable(data, "t") {
      checkAnswer(sql("SELECT `_1`.`_2`[0] FROM t"), data.map {
        case Tuple1((_, Seq(string))) => Row(string)
      })
    }
  }

  test("nested data - array of struct") {
    val data = (1 to 10).map(i => Tuple1(Seq(i -> s"val_$i")))
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

      spark.range(0, 10).select($"id" as "Acol").write.orc(path)
      spark.read.orc(path).schema("Acol")
      intercept[IllegalArgumentException] {
        spark.read.orc(path).schema("acol")
      }
      checkAnswer(spark.read.orc(path).select("acol").sort("acol"),
        (0 until 10).map(Row(_)))
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
        spark.createDataFrame(data).toDF("a", "b").repartition(10).write.orc(path)
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
            sourceDf.count() < 10 && expectedData.subsetOf(data),
            s"No data was filtered for predicate: $pred")
        }

        checkPredicate($"a" === 5, List(5).map(Row(_, null)))
        checkPredicate($"a" <=> 5, List(5).map(Row(_, null)))
        checkPredicate($"a" < 5, List(1, 3).map(Row(_, null)))
        checkPredicate($"a" <= 5, List(1, 3, 5).map(Row(_, null)))
        checkPredicate($"a" > 5, List(7, 9).map(Row(_, null)))
        checkPredicate($"a" >= 5, List(5, 7, 9).map(Row(_, null)))
        checkPredicate($"a".isNull, List(null).map(Row(_, null)))
        checkPredicate($"b".isNotNull, List())
        checkPredicate($"a".isin(3, 5, 7), List(3, 5, 7).map(Row(_, null)))
        checkPredicate($"a" > 0 && $"a" < 3, List(1).map(Row(_, null)))
        checkPredicate($"a" < 1 || $"a" > 8, List(9).map(Row(_, null)))
        checkPredicate(!($"a" > 3), List(1, 3).map(Row(_, null)))
        checkPredicate(!($"a" > 0 && $"a" < 3), List(3, 5, 7, 9).map(Row(_, null)))
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
      checkPredicatePushDown(spark.createDataFrame(data).toDF("a"), 10, "a == 2")
    }
  }

  test("Support for pushing down filters for timestamp types") {
    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      val timeString = "2015-08-20 14:57:00"
      val data = (0 until 10).map { i =>
        val milliseconds = Timestamp.valueOf(timeString).getTime + i * 3600
        Tuple1(new Timestamp(milliseconds))
      }
      checkPredicatePushDown(spark.createDataFrame(data).toDF("a"), 10, s"a == '$timeString'")
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

  test("Empty schema does not read data from ORC file") {
    val data = Seq((1, 1), (2, 2))
    withOrcFile(data) { path =>
      val conf = new Configuration()
      conf.set(OrcConf.INCLUDE_COLUMNS.getAttribute, "")
      conf.setBoolean("hive.io.file.read.all.columns", false)

      val orcRecordReader = {
        val file = new File(path).listFiles().find(_.getName.endsWith(".orc")).head
        val split = new FileSplit(new Path(file.toURI), 0, file.length, Array.empty[String])
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext = new TaskAttemptContextImpl(conf, attemptId)
        val oif = new OrcInputFormat[OrcStruct]
        oif.createRecordReader(split, hadoopAttemptContext)
      }

      val recordsIterator = new RecordReaderIterator[OrcStruct](orcRecordReader)
      try {
        assert(recordsIterator.next().toString == "{null, null}")
      } finally {
        recordsIterator.close()
      }
    }
  }

  test("read from multiple orc input paths") {
    val path1 = Utils.createTempDir()
    val path2 = Utils.createTempDir()
    makeOrcFile((1 to 10).map(Tuple1.apply), path1)
    makeOrcFile((1 to 10).map(Tuple1.apply), path2)
    val df = spark.read.orc(path1.getCanonicalPath, path2.getCanonicalPath)
    assert(df.count() == 20)
  }

  test("Enabling/disabling ignoreCorruptFiles") {
    def testIgnoreCorruptFiles(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.orc(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.orc(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.orc(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(df, Seq(Row(0), Row(1)))
      }
    }

    def testIgnoreCorruptFilesWithoutSchemaInfer(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.orc(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.orc(new Path(basePath, "second").toString)
        spark.range(2, 3).toDF("a").write.json(new Path(basePath, "third").toString)
        val df = spark.read.schema("a long").orc(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString,
          new Path(basePath, "third").toString)
        checkAnswer(df, Seq(Row(0), Row(1)))
      }
    }

    def testAllCorruptFiles(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.json(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.json(new Path(basePath, "second").toString)
        val df = spark.read.orc(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString)
        assert(df.count() == 0)
      }
    }

    def testAllCorruptFilesWithoutSchemaInfer(): Unit = {
      withTempDir { dir =>
        val basePath = dir.getCanonicalPath
        spark.range(1).toDF("a").write.json(new Path(basePath, "first").toString)
        spark.range(1, 2).toDF("a").write.json(new Path(basePath, "second").toString)
        val df = spark.read.schema("a long").orc(
          new Path(basePath, "first").toString,
          new Path(basePath, "second").toString)
        assert(df.count() == 0)
      }
    }

    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "true") {
      testIgnoreCorruptFiles()
      testIgnoreCorruptFilesWithoutSchemaInfer()
      checkError(
        exception = intercept[AnalysisException] {
          testAllCorruptFiles()
        },
        errorClass = "UNABLE_TO_INFER_SCHEMA",
        parameters = Map("format" -> "ORC")
      )
      testAllCorruptFilesWithoutSchemaInfer()
    }

    withSQLConf(SQLConf.IGNORE_CORRUPT_FILES.key -> "false") {
      val e1 = intercept[SparkException] {
        testIgnoreCorruptFiles()
      }
      assert(e1.getErrorClass.startsWith("FAILED_READ_FILE"))
      assert(e1.getCause.getMessage.contains("Malformed ORC file") ||
        // Hive ORC table scan uses a different code path and has one more error stack
        e1.getCause.getCause.getMessage.contains("Malformed ORC file"))
      val e2 = intercept[SparkException] {
        testIgnoreCorruptFilesWithoutSchemaInfer()
      }
      assert(e2.getErrorClass.startsWith("FAILED_READ_FILE"))
      assert(e2.getCause.getMessage.contains("Malformed ORC file") ||
        // Hive ORC table scan uses a different code path and has one more error stack
        e2.getCause.getCause.getMessage.contains("Malformed ORC file"))
      checkErrorMatchPVals(
        exception = intercept[SparkException] {
          testAllCorruptFiles()
        },
        errorClass = "FAILED_READ_FILE.CANNOT_READ_FILE_FOOTER",
        parameters = Map("path" -> "file:.*")
      )
      val e4 = intercept[SparkException] {
        testAllCorruptFilesWithoutSchemaInfer()
      }
      assert(e4.getErrorClass.startsWith("FAILED_READ_FILE"))
      assert(e4.getCause.getMessage.contains("Malformed ORC file") ||
        // Hive ORC table scan uses a different code path and has one more error stack
        e4.getCause.getCause.getMessage.contains("Malformed ORC file"))
    }
  }

  test("SPARK-27160 Predicate pushdown correctness on DecimalType for ORC") {
    withTempPath { dir =>
      withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
        val path = dir.getCanonicalPath
        Seq(BigDecimal(0.1), BigDecimal(0.2), BigDecimal(-0.3))
          .toDF("x").write.orc(path)
        val df = spark.read.orc(path)
        checkAnswer(df.filter("x >= 0.1"), Seq(Row(0.1), Row(0.2)))
        checkAnswer(df.filter("x > 0.1"), Seq(Row(0.2)))
        checkAnswer(df.filter("x <= 0.15"), Seq(Row(0.1), Row(-0.3)))
        checkAnswer(df.filter("x < 0.1"), Seq(Row(-0.3)))
        checkAnswer(df.filter("x == 0.2"), Seq(Row(0.2)))
      }
    }
  }
}

abstract class OrcQuerySuite extends OrcQueryTest with SharedSparkSession {
  import testImplicits._

  test("LZO compression options for writing to an ORC file") {
    withTempPath { file =>
      spark.range(0, 10).write
        .option("compression", OrcCompressionCodec.LZO.name())
        .orc(file.getCanonicalPath)

      val maybeOrcFile = file.listFiles().find(_.getName.endsWith(".lzo.orc"))
      assert(maybeOrcFile.isDefined)

      val orcFilePath = new Path(maybeOrcFile.get.getAbsolutePath)
      val conf = OrcFile.readerOptions(new Configuration())
      Utils.tryWithResource(OrcFile.createReader(orcFilePath, conf)) { reader =>
        assert(OrcCompressionCodec.LZO.name() === reader.getCompressionKind.name)
      }
    }
  }

  test("Schema discovery on empty ORC files") {
    // SPARK-8501 is fixed.
    withTempPath { dir =>
      val path = dir.getCanonicalPath

      withTable("empty_orc") {
        withTempView("empty", "single") {
          spark.sql(
            s"""CREATE TABLE empty_orc(key INT, value STRING)
               |USING ORC
               |LOCATION '${dir.toURI}'
             """.stripMargin)

          val emptyDF = Seq.empty[(Int, String)].toDF("key", "value").coalesce(1)
          emptyDF.createOrReplaceTempView("empty")

          // This creates 1 empty ORC file with ORC SerDe.  We are using this trick because
          // Spark SQL ORC data source always avoids write empty ORC files.
          spark.sql(
            s"""INSERT INTO TABLE empty_orc
               |SELECT key, value FROM empty
             """.stripMargin)

          val df = spark.read.orc(path)
          assert(df.schema === emptyDF.schema.asNullable)
          checkAnswer(df, emptyDF)
        }
      }
    }
  }

  test("SPARK-21791 ORC should support column names with dot") {
    withTempDir { dir =>
      val path = new File(dir, "orc").getCanonicalPath
      Seq(Some(1), None).toDF("col.dots").write.orc(path)
      assert(spark.read.orc(path).collect().length == 2)
    }
  }

  test("SPARK-25579 ORC PPD should support column names with dot") {
    withSQLConf(SQLConf.ORC_FILTER_PUSHDOWN_ENABLED.key -> "true") {
      checkPredicatePushDown(spark.range(10).toDF("col.dot"), 10, "`col.dot` == 2")
    }
  }

  test("SPARK-20728 Make ORCFileFormat configurable between sql/hive and sql/core") {
    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "hive") {
      val e = intercept[AnalysisException] {
        sql("CREATE TABLE spark_20728(a INT) USING ORC")
      }
      assert(e.message.contains("Hive built-in ORC data source must be used with Hive support"))
    }

    withSQLConf(SQLConf.ORC_IMPLEMENTATION.key -> "native") {
      withTable("spark_20728") {
        sql("CREATE TABLE spark_20728(a INT) USING ORC")
        val fileFormat = sql("SELECT * FROM spark_20728").queryExecution.analyzed.collectFirst {
          case l: LogicalRelation => l.relation.asInstanceOf[HadoopFsRelation].fileFormat.getClass
        }
        assert(fileFormat == Some(classOf[OrcFileFormat]))
      }
    }
  }

  test("SPARK-34862: Support ORC vectorized reader for nested column") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(10).map { x =>
        val stringColumn = s"$x" * 10
        val structColumn = (x, s"$x" * 100)
        val arrayColumn = (0 until 5).map(i => (x + i, s"$x" * 5))
        val mapColumn = Map(
          s"$x" -> (x * 0.1, (x, s"$x" * 100)),
          (s"$x" * 2) -> (x * 0.2, (x, s"$x" * 200)),
          (s"$x" * 3) -> (x * 0.3, (x, s"$x" * 300)))
        (x, stringColumn, structColumn, arrayColumn, mapColumn)
      }.toDF("int_col", "string_col", "struct_col", "array_col", "map_col")
      df.write.format("orc").save(path)

      withSQLConf(SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true") {
        val readDf = spark.read.orc(path)
        val vectorizationEnabled = readDf.queryExecution.executedPlan.exists {
          case scan @ (_: FileSourceScanExec | _: BatchScanExec) => scan.supportsColumnar
          case _ => false
        }
        assert(vectorizationEnabled)
        checkAnswer(readDf, df)
      }
    }
  }

  test("SPARK-37728: Reading nested columns with ORC vectorized reader should not " +
    "cause ArrayIndexOutOfBoundsException") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(100).map { _ =>
        val arrayColumn = (0 until 50).map(_ => (0 until 1000).map(k => k.toString))
        arrayColumn
      }.toDF("record").repartition(1)
      df.write.format("orc").save(path)

      withSQLConf(SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true") {
        val readDf = spark.read.orc(path)
        val vectorizationEnabled = readDf.queryExecution.executedPlan.exists {
          case scan @ (_: FileSourceScanExec | _: BatchScanExec) => scan.supportsColumnar
          case _ => false
        }
        assert(vectorizationEnabled)
        checkAnswer(readDf, df)
      }
    }
  }

  test("SPARK-36594: ORC vectorized reader should properly check maximal number of fields") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(10).map { x =>
        val stringColumn = s"$x" * 10
        val structColumn = (x, s"$x" * 100)
        val arrayColumn = (0 until 5).map(i => (x + i, s"$x" * 5))
        val mapColumn = Map(s"$x" -> (x * 0.1, (x, s"$x" * 100)))
        (x, stringColumn, structColumn, arrayColumn, mapColumn)
      }.toDF("int_col", "string_col", "struct_col", "array_col", "map_col")
      df.write.format("orc").save(path)

      Seq(("5", false), ("10", true)).foreach {
        case (maxNumFields, vectorizedEnabled) =>
          withSQLConf(SQLConf.ORC_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
            SQLConf.WHOLESTAGE_MAX_NUM_FIELDS.key -> maxNumFields) {
            val scanPlan = spark.read.orc(path).queryExecution.executedPlan
            assert(scanPlan.exists {
              case scan @ (_: FileSourceScanExec | _: BatchScanExec) => scan.supportsColumnar
              case _ => false
            } == vectorizedEnabled)
          }
      }
    }
  }

  test("Read/write all timestamp types") {
    val data = (0 to 255).map { i =>
      (new Timestamp(i), LocalDateTime.of(2019, 3, 21, 0, 2, 3, 456000000 + i))
    } :+ (null, null)

    withOrcFile(data) { file =>
      withAllNativeOrcReaders {
        checkAnswer(spark.read.orc(file), data.toDF().collect())
      }
    }
  }

  test("SPARK-37463: read/write Timestamp ntz to Orc with different time zone") {
    DateTimeTestUtils.withDefaultTimeZone(DateTimeTestUtils.LA) {
      val sqlText = """
                      |select
                      | timestamp_ntz '2021-06-01 00:00:00' ts_ntz1,
                      | timestamp_ntz '1883-11-16 00:00:00.0' as ts_ntz2,
                      | timestamp_ntz '2021-03-14 02:15:00.0' as ts_ntz3
                      |""".stripMargin

      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = sql(sqlText)

        df.write.mode("overwrite").orc(path)

        val query = s"select * from `orc`.`$path`"

        DateTimeTestUtils.outstandingZoneIds.foreach { zoneId =>
          DateTimeTestUtils.withDefaultTimeZone(zoneId) {
            withAllNativeOrcReaders {
              checkAnswer(sql(query), df)
            }
          }
        }
      }
    }
  }

  // SPARK-39519: Ignore this case because it requires more than 4g heap memory to ensure test
  // stability when use Java 11. Should test it manually when upgrading `hive-storage-api`
  ignore("SPARK-39387: BytesColumnVector should not throw RuntimeException due to overflow") {
    withTempPath { dir =>
      val path = dir.getCanonicalPath
      val df = spark.range(1, 22, 1, 1).map { _ =>
        val byteData = Array.fill[Byte](1024 * 1024)('X')
        val mapData = (1 to 100).map(i => (i, byteData))
        mapData
      }.toDF()
      df.write.format("orc").save(path)
    }
  }

  test("SPARK-39381: Make vectorized orc columar writer batch size configurable") {
    Seq(10, 100).foreach(batchSize => {
      withSQLConf(SQLConf.ORC_VECTORIZED_WRITER_BATCH_SIZE.key -> batchSize.toString) {
        withTempPath { dir =>
          val path = dir.getCanonicalPath
          val df = spark.range(1, 1024, 1, 1).map { _ =>
            val byteData = Array.fill[Byte](5 * 1024 * 1024)('X')
            byteData
          }.toDF()
          df.write.format("orc").save(path)
        }
      }
    })
  }

  test("SPARK-39830: Reading ORC table that requires type promotion may throw AIOOBE") {
    withSQLConf(SQLConf.ORC_VECTORIZED_WRITER_BATCH_SIZE.key -> "1",
      "orc.stripe.size" -> "10240",
      "orc.rows.between.memory.checks" -> "1") {
      withTempPath { dir =>
        val path = dir.getCanonicalPath
        val df = spark.range(1, 1 + 512, 1, 1).map { i =>
          if (i == 1) {
            (i, Array.fill[Byte](5 * 1024 * 1024)('X'))
          } else {
            (i, Array.fill[Byte](1)('X'))
          }
        }.toDF("c1", "c2")
        df.write.format("orc").save(path)
        withTable("t1") {
          spark.sql(s"create table t1 (c1 string,c2 binary) using orc location '$path'")
          spark.sql("select * from t1").collect()
        }
      }
    }
  }
}

class OrcV1QuerySuite extends OrcQuerySuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "orc")
}

class OrcV2QuerySuite extends OrcQuerySuite {
  override protected def sparkConf: SparkConf =
    super
      .sparkConf
      .set(SQLConf.USE_V1_SOURCE_LIST, "")
}
