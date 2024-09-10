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

package org.apache.spark.sql.execution.datasources.parquet

import java.time.LocalDateTime
import java.util.Locale

import scala.collection.mutable
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import com.google.common.primitives.UnsignedLong
import org.apache.hadoop.fs.{FileSystem, Path}
import org.apache.hadoop.mapreduce.{JobContext, TaskAttemptContext}
import org.apache.parquet.column.{Encoding, ParquetProperties}
import org.apache.parquet.column.ParquetProperties.WriterVersion.PARQUET_1_0
import org.apache.parquet.example.data.Group
import org.apache.parquet.example.data.simple.{SimpleGroup, SimpleGroupFactory}
import org.apache.parquet.hadoop._
import org.apache.parquet.hadoop.example.ExampleParquetWriter
import org.apache.parquet.io.api.Binary
import org.apache.parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.{SPARK_VERSION_SHORT, SparkException, TestUtils}
import org.apache.spark.sql._
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.catalyst.expressions.{GenericInternalRow, UnsafeRow}
import org.apache.spark.sql.catalyst.util.DateTimeUtils
import org.apache.spark.sql.execution.datasources.SQLHadoopMapReduceCommitProtocol
import org.apache.spark.sql.functions._
import org.apache.spark.sql.internal.SQLConf
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types._
import org.apache.spark.unsafe.types.UTF8String

/**
 * A test suite that tests basic Parquet I/O.
 */
class ParquetIOSuite extends QueryTest with ParquetTest with SharedSparkSession {
  import testImplicits._

  /**
   * Writes `data` to a Parquet file, reads it back and check file contents.
   */
  protected def checkParquetFile[T <: Product : ClassTag: TypeTag](data: Seq[T]): Unit = {
    withParquetDataFrame(data)(r => checkAnswer(r, data.map(Row.fromTuple)))
  }

  test("basic data types (without binary)") {
    val data = (1 to 4).map { i =>
      (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
    }
    checkParquetFile(data)
  }

  test("raw binary") {
    val data = (1 to 4).map(i => Tuple1(Array.fill(3)(i.toByte)))
    withParquetDataFrame(data) { df =>
      assertResult(data.map(_._1.mkString(",")).sorted) {
        df.collect().map(_.getAs[Array[Byte]](0).mkString(",")).sorted
      }
    }
  }

  test("SPARK-11694 Parquet logical types are not being tested properly") {
    val parquetSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  required int32 a(INT_8);
        |  required int32 b(INT_16);
        |  required int32 c(DATE);
        |  required int32 d(DECIMAL(1,0));
        |  required int64 e(DECIMAL(10,0));
        |  required binary f(UTF8);
        |  required binary g(ENUM);
        |  required binary h(DECIMAL(32,0));
        |  required fixed_len_byte_array(32) i(DECIMAL(32,0));
        |  required int64 j(TIMESTAMP_MILLIS);
        |  required int64 k(TIMESTAMP_MICROS);
        |}
      """.stripMargin)

    val expectedSparkTypes = Seq(ByteType, ShortType, DateType, DecimalType(1, 0),
      DecimalType(10, 0), StringType, StringType, DecimalType(32, 0), DecimalType(32, 0),
      TimestampType, TimestampType)

    withTempPath { location =>
      val path = new Path(location.getCanonicalPath)
      val conf = spark.sessionState.newHadoopConf()
      writeMetadata(parquetSchema, path, conf)
      readParquetFile(path.toString)(df => {
        val sparkTypes = df.schema.map(_.dataType)
        assert(sparkTypes === expectedSparkTypes)
      })
    }
  }

  test("SPARK-41096: FIXED_LEN_BYTE_ARRAY support") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required FIXED_LEN_BYTE_ARRAY(1) a;
            |  required FIXED_LEN_BYTE_ARRAY(3) b;
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        (0 until 10).map(_.toString).foreach { n =>
          val record = new SimpleGroup(schema)
          record.add(0, Binary.fromString(n))
          record.add(1, Binary.fromString(n + n + n))
          writer.write(record)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        Seq(true, false).foreach { vectorizedReaderEnabled =>
          readParquetFile(path.toString, vectorizedReaderEnabled) { df =>
            checkAnswer(df, (48 until 58).map(n => // char '0' is 48 in ascii
              Row(Array(n), Array(n, n, n))))
          }
        }
      }
    }
  }

  test("string") {
    val data = (1 to 4).map(i => Tuple1(i.toString))
    // Property spark.sql.parquet.binaryAsString shouldn't affect Parquet files written by Spark SQL
    // as we store Spark SQL schema in the extra metadata.
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "false")(checkParquetFile(data))
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING.key -> "true")(checkParquetFile(data))
  }

  test("SPARK-36182: TimestampNTZ") {
    withSQLConf(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> "true") {
      val data = Seq("2021-01-01T00:00:00", "1970-07-15T01:02:03.456789")
        .map(ts => Tuple1(LocalDateTime.parse(ts)))
      withAllParquetReaders {
        checkParquetFile(data)
      }
    }
  }

  test("Read TimestampNTZ and TimestampLTZ for various logical TIMESTAMP types") {
    val schema = MessageTypeParser.parseMessageType(
      """message root {
        |  required int64 timestamp_ltz_millis_depr(TIMESTAMP_MILLIS);
        |  required int64 timestamp_ltz_micros_depr(TIMESTAMP_MICROS);
        |  required int64 timestamp_ltz_millis(TIMESTAMP(MILLIS,true));
        |  required int64 timestamp_ltz_micros(TIMESTAMP(MICROS,true));
        |  required int64 timestamp_ntz_millis(TIMESTAMP(MILLIS,false));
        |  required int64 timestamp_ntz_micros(TIMESTAMP(MICROS,false));
        |}
      """.stripMargin)

    for (dictEnabled <- Seq(true, false)) {
      withTempDir { dir =>
        val tablePath = new Path(s"${dir.getCanonicalPath}/timestamps.parquet")
        val numRecords = 100

        val writer = createParquetWriter(schema, tablePath, dictionaryEnabled = dictEnabled)
        (0 until numRecords).foreach { i =>
          val record = new SimpleGroup(schema)
          for (group <- Seq(0, 2, 4)) {
            record.add(group, 1000L) // millis
            record.add(group + 1, 1000000L) // micros
          }
          writer.write(record)
        }
        writer.close

        for (inferTimestampNTZ <- Seq(true, false)) {
          withSQLConf(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> s"$inferTimestampNTZ") {
            val timestampNTZType = if (inferTimestampNTZ) TimestampNTZType else TimestampType

            withAllParquetReaders {
              val df = spark.read.parquet(tablePath.toString)
              assertResult(df.schema) {
                StructType(
                  StructField("timestamp_ltz_millis_depr", TimestampType, nullable = true) ::
                  StructField("timestamp_ltz_micros_depr", TimestampType, nullable = true) ::
                  StructField("timestamp_ltz_millis", TimestampType, nullable = true) ::
                  StructField("timestamp_ltz_micros", TimestampType, nullable = true) ::
                  StructField("timestamp_ntz_millis", timestampNTZType, nullable = true) ::
                  StructField("timestamp_ntz_micros", timestampNTZType, nullable = true) ::
                  Nil
                )
              }

              val ltz_value = new java.sql.Timestamp(1000L)
              val ntz_value = LocalDateTime.of(1970, 1, 1, 0, 0, 1)

              val exp = if (inferTimestampNTZ) {
                (0 until numRecords).map { _ =>
                  (ltz_value, ltz_value, ltz_value, ltz_value, ntz_value, ntz_value)
                }.toDF()
              } else {
                (0 until numRecords).map { _ =>
                  (ltz_value, ltz_value, ltz_value, ltz_value, ltz_value, ltz_value)
                }.toDF()
              }

              checkAnswer(df, exp)
            }
          }
        }
      }
    }
  }

  test("Write TimestampNTZ type") {
    // The configuration PARQUET_INFER_TIMESTAMP_NTZ_ENABLED doesn't affect the behavior of writes.
    Seq(true, false).foreach { inferTimestampNTZ =>
      withSQLConf(SQLConf.PARQUET_INFER_TIMESTAMP_NTZ_ENABLED.key -> inferTimestampNTZ.toString) {
        withTempPath { dir =>
          val data = Seq(LocalDateTime.parse("2021-01-01T00:00:00")).toDF("col")
          data.write.parquet(dir.getCanonicalPath)
          assertResult(spark.read.parquet(dir.getCanonicalPath).schema) {
            StructType(
              StructField("col", TimestampNTZType, nullable = true) ::
                Nil
            )
          }
        }
      }
    }
  }

  testStandardAndLegacyModes("fixed-length decimals") {
    def makeDecimalRDD(decimal: DecimalType): DataFrame = {
      spark
        .range(1000)
        // Parquet doesn't allow column names with spaces, have to add an alias here.
        // Minus 500 here so that negative decimals are also tested.
        .select((($"id" - 500) / 100.0) cast decimal as Symbol("dec"))
        .coalesce(1)
    }

    var combinations = Seq((5, 2), (1, 0), (18, 10), (18, 17), (19, 0), (38, 37))
    // If ANSI mode is on, the combination (1, 1) will cause a runtime error. Otherwise, the
    // decimal RDD contains all null values and should be able to read back from Parquet.
    if (!SQLConf.get.ansiEnabled) {
      combinations = combinations++ Seq((1, 1))
    }
    for ((precision, scale) <- combinations) {
      withTempPath { dir =>
        val data = makeDecimalRDD(DecimalType(precision, scale))
        data.write.parquet(dir.getCanonicalPath)
        readParquetFile(dir.getCanonicalPath) { df => {
          checkAnswer(df, data.collect().toSeq)
        }}
      }
    }
  }

  test("date type") {
    def makeDateRDD(): DataFrame =
      sparkContext
        .parallelize(0 to 1000)
        .map(i => Tuple1(DateTimeUtils.toJavaDate(i)))
        .toDF()
        .select($"_1")

    withTempPath { dir =>
      val data = makeDateRDD()
      data.write.parquet(dir.getCanonicalPath)
      readParquetFile(dir.getCanonicalPath) { df =>
        checkAnswer(df, data.collect().toSeq)
      }
    }
  }

  testStandardAndLegacyModes("map") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> s"val_$i")))
    checkParquetFile(data)
  }

  testStandardAndLegacyModes("array") {
    val data = (1 to 4).map(i => Tuple1(Seq(i, i + 1)))
    checkParquetFile(data)
  }

  testStandardAndLegacyModes("array and double") {
    val data = (1 to 4).map(i => (i.toDouble, Seq(i.toDouble, (i + 1).toDouble)))
    checkParquetFile(data)
  }

  testStandardAndLegacyModes("struct") {
    val data = (1 to 4).map(i => Tuple1((i, s"val_$i")))
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(struct) =>
        Row(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  testStandardAndLegacyModes("array of struct") {
    val data = (1 to 4).map { i =>
      Tuple1(
        Seq(
          Tuple1(s"1st_val_$i"),
          Tuple1(s"2nd_val_$i")
        )
      )
    }
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(array) =>
        Row(array.map(struct => Row(struct.productIterator.toSeq: _*)))
      })
    }
  }

  testStandardAndLegacyModes("array of nested struct") {
    val data = (1 to 4).map { i =>
      Tuple1(
        Seq(
          Tuple1(
            Tuple1(s"1st_val_$i")),
          Tuple1(
            Tuple1(s"2nd_val_$i"))
        )
      )
    }
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(array) =>
        Row(array.map { case Tuple1(Tuple1(str)) => Row(Row(str))})
      })
    }
  }

  testStandardAndLegacyModes("nested struct with array of array as field") {
    val data = (1 to 4).map(i => Tuple1((i, Seq(Seq(s"val_$i")))))
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(struct) =>
        Row(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  testStandardAndLegacyModes("nested map with struct as key type") {
    val data = (1 to 4).map { i =>
      Tuple1(
        Map(
          (i, s"kA_$i") -> s"vA_$i",
          (i, s"kB_$i") -> s"vB_$i"
        )
      )
    }
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(m) =>
        Row(m.map { case (k, v) => Row(k.productIterator.toSeq: _*) -> v })
      })
    }
  }

  testStandardAndLegacyModes("nested map with struct as value type") {
    val data = (1 to 4).map { i =>
      Tuple1(
        Map(
          s"kA_$i" -> ((i, s"vA_$i")),
          s"kB_$i" -> ((i, s"vB_$i"))
        )
      )
    }
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(m) =>
        Row(m.transform((_, struct) => Row(struct.productIterator.toSeq: _*)))
      })
    }
  }

  test("nulls") {
    val allNulls = (
      null.asInstanceOf[java.lang.Boolean],
      null.asInstanceOf[Integer],
      null.asInstanceOf[java.lang.Long],
      null.asInstanceOf[java.lang.Float],
      null.asInstanceOf[java.lang.Double])

    withParquetDataFrame(allNulls :: Nil) { df =>
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows.head === Row(Seq.fill(5)(null): _*))
    }
  }

  test("nones") {
    val allNones = (
      None.asInstanceOf[Option[Int]],
      None.asInstanceOf[Option[Long]],
      None.asInstanceOf[Option[String]])

    withParquetDataFrame(allNones :: Nil) { df =>
      val rows = df.collect()
      assert(rows.length === 1)
      assert(rows.head === Row(Seq.fill(3)(null): _*))
    }
  }

  test("vectorized reader: array") {
    val data = Seq(
      Tuple1(null),
      Tuple1(Seq()),
      Tuple1(Seq("a", "b", "c")),
      Tuple1(Seq(null))
    )

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(df.sort("_1"),
          Row(null) :: Row(Seq()) :: Row(Seq(null)) :: Row(Seq("a", "b", "c")) :: Nil
        )
      }
    }
  }

  test("vectorized reader: missing array") {
    Seq(true, false).foreach { offheapEnabled =>
      withSQLConf(
          SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString) {
        val data = Seq(
          Tuple1(null),
          Tuple1(Seq()),
          Tuple1(Seq("a", "b", "c")),
          Tuple1(Seq(null))
        )

        val readSchema = new StructType().add("_2", new ArrayType(
          new StructType().add("a", LongType, nullable = true),
          containsNull = true)
        )

        withParquetFile(data) { file =>
          checkAnswer(spark.read.schema(readSchema).parquet(file),
            Row(null) :: Row(null) :: Row(null) :: Row(null) :: Nil
          )
        }
      }
    }
  }

  test("vectorized reader: array of array") {
    val data = Seq(
      Tuple1(Seq(Seq(0, 1), Seq(2, 3))),
      Tuple1(Seq(Seq(4, 5), Seq(6, 7)))
    )

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(df.sort("_1"),
          Row(Seq(Seq(0, 1), Seq(2, 3))) :: Row(Seq(Seq(4, 5), Seq(6, 7))) :: Nil
        )
      }
    }
  }

  test("vectorized reader: struct of array") {
    val data = Seq(
      Tuple1(Tuple2("a", null)),
      Tuple1(null),
      Tuple1(Tuple2(null, null)),
      Tuple1(Tuple2(null, Seq("b", "c"))),
      Tuple1(Tuple2("d", Seq("e", "f"))),
      Tuple1(null)
    )

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(df,
          Row(Row("a", null)) :: Row(null) :: Row(Row(null, null)) ::
              Row(Row(null, Seq("b", "c"))) :: Row(Row("d", Seq("e", "f"))) :: Row(null) :: Nil
        )
      }
    }
  }

  test("vectorized reader: array of struct") {
    val data = Seq(
      Tuple1(null),
      Tuple1(Seq()),
      Tuple1(Seq(Tuple2("a", null), Tuple2(null, "b"))),
      Tuple1(Seq(null)),
      Tuple1(Seq(Tuple2(null, null), Tuple2("c", null), null)),
      Tuple1(Seq())
    )

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(df,
          Row(null) ::
              Row(Seq()) ::
              Row(Seq(Row("a", null), Row(null, "b"))) ::
              Row(Seq(null)) ::
              Row(Seq(Row(null, null), Row("c", null), null)) ::
              Row(Seq()) ::
              Nil)
      }
    }
  }


  test("vectorized reader: array of nested struct") {
    val data = Seq(
      Tuple1(Tuple2("a", null)),
      Tuple1(Tuple2("b", Seq(Tuple2("c", "d")))),
      Tuple1(null),
      Tuple1(Tuple2("e", Seq(Tuple2("f", null), Tuple2(null, "g")))),
      Tuple1(Tuple2(null, null)),
      Tuple1(Tuple2(null, Seq(null))),
      Tuple1(Tuple2(null, Seq(Tuple2(null, null), Tuple2("h", null), null))),
      Tuple1(Tuple2("i", Seq())),
      Tuple1(null)
    )

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(df,
          Row(Row("a", null)) ::
              Row(Row("b", Seq(Row("c", "d")))) ::
              Row(null) ::
              Row(Row("e", Seq(Row("f", null), Row(null, "g")))) ::
              Row(Row(null, null)) ::
              Row(Row(null, Seq(null))) ::
              Row(Row(null, Seq(Row(null, null), Row("h", null), null))) ::
              Row(Row("i", Seq())) ::
              Row(null) ::
              Nil)
      }
    }
  }

  test("vectorized reader: required array with required elements") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path, expected: Seq[Seq[String]]): Unit = {
        val schemaStr =
          """message spark_schema {
            |  required group _1 (LIST) {
            |    repeated group list {
            |      required binary element (UTF8);
            |    }
            |  }
            |}
             """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)
        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        val factory = new SimpleGroupFactory(schema)
        expected.foreach { values =>
          val group = factory.newGroup()
          val list = group.addGroup(0)
          values.foreach { value =>
            list.addGroup(0).append("element", value)
          }
          writer.write(group)
        }
        writer.close()
      }

      // write the following into the Parquet file:
      //   0: [ "a", "b" ]
      //   1: [ ]
      //   2: [ "c", "d" ]
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = Seq(Seq("a", "b"), Seq(), Seq("c", "d"))
        makeRawParquetFile(path, expected)
        readParquetFile(path.toString) { df => checkAnswer(df, expected.map(Row(_))) }
      }
    }
  }

  test("vectorized reader: optional array with required elements") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path, expected: Seq[Seq[String]]): Unit = {
        val schemaStr =
          """message spark_schema {
            |  optional group _1 (LIST) {
            |    repeated group list {
            |      required binary element (UTF8);
            |    }
            |  }
            |}
             """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)
        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        val factory = new SimpleGroupFactory(schema)
        expected.foreach { values =>
          val group = factory.newGroup()
          if (values != null) {
            val list = group.addGroup(0)
            values.foreach { value =>
              list.addGroup(0).append("element", value)
            }
          }
          writer.write(group)
        }
        writer.close()
      }

      // write the following into the Parquet file:
      //   0: [ "a", "b" ]
      //   1: null
      //   2: [ "c", "d" ]
      //   3: [ ]
      //   4: [ "e", "f" ]
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = Seq(Seq("a", "b"), null, Seq("c", "d"), Seq(), Seq("e", "f"))
        makeRawParquetFile(path, expected)
        readParquetFile(path.toString) { df => checkAnswer(df, expected.map(Row(_))) }
      }
    }
  }

  test("vectorized reader: required array with optional elements") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path, expected: Seq[Seq[String]]): Unit = {
        val schemaStr =
          """message spark_schema {
            |  required group _1 (LIST) {
            |    repeated group list {
            |      optional binary element (UTF8);
            |    }
            |  }
            |}
             """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)
        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        val factory = new SimpleGroupFactory(schema)
        expected.foreach { values =>
          val group = factory.newGroup()
          if (values != null) {
            val list = group.addGroup(0)
            values.foreach { value =>
              val group = list.addGroup(0)
              if (value != null) group.append("element", value)
            }
          }
          writer.write(group)
        }
        writer.close()
      }

      // write the following into the Parquet file:
      //   0: [ "a", null ]
      //   3: [ ]
      //   4: [ null, "b" ]
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = Seq(Seq("a", null), Seq(), Seq(null, "b"))
        makeRawParquetFile(path, expected)
        readParquetFile(path.toString) { df => checkAnswer(df, expected.map(Row(_))) }
      }
    }
  }

  test("vectorized reader: required array with legacy format") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path, expected: Seq[Seq[String]]): Unit = {
        val schemaStr =
          """message spark_schema {
            |  repeated binary element (UTF8);
            |}
             """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)
        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        val factory = new SimpleGroupFactory(schema)
        expected.foreach { values =>
          val group = factory.newGroup()
          values.foreach(group.append("element", _))
          writer.write(group)
        }
        writer.close()
      }

      // write the following into the Parquet file:
      //   0: [ "a", "b" ]
      //   3: [ ]
      //   4: [ "c", "d" ]
      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        val expected = Seq(Seq("a", "b"), Seq(), Seq("c", "d"))
        makeRawParquetFile(path, expected)
        readParquetFile(path.toString) { df => checkAnswer(df, expected.map(Row(_))) }
      }
    }
  }

  test("vectorized reader: struct") {
    val data = Seq(
      Tuple1(null),
      Tuple1((1, "a")),
      Tuple1((2, null)),
      Tuple1((3, "b")),
      Tuple1(null)
    )

    withParquetFile(data) { file =>
      readParquetFile(file) { df =>
        checkAnswer(df.sort("_1"),
          Row(null) :: Row(null) :: Row(Row(1, "a")) :: Row(Row(2, null)) :: Row(Row(3, "b")) :: Nil
        )
      }
    }
  }

  test("vectorized reader: missing all struct fields") {
    Seq(true, false).foreach { offheapEnabled =>
      withSQLConf(
          SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString) {
        val data = Seq(
          Tuple1((1, "a")),
          Tuple1((2, null)),
          Tuple1(null)
        )

        val readSchema = new StructType().add("_1",
          new StructType()
              .add("_3", IntegerType, nullable = true)
              .add("_4", LongType, nullable = true),
          nullable = true)

        withParquetFile(data) { file =>
          checkAnswer(spark.read.schema(readSchema).parquet(file),
            Row(null) :: Row(null) :: Row(null) :: Nil
          )
        }
      }
    }
  }

  test("vectorized reader: missing some struct fields") {
    Seq(true, false).foreach { offheapEnabled =>
      withSQLConf(
          SQLConf.PARQUET_VECTORIZED_READER_NESTED_COLUMN_ENABLED.key -> "true",
          SQLConf.COLUMN_VECTOR_OFFHEAP_ENABLED.key -> offheapEnabled.toString) {
        val data = Seq(
          Tuple1((1, "a")),
          Tuple1((2, null)),
          Tuple1(null)
        )

        val readSchema = new StructType().add("_1",
          new StructType()
              .add("_1", IntegerType, nullable = true)
              .add("_3", LongType, nullable = true),
          nullable = true)

        withParquetFile(data) { file =>
          checkAnswer(spark.read.schema(readSchema).parquet(file),
            Row(null) :: Row(Row(1, null)) :: Row(Row(2, null)) :: Nil
          )
        }
      }
    }
  }

  test("SPARK-34817: Support for unsigned Parquet logical types") {
    val parquetSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  required INT32 a(UINT_8);
        |  required INT32 b(UINT_16);
        |  required INT32 c(UINT_32);
        |  required INT64 d(UINT_64);
        |}
      """.stripMargin)

    val expectedSparkTypes = Seq(ShortType, IntegerType, LongType, DecimalType.LongDecimal)

    withTempPath { location =>
      val path = new Path(location.getCanonicalPath)
      val conf = spark.sessionState.newHadoopConf()
      writeMetadata(parquetSchema, path, conf)
      val sparkTypes = spark.read.parquet(path.toString).schema.map(_.dataType)
      assert(sparkTypes === expectedSparkTypes)
    }
  }

  test("SPARK-11692 Support for Parquet logical types, JSON and BSON (embedded types)") {
    val parquetSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  required binary a(JSON);
        |  required binary b(BSON);
        |}
      """.stripMargin)

    val expectedSparkTypes = Seq(StringType, BinaryType)

    withTempPath { location =>
      val path = new Path(location.getCanonicalPath)
      val conf = spark.sessionState.newHadoopConf()
      writeMetadata(parquetSchema, path, conf)
      val sparkTypes = spark.read.parquet(path.toString).schema.map(_.dataType)
      assert(sparkTypes === expectedSparkTypes)
    }
  }

  test("compression codec") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    def compressionCodecFor(path: String, codecName: String): String = {
      val codecs = for {
        footer <- readAllFootersWithoutSummaryFiles(new Path(path), hadoopConf)
        block <- footer.getParquetMetadata.getBlocks.asScala
        column <- block.getColumns.asScala
      } yield column.getCodec.name()

      assert(codecs.distinct === Seq(codecName))
      codecs.head
    }

    val data = (0 until 10).map(i => (i, i.toString))

    def checkCompressionCodec(codec: ParquetCompressionCodec): Unit = {
      withSQLConf(SQLConf.PARQUET_COMPRESSION.key -> codec.name()) {
        withParquetFile(data) { path =>
          assertResult(spark.conf.get(SQLConf.PARQUET_COMPRESSION).toUpperCase(Locale.ROOT)) {
            compressionCodecFor(path, codec.name())
          }
        }
      }
    }

    // Checks default compression codec
    checkCompressionCodec(
      ParquetCompressionCodec.fromString(spark.conf.get(SQLConf.PARQUET_COMPRESSION)))

    ParquetCompressionCodec.availableCodecs.asScala.foreach(checkCompressionCodec(_))
  }

  private def createParquetWriter(
      schema: MessageType,
      path: Path,
      dictionaryEnabled: Boolean = false,
      pageSize: Int = 1024,
      dictionaryPageSize: Int = 1024): ParquetWriter[Group] = {
    val hadoopConf = spark.sessionState.newHadoopConf()

    ExampleParquetWriter
      .builder(path)
      .withDictionaryEncoding(dictionaryEnabled)
      .withType(schema)
      .withWriterVersion(PARQUET_1_0)
      .withCompressionCodec(ParquetCompressionCodec.GZIP.getCompressionCodec)
      .withRowGroupSize(1024 * 1024)
      .withPageSize(pageSize)
      .withDictionaryPageSize(dictionaryPageSize)
      .withConf(hadoopConf)
      .build()
  }

  test("SPARK-34859: test multiple pages with different sizes and nulls") {
    def makeRawParquetFile(
        path: Path,
        dictionaryEnabled: Boolean,
        n: Int,
        pageSize: Int): Seq[Option[Int]] = {
      val schemaStr =
        """
          |message root {
          |  optional boolean _1;
          |  optional int32   _2;
          |  optional int64   _3;
          |  optional float   _4;
          |  optional double  _5;
          |}
        """.stripMargin

      val schema = MessageTypeParser.parseMessageType(schemaStr)
      val writer = createParquetWriter(schema, path,
        dictionaryEnabled = dictionaryEnabled, pageSize = pageSize, dictionaryPageSize = pageSize)

      val rand = scala.util.Random
      val expected = (0 until n).map { i =>
        if (rand.nextBoolean()) {
          None
        } else {
          Some(i)
        }
      }
      expected.foreach { opt =>
        val record = new SimpleGroup(schema)
        opt match {
          case Some(i) =>
            record.add(0, i % 2 == 0)
            record.add(1, i)
            record.add(2, i.toLong)
            record.add(3, i.toFloat)
            record.add(4, i.toDouble)
          case _ =>
        }
        writer.write(record)
      }

      writer.close()
      expected
    }

    Seq(true, false).foreach { dictionaryEnabled =>
      Seq(64, 128, 89).foreach { pageSize =>
        withTempDir { dir =>
          val path = new Path(dir.toURI.toString, "part-r-0.parquet")
          val expected = makeRawParquetFile(path, dictionaryEnabled, 1000, pageSize)
          readParquetFile(path.toString) { df =>
            checkAnswer(df, expected.map {
              case None =>
                Row(null, null, null, null, null)
              case Some(i) =>
                Row(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
            })
          }
        }
      }
    }
  }

  test("read raw Parquet file") {
    def makeRawParquetFile(path: Path): Unit = {
      val schemaStr =
        """
          |message root {
          |  required boolean _1;
          |  required int32   _2;
          |  required int64   _3;
          |  required float   _4;
          |  required double  _5;
          |}
        """.stripMargin
      val schema = MessageTypeParser.parseMessageType(schemaStr)


      val writer = createParquetWriter(schema, path)

      (0 until 10).foreach { i =>
        val record = new SimpleGroup(schema)
        record.add(0, i % 2 == 0)
        record.add(1, i)
        record.add(2, i.toLong)
        record.add(3, i.toFloat)
        record.add(4, i.toDouble)
        writer.write(record)
      }

      writer.close()
    }

    withTempDir { dir =>
      val path = new Path(dir.toURI.toString, "part-r-0.parquet")
      makeRawParquetFile(path)
      readParquetFile(path.toString) { df =>
        checkAnswer(df, (0 until 10).map { i =>
          Row(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble) })
      }
    }
  }

  test("SPARK-34817: Read UINT_8/UINT_16/UINT_32 from parquet") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required INT32 a(UINT_8);
            |  required INT32 b(UINT_16);
            |  required INT32 c(UINT_32);
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        val factory = new SimpleGroupFactory(schema)
        (0 until 1000).foreach { i =>
          val group = factory.newGroup()
            .append("a", i % 100 + Byte.MaxValue)
            .append("b", i % 100 + Short.MaxValue)
            .append("c", i % 100 + Int.MaxValue)
          writer.write(group)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        readParquetFile(path.toString) { df =>
          checkAnswer(df, (0 until 1000).map { i =>
            Row(i % 100 + Byte.MaxValue,
              i % 100 + Short.MaxValue,
              i % 100 + Int.MaxValue.toLong)
          })
        }
      }
    }
  }

  test("SPARK-34817: Read UINT_64 as Decimal from parquet") {
    Seq(true, false).foreach { dictionaryEnabled =>
      def makeRawParquetFile(path: Path): Unit = {
        val schemaStr =
          """message root {
            |  required INT64 a(UINT_64);
            |}
        """.stripMargin
        val schema = MessageTypeParser.parseMessageType(schemaStr)

        val writer = createParquetWriter(schema, path, dictionaryEnabled)

        val factory = new SimpleGroupFactory(schema)
        (-500 until 500).foreach { i =>
          val group = factory.newGroup()
            .append("a", i % 100L)
          writer.write(group)
        }
        writer.close()
      }

      withTempDir { dir =>
        val path = new Path(dir.toURI.toString, "part-r-0.parquet")
        makeRawParquetFile(path)
        readParquetFile(path.toString) { df =>
          checkAnswer(df, (-500 until 500).map { i =>
            val bi = UnsignedLong.fromLongBits(i % 100L).bigIntegerValue()
            Row(new java.math.BigDecimal(bi))
          })
        }
      }
    }
  }

  test("SPARK-35640: read binary as timestamp should throw schema incompatible error") {
    val data = (1 to 4).map(i => Tuple1(i.toString))
    val readSchema = StructType(Seq(StructField("_1", DataTypes.TimestampType)))

    withParquetFile(data) { path =>
      checkErrorMatchPVals(
        exception = intercept[SparkException] {
          spark.read.schema(readSchema).parquet(path).collect()
        },
        condition = "FAILED_READ_FILE.PARQUET_COLUMN_DATA_TYPE_MISMATCH",
        parameters = Map(
          "path" -> ".*",
          "column" -> "\\[_1\\]",
          "expectedType" -> "timestamp",
          "actualType" -> "BINARY"
        )
      )
    }
  }

  test("write metadata") {
    val hadoopConf = spark.sessionState.newHadoopConf()
    withTempPath { file =>
      val path = new Path(file.toURI.toString)
      val fs = FileSystem.getLocal(hadoopConf)
      val schema = schemaFor[(Int, String)]
      writeMetadata(schema, path, hadoopConf)

      assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)))
      assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)))

      val expectedSchema = new SparkToParquetSchemaConverter().convert(schema)
      val actualSchema = readFooter(path, hadoopConf).getFileMetaData.getSchema

      actualSchema.checkContains(expectedSchema)
      expectedSchema.checkContains(actualSchema)
    }
  }

  test("save - overwrite") {
    withParquetFile((1 to 10).map(i => (i, i.toString))) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().write.format("parquet").mode(SaveMode.Overwrite).save(file)
      readParquetFile(file) { df =>
        checkAnswer(df, newData.map(Row.fromTuple))
      }
    }
  }

  test("save - ignore") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().write.format("parquet").mode(SaveMode.Ignore).save(file)
      readParquetFile(file) { df =>
        checkAnswer(df, data.map(Row.fromTuple))
      }
    }
  }

  test("save - throw") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      val errorMessage = intercept[Throwable] {
        newData.toDF().write.format("parquet").mode(SaveMode.ErrorIfExists).save(file)
      }.getMessage
      assert(errorMessage.contains("already exists"))
    }
  }

  test("save - append") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().write.format("parquet").mode(SaveMode.Append).save(file)
      readParquetFile(file) { df =>
        checkAnswer(df, (data ++ newData).map(Row.fromTuple))
      }
    }
  }

  test("SPARK-6315 regression test") {
    // Spark 1.1 and prior versions write Spark schema as case class string into Parquet metadata.
    // This has been deprecated by JSON format since 1.2.  Notice that, 1.3 further refactored data
    // types API, and made StructType.fields an array.  This makes the result of StructType.toString
    // different from prior versions: there's no "Seq" wrapping the fields part in the string now.
    val sparkSchema =
      "StructType(Seq(StructField(a,BooleanType,false),StructField(b,IntegerType,false)))"

    // The Parquet schema is intentionally made different from the Spark schema.  Because the new
    // Parquet data source simply falls back to the Parquet schema once it fails to parse the Spark
    // schema.  By making these two different, we are able to assert the old style case class string
    // is parsed successfully.
    val parquetSchema = MessageTypeParser.parseMessageType(
      """message root {
        |  required int32 c;
        |}
      """.stripMargin)

    withTempPath { location =>
      val extraMetadata = Map(ParquetReadSupport.SPARK_METADATA_KEY -> sparkSchema.toString)
      val path = new Path(location.getCanonicalPath)
      val conf = spark.sessionState.newHadoopConf()
      writeMetadata(parquetSchema, path, conf, extraMetadata)

      readParquetFile(path.toString) { df =>
        assertResult(df.schema) {
          StructType(
            StructField("a", BooleanType, nullable = true) ::
              StructField("b", IntegerType, nullable = true) ::
              Nil)
        }
      }
    }
  }

  test("SPARK-8121: spark.sql.parquet.output.committer.class shouldn't be overridden") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName) {
      val extraOptions = Map(
        SQLConf.OUTPUT_COMMITTER_CLASS.key -> classOf[ParquetOutputCommitter].getCanonicalName,
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key ->
          classOf[JobCommitFailureParquetOutputCommitter].getCanonicalName
      )
      withTempPath { dir =>
        val message = intercept[RuntimeException] {
          spark.range(0, 1).write.options(extraOptions).parquet(dir.getCanonicalPath)
        }.getMessage
        assert(message === "Intentional exception for testing purposes")
      }
    }
  }

  test("SPARK-6330 regression test") {
    // In 1.3.0, save to fs other than file: without configuring core-site.xml would get:
    // IllegalArgumentException: Wrong FS: hdfs://..., expected: file:///
    intercept[Throwable] {
      spark.read.parquet("file:///nonexistent")
    }
    val errorMessage = intercept[Throwable] {
      spark.read.parquet("hdfs://nonexistent")
    }.toString
    assert(errorMessage.contains("UnknownHostException") ||
        errorMessage.contains("is not a valid DFS filename"))
  }

  test("SPARK-7837 Do not close output writer twice when commitTask() fails") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
      classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName) {
      // Using a output committer that always fail when committing a task, so that both
      // `commitTask()` and `abortTask()` are invoked.
      val extraOptions = Map[String, String](
        SQLConf.PARQUET_OUTPUT_COMMITTER_CLASS.key ->
          classOf[TaskCommitFailureParquetOutputCommitter].getCanonicalName
      )

      // Before fixing SPARK-7837, the following code results in an NPE because both
      // `commitTask()` and `abortTask()` try to close output writers.

      withTempPath { dir =>
        val m1 = intercept[SparkException] {
          spark.range(1).coalesce(1).write.options(extraOptions).parquet(dir.getCanonicalPath)
        }
        assert(m1.getErrorClass == "TASK_WRITE_FAILED")
        assert(m1.getCause.getMessage.contains("Intentional exception for testing purposes"))
      }

      withTempPath { dir =>
        val m2 = intercept[SparkException] {
          val df = spark.range(1).select($"id" as Symbol("a"), $"id" as Symbol("b"))
            .coalesce(1)
          df.write.partitionBy("a").options(extraOptions).parquet(dir.getCanonicalPath)
        }
        if (m2.getErrorClass != null) {
          assert(m2.getErrorClass == "TASK_WRITE_FAILED")
          assert(m2.getCause.getMessage.contains("Intentional exception for testing purposes"))
        } else {
          assert(m2.getMessage.contains("TASK_WRITE_FAILED"))
        }
      }
    }
  }

  test("SPARK-11044 Parquet writer version fixed as version1 ") {
    withSQLConf(SQLConf.FILE_COMMIT_PROTOCOL_CLASS.key ->
        classOf[SQLHadoopMapReduceCommitProtocol].getCanonicalName) {
      // For dictionary encoding, Parquet changes the encoding types according to its writer
      // version. So, this test checks one of the encoding types in order to ensure that
      // the file is written with writer version2.
      val extraOptions = Map[String, String](
        // Write a Parquet file with writer version2.
        ParquetOutputFormat.WRITER_VERSION -> ParquetProperties.WriterVersion.PARQUET_2_0.toString,
        // By default, dictionary encoding is enabled from Parquet 1.2.0 but
        // it is enabled just in case.
        ParquetOutputFormat.ENABLE_DICTIONARY -> "true"
      )

      val hadoopConf = spark.sessionState.newHadoopConfWithOptions(extraOptions)

      withSQLConf(ParquetOutputFormat.JOB_SUMMARY_LEVEL -> "ALL") {
        withTempPath { dir =>
          val path = s"${dir.getCanonicalPath}/part-r-0.parquet"
          spark.range(1 << 16).selectExpr("(id % 4) AS i")
            .coalesce(1).write.options(extraOptions).mode("overwrite").parquet(path)

          val blockMetadata = readFooter(new Path(path), hadoopConf).getBlocks.asScala.head
          val columnChunkMetadata = blockMetadata.getColumns.asScala.head

          // If the file is written with version2, this should include
          // Encoding.RLE_DICTIONARY type. For version1, it is Encoding.PLAIN_DICTIONARY
          assert(columnChunkMetadata.getEncodings.contains(Encoding.RLE_DICTIONARY))
        }
      }
    }
  }

  test("null and non-null strings") {
    // Create a dataset where the first values are NULL and then some non-null values. The
    // number of non-nulls needs to be bigger than the ParquetReader batch size.
    val data: Dataset[String] = spark.range(200).map (i =>
      if (i < 150) null
      else "a"
    )
    val df = data.toDF("col")
    assert(df.agg("col" -> "count").collect().head.getLong(0) == 50)

    withTempPath { dir =>
      val path = s"${dir.getCanonicalPath}/data"
      df.write.parquet(path)

      readParquetFile(path) { df2 =>
        assert(df2.agg("col" -> "count").collect().head.getLong(0) == 50)
      }
    }
  }

  test("read dictionary encoded decimals written as INT32") {
    withAllParquetReaders {
      checkAnswer(
        // Decimal column in this file is encoded using plain dictionary
        readResourceParquetFile("test-data/dec-in-i32.parquet"),
        spark.range(1 << 4).select($"id" % 10 cast DecimalType(5, 2) as Symbol("i32_dec")))
    }
  }

  test("read dictionary encoded decimals written as INT64") {
    withAllParquetReaders {
      checkAnswer(
        // Decimal column in this file is encoded using plain dictionary
        readResourceParquetFile("test-data/dec-in-i64.parquet"),
        spark.range(1 << 4).select($"id" % 10 cast DecimalType(10, 2) as Symbol("i64_dec")))
    }
  }

  test("read dictionary encoded decimals written as FIXED_LEN_BYTE_ARRAY") {
    withAllParquetReaders {
      checkAnswer(
        // Decimal column in this file is encoded using plain dictionary
        readResourceParquetFile("test-data/dec-in-fixed-len.parquet"),
        spark.range(1 << 4)
          .select($"id" % 10 cast DecimalType(10, 2) as Symbol("fixed_len_dec")))
    }
  }

  test("read dictionary and plain encoded timestamp_millis written as INT64") {
    withAllParquetReaders {
      checkAnswer(
        // timestamp column in this file is encoded using combination of plain
        // and dictionary encodings.
        readResourceParquetFile("test-data/timemillis-in-i64.parquet"),
        (1 to 3).map(i => Row(new java.sql.Timestamp(10))))
    }
  }

  test("SPARK-40128 read DELTA_LENGTH_BYTE_ARRAY encoded strings") {
    withAllParquetReaders {
      checkAnswer(
        // "fruit" column in this file is encoded using DELTA_LENGTH_BYTE_ARRAY.
        // The file comes from https://github.com/apache/parquet-testing
        readResourceParquetFile("test-data/delta_length_byte_array.parquet"),
        (0 to 999).map(i => Row("apple_banana_mango" + Integer.toString(i * i))))
    }
  }

  test("SPARK-12589 copy() on rows returned from reader works for strings") {
    withTempPath { dir =>
      val data = (1, "abc") ::(2, "helloabcde") :: Nil
      data.toDF().write.parquet(dir.getCanonicalPath)
      var hash1: Int = 0
      var hash2: Int = 0
      (false :: true :: Nil).foreach { v =>
        withSQLConf(SQLConf.PARQUET_VECTORIZED_READER_ENABLED.key -> v.toString) {
          val df = spark.read.parquet(dir.getCanonicalPath)
          val rows = df.queryExecution.toRdd.map(_.copy()).collect()
          val unsafeRows = rows.map(_.asInstanceOf[UnsafeRow])
          if (!v) {
            hash1 = unsafeRows(0).hashCode()
            hash2 = unsafeRows(1).hashCode()
          } else {
            assert(hash1 == unsafeRows(0).hashCode())
            assert(hash2 == unsafeRows(1).hashCode())
          }
        }
      }
    }
  }

  test("SPARK-36726: test incorrect Parquet row group file offset") {
    readParquetFile(testFile("test-data/malformed-file-offset.parquet")) { df =>
      assert(df.count() == 3650)
    }
  }

  test("VectorizedParquetRecordReader - direct path read") {
    val data = (0 to 10).map(i => (i, (i + 'a').toChar.toString))
    withTempPath { dir =>
      spark.createDataFrame(data).repartition(1).write.parquet(dir.getCanonicalPath)
      val file = TestUtils.listDirectory(dir).head;
      {
        val conf = spark.sessionState.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        try {
          reader.initialize(file, null)
          val result = mutable.ArrayBuffer.empty[(Int, String)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            val v = (row.getInt(0), row.getString(1))
            result += v
          }
          assert(data.toSet == result.toSet)
        } finally {
          reader.close()
        }
      }

      // Project just one column
      {
        val conf = spark.sessionState.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        try {
          reader.initialize(file, ("_2" :: Nil).asJava)
          val result = mutable.ArrayBuffer.empty[(String)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            result += row.getString(0)
          }
          assert(data.map(_._2).toSet == result.toSet)
        } finally {
          reader.close()
        }
      }

      // Project columns in opposite order
      {
        val conf = spark.sessionState.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        try {
          reader.initialize(file, ("_2" :: "_1" :: Nil).asJava)
          val result = mutable.ArrayBuffer.empty[(String, Int)]
          while (reader.nextKeyValue()) {
            val row = reader.getCurrentValue.asInstanceOf[InternalRow]
            val v = (row.getString(0), row.getInt(1))
            result += v
          }
          assert(data.map { x => (x._2, x._1) }.toSet == result.toSet)
        } finally {
          reader.close()
        }
      }

      // Empty projection
      {
        val conf = spark.sessionState.conf
        val reader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        try {
          reader.initialize(file, List[String]().asJava)
          var result = 0
          while (reader.nextKeyValue()) {
            result += 1
          }
          assert(result == data.length)
        } finally {
          reader.close()
        }
      }
    }
  }

  test("VectorizedParquetRecordReader - partition column types") {
    withTempPath { dir =>
      Seq(1).toDF().repartition(1).write.parquet(dir.getCanonicalPath)

      val dataTypes =
        Seq(StringType, BooleanType, ByteType, BinaryType, ShortType, IntegerType, LongType,
          FloatType, DoubleType, DecimalType(25, 5), DateType, TimestampType)

      val constantValues =
        Seq(
          UTF8String.fromString("a string"),
          true,
          1.toByte,
          "Spark SQL".getBytes,
          2.toShort,
          3,
          Long.MaxValue,
          0.25.toFloat,
          0.75D,
          Decimal("1234.23456"),
          DateTimeUtils.fromJavaDate(java.sql.Date.valueOf("2015-01-01")),
          DateTimeUtils.fromJavaTimestamp(java.sql.Timestamp.valueOf("2015-01-01 23:50:59.123")))

      dataTypes.zip(constantValues).foreach { case (dt, v) =>
        val schema = StructType(StructField("pcol", dt) :: Nil)
        val conf = spark.sessionState.conf
        val vectorizedReader = new VectorizedParquetRecordReader(
          conf.offHeapColumnVectorEnabled, conf.parquetVectorizedReaderBatchSize)
        val partitionValues = new GenericInternalRow(Array(v))
        val file = TestUtils.listDirectory(dir).head

        try {
          vectorizedReader.initialize(file, null)
          vectorizedReader.initBatch(schema, partitionValues)
          vectorizedReader.nextKeyValue()
          val row = vectorizedReader.getCurrentValue.asInstanceOf[InternalRow]

          // Use `GenericMutableRow` by explicitly copying rather than `ColumnarBatch`
          // in order to use get(...) method which is not implemented in `ColumnarBatch`.
          val actual = row.copy().get(1, dt)
          val expected = v
          if (dt.isInstanceOf[BinaryType]) {
            assert(actual.asInstanceOf[Array[Byte]] sameElements expected.asInstanceOf[Array[Byte]])
          } else {
            assert(actual == expected)
          }
        } finally {
          vectorizedReader.close()
        }
      }
    }
  }

  test("SPARK-18433: Improve DataSource option keys to be more case-insensitive") {
    withSQLConf(
      SQLConf.PARQUET_COMPRESSION.key -> ParquetCompressionCodec.SNAPPY.lowerCaseName()) {
      val option = new ParquetOptions(
        Map("Compression" -> ParquetCompressionCodec.UNCOMPRESSED.lowerCaseName()),
        spark.sessionState.conf)
      assert(option.compressionCodecClassName == ParquetCompressionCodec.UNCOMPRESSED.name)
    }
  }

  test("SPARK-40667: validate Parquet Options") {
    assert(ParquetOptions.getAllOptions.size == 5)
    // Please add validation on any new parquet options here
    assert(ParquetOptions.isValidOption("mergeSchema"))
    assert(ParquetOptions.isValidOption("compression"))
    assert(ParquetOptions.isValidOption("parquet.compression"))
    assert(ParquetOptions.isValidOption("datetimeRebaseMode"))
    assert(ParquetOptions.isValidOption("int96RebaseMode"))
  }

  test("SPARK-23173 Writing a file with data converted from JSON with and incorrect user schema") {
    withTempPath { file =>
      val jsonData =
        """{
        |  "a": 1,
        |  "c": "foo"
        |}
        |""".stripMargin
      val jsonSchema = new StructType()
        .add("a", LongType, nullable = false)
        .add("b", StringType, nullable = false)
        .add("c", StringType, nullable = false)
      spark.range(1).select(from_json(lit(jsonData), jsonSchema) as "input")
        .write.parquet(file.getAbsolutePath)
      checkAnswer(spark.read.parquet(file.getAbsolutePath), Seq(Row(Row(1, null, "foo"))))
    }
  }

  test("Write Spark version into Parquet metadata") {
    withTempPath { dir =>
      spark.range(1).repartition(1).write.parquet(dir.getAbsolutePath)
      assert(getMetaData(dir)(SPARK_VERSION_METADATA_KEY) === SPARK_VERSION_SHORT)
    }
  }

  Seq(true, false).foreach { vec =>
    test(s"SPARK-34167: read LongDecimals with precision < 10, VectorizedReader $vec") {
      // decimal32-written-as-64-bit.snappy.parquet was generated using a 3rd-party library. It has
      // 10 rows of Decimal(9, 1) written as LongDecimal instead of an IntDecimal
      readParquetFile(testFile("test-data/decimal32-written-as-64-bit.snappy.parquet"), vec) {
        df =>
          assert(10 == df.collect().length)
          val first10Df = df.head(10)
          assert(
            Seq(792059492, 986842987, 540247998, null, 357991078,
              494131059, 92536396, 426847157, -999999999, 204486094)
              .zip(first10Df).forall(d =>
              d._2.isNullAt(0) && d._1 == null ||
                d._1 == d._2.getDecimal(0).unscaledValue().intValue()
            ))
      }
      // decimal32-written-as-64-bit-dict.snappy.parquet was generated using a 3rd-party library. It
      // has 2048 rows of Decimal(3, 1) written as LongDecimal instead of an IntDecimal
      readParquetFile(
        testFile("test-data/decimal32-written-as-64-bit-dict.snappy.parquet"), vec) {
        df =>
          assert(2048 == df.collect().length)
          val first10Df = df.head(10)
          assert(Seq(751, 937, 511, null, 337, 467, 84, 403, -999, 190)
            .zip(first10Df).forall(d =>
            d._2.isNullAt(0) && d._1 == null ||
              d._1 == d._2.getDecimal(0).unscaledValue().intValue()))

          val last10Df = df.tail(10)
          assert(Seq(866, 20, 492, 76, 824, 604, 343, 820, 864, 243)
            .zip(last10Df).forall(d =>
            d._1 == d._2.getDecimal(0).unscaledValue().intValue()))
      }
    }
  }
}

class JobCommitFailureParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitJob(jobContext: JobContext): Unit = {
    sys.error("Intentional exception for testing purposes")
  }
}

class TaskCommitFailureParquetOutputCommitter(outputPath: Path, context: TaskAttemptContext)
  extends ParquetOutputCommitter(outputPath, context) {

  override def commitTask(context: TaskAttemptContext): Unit = {
    sys.error("Intentional exception for testing purposes")
  }
}
