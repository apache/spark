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

import scala.collection.JavaConversions._
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileSystem, Path}
import org.scalatest.BeforeAndAfterAll
import parquet.example.data.simple.SimpleGroup
import parquet.example.data.{Group, GroupWriter}
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.hadoop.metadata.{ParquetMetadata, FileMetaData, CompressionCodecName}
import parquet.hadoop.{Footer, ParquetFileWriter, ParquetWriter}
import parquet.io.api.RecordConsumer
import parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.test.TestSQLContext.implicits._
import org.apache.spark.sql.types._
import org.apache.spark.sql.{DataFrame, QueryTest, SQLConf, SaveMode}

// Write support class for nested groups: ParquetWriter initializes GroupWriteSupport
// with an empty configuration (it is after all not intended to be used in this way?)
// and members are private so we need to make our own in order to pass the schema
// to the writer.
private[parquet] class TestGroupWriteSupport(schema: MessageType) extends WriteSupport[Group] {
  var groupWriter: GroupWriter = null

  override def prepareForWrite(recordConsumer: RecordConsumer): Unit = {
    groupWriter = new GroupWriter(recordConsumer, schema)
  }

  override def init(configuration: Configuration): WriteContext = {
    new WriteContext(schema, new java.util.HashMap[String, String]())
  }

  override def write(record: Group) {
    groupWriter.write(record)
  }
}

/**
 * A test suite that tests basic Parquet I/O.
 */
class ParquetIOSuiteBase extends QueryTest with ParquetTest {
  val sqlContext = TestSQLContext

  import sqlContext.implicits.localSeqToDataFrameHolder

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

  test("string") {
    val data = (1 to 4).map(i => Tuple1(i.toString))
    // Property spark.sql.parquet.binaryAsString shouldn't affect Parquet files written by Spark SQL
    // as we store Spark SQL schema in the extra metadata.
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING -> "false")(checkParquetFile(data))
    withSQLConf(SQLConf.PARQUET_BINARY_AS_STRING -> "true")(checkParquetFile(data))
  }

  test("fixed-length decimals") {

    def makeDecimalRDD(decimal: DecimalType): DataFrame =
      sparkContext
        .parallelize(0 to 1000)
        .map(i => Tuple1(i / 100.0))
        .toDF()
        // Parquet doesn't allow column names with spaces, have to add an alias here
        .select($"_1" cast decimal as "dec")

    for ((precision, scale) <- Seq((5, 2), (1, 0), (1, 1), (18, 10), (18, 17))) {
      withTempPath { dir =>
        val data = makeDecimalRDD(DecimalType(precision, scale))
        data.saveAsParquetFile(dir.getCanonicalPath)
        checkAnswer(parquetFile(dir.getCanonicalPath), data.collect().toSeq)
      }
    }

    // Decimals with precision above 18 are not yet supported
    intercept[RuntimeException] {
      withTempPath { dir =>
        makeDecimalRDD(DecimalType(19, 10)).saveAsParquetFile(dir.getCanonicalPath)
        parquetFile(dir.getCanonicalPath).collect()
      }
    }

    // Unlimited-length decimals are not yet supported
    intercept[RuntimeException] {
      withTempPath { dir =>
        makeDecimalRDD(DecimalType.Unlimited).saveAsParquetFile(dir.getCanonicalPath)
        parquetFile(dir.getCanonicalPath).collect()
      }
    }
  }

  test("map") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> s"val_$i")))
    checkParquetFile(data)
  }

  test("array") {
    val data = (1 to 4).map(i => Tuple1(Seq(i, i + 1)))
    checkParquetFile(data)
  }

  test("struct") {
    val data = (1 to 4).map(i => Tuple1((i, s"val_$i")))
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(struct) =>
        Row(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  test("nested struct with array of array as field") {
    val data = (1 to 4).map(i => Tuple1((i, Seq(Seq(s"val_$i")))))
    withParquetDataFrame(data) { df =>
      // Structs are converted to `Row`s
      checkAnswer(df, data.map { case Tuple1(struct) =>
        Row(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  test("nested map with struct as value type") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> (i, s"val_$i"))))
    withParquetDataFrame(data) { df =>
      checkAnswer(df, data.map { case Tuple1(m) =>
        Row(m.mapValues(struct => Row(struct.productIterator.toSeq: _*)))
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
      assert(rows.size === 1)
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
      assert(rows.size === 1)
      assert(rows.head === Row(Seq.fill(3)(null): _*))
    }
  }

  test("compression codec") {
    def compressionCodecFor(path: String) = {
      val codecs = ParquetTypesConverter
        .readMetaData(new Path(path), Some(configuration))
        .getBlocks
        .flatMap(_.getColumns)
        .map(_.getCodec.name())
        .distinct

      assert(codecs.size === 1)
      codecs.head
    }

    val data = (0 until 10).map(i => (i, i.toString))

    def checkCompressionCodec(codec: CompressionCodecName): Unit = {
      withSQLConf(SQLConf.PARQUET_COMPRESSION -> codec.name()) {
        withParquetFile(data) { path =>
          assertResult(conf.parquetCompressionCodec.toUpperCase) {
            compressionCodecFor(path)
          }
        }
      }
    }

    // Checks default compression codec
    checkCompressionCodec(CompressionCodecName.fromConf(conf.parquetCompressionCodec))

    checkCompressionCodec(CompressionCodecName.UNCOMPRESSED)
    checkCompressionCodec(CompressionCodecName.GZIP)
    checkCompressionCodec(CompressionCodecName.SNAPPY)
  }

  test("read raw Parquet file") {
    def makeRawParquetFile(path: Path): Unit = {
      val schema = MessageTypeParser.parseMessageType(
        """
          |message root {
          |  required boolean _1;
          |  required int32   _2;
          |  required int64   _3;
          |  required float   _4;
          |  required double  _5;
          |}
        """.stripMargin)

      val writeSupport = new TestGroupWriteSupport(schema)
      val writer = new ParquetWriter[Group](path, writeSupport)

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
      checkAnswer(parquetFile(path.toString), (0 until 10).map { i =>
        Row(i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
      })
    }
  }

  test("write metadata") {
    withTempPath { file =>
      val path = new Path(file.toURI.toString)
      val fs = FileSystem.getLocal(configuration)
      val attributes = ScalaReflection.attributesFor[(Int, String)]
      ParquetTypesConverter.writeMetaData(attributes, path, configuration)

      assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_COMMON_METADATA_FILE)))
      assert(fs.exists(new Path(path, ParquetFileWriter.PARQUET_METADATA_FILE)))

      val metaData = ParquetTypesConverter.readMetaData(path, Some(configuration))
      val actualSchema = metaData.getFileMetaData.getSchema
      val expectedSchema = ParquetTypesConverter.convertFromAttributes(attributes)

      actualSchema.checkContains(expectedSchema)
      expectedSchema.checkContains(actualSchema)
    }
  }

  test("save - overwrite") {
    withParquetFile((1 to 10).map(i => (i, i.toString))) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().save("org.apache.spark.sql.parquet", SaveMode.Overwrite, Map("path" -> file))
      checkAnswer(parquetFile(file), newData.map(Row.fromTuple))
    }
  }

  test("save - ignore") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().save("org.apache.spark.sql.parquet", SaveMode.Ignore, Map("path" -> file))
      checkAnswer(parquetFile(file), data.map(Row.fromTuple))
    }
  }

  test("save - throw") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      val errorMessage = intercept[Throwable] {
        newData.toDF().save(
          "org.apache.spark.sql.parquet", SaveMode.ErrorIfExists, Map("path" -> file))
      }.getMessage
      assert(errorMessage.contains("already exists"))
    }
  }

  test("save - append") {
    val data = (1 to 10).map(i => (i, i.toString))
    withParquetFile(data) { file =>
      val newData = (11 to 20).map(i => (i, i.toString))
      newData.toDF().save("org.apache.spark.sql.parquet", SaveMode.Append, Map("path" -> file))
      checkAnswer(parquetFile(file), (data ++ newData).map(Row.fromTuple))
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
      val extraMetadata = Map(RowReadSupport.SPARK_METADATA_KEY -> sparkSchema.toString)
      val fileMetadata = new FileMetaData(parquetSchema, extraMetadata, "Spark")
      val path = new Path(location.getCanonicalPath)

      ParquetFileWriter.writeMetadataFile(
        sparkContext.hadoopConfiguration,
        path,
        new Footer(path, new ParquetMetadata(fileMetadata, Nil)) :: Nil)

      assertResult(parquetFile(path.toString).schema) {
        StructType(
          StructField("a", BooleanType, nullable = false) ::
          StructField("b", IntegerType, nullable = false) ::
          Nil)
      }
    }
  }
}

class ParquetDataSourceOnIOSuite extends ParquetIOSuiteBase with BeforeAndAfterAll {
  val originalConf = sqlContext.conf.parquetUseDataSourceApi

  override protected def beforeAll(): Unit = {
    sqlContext.conf.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "true")
  }

  override protected def afterAll(): Unit = {
    sqlContext.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, originalConf.toString)
  }

  test("SPARK-6330 regression test") {
    // In 1.3.0, save to fs other than file: without configuring core-site.xml would get:
    // IllegalArgumentException: Wrong FS: hdfs://..., expected: file:///
    intercept[java.io.FileNotFoundException] {
      sqlContext.parquetFile("file:///nonexistent")
    }
    val errorMessage = intercept[Throwable] {
      sqlContext.parquetFile("hdfs://nonexistent")
    }.toString
    assert(errorMessage.contains("UnknownHostException"))
  }
}

class ParquetDataSourceOffIOSuite extends ParquetIOSuiteBase with BeforeAndAfterAll {
  val originalConf = sqlContext.conf.parquetUseDataSourceApi

  override protected def beforeAll(): Unit = {
    sqlContext.conf.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, "false")
  }

  override protected def afterAll(): Unit = {
    sqlContext.setConf(SQLConf.PARQUET_USE_DATA_SOURCE_API, originalConf.toString)
  }
}
