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
import parquet.example.data.simple.SimpleGroup
import parquet.example.data.{Group, GroupWriter}
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.{ParquetFileWriter, ParquetWriter}
import parquet.io.api.RecordConsumer
import parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.sql.catalyst.ScalaReflection
import org.apache.spark.sql.catalyst.expressions.Row
import org.apache.spark.sql.catalyst.types.DecimalType
import org.apache.spark.sql.test.TestSQLContext
import org.apache.spark.sql.test.TestSQLContext._
import org.apache.spark.sql.{QueryTest, SQLConf, SchemaRDD}

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
class ParquetIOSuite extends QueryTest with ParquetTest {
  val sqlContext = TestSQLContext

  /**
   * Writes `data` to a Parquet file, reads it back and check file contents.
   */
  protected def checkParquetFile[T <: Product: ClassTag: TypeTag](data: Seq[T]): Unit = {
    withParquetRDD(data)(checkAnswer(_, data))
  }

  test("basic data types (without binary)") {
    val data = (1 to 4).map { i =>
      (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
    }
    checkParquetFile(data)
  }

  test("raw binary") {
    val data = (1 to 4).map(i => Tuple1(Array.fill(3)(i.toByte)))
    withParquetRDD(data) { rdd =>
      assertResult(data.map(_._1.mkString(",")).sorted) {
        rdd.collect().map(_.getAs[Array[Byte]](0).mkString(",")).sorted
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
    def makeDecimalRDD(decimal: DecimalType): SchemaRDD =
      sparkContext
        .parallelize(0 to 1000)
        .map(i => Tuple1(i / 100.0))
        .select('_1 cast decimal)

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
    withParquetRDD(data) { rdd =>
      // Structs are converted to `Row`s
      checkAnswer(rdd, data.map { case Tuple1(struct) =>
        Tuple1(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  test("nested struct with array of array as field") {
    val data = (1 to 4).map(i => Tuple1((i, Seq(Seq(s"val_$i")))))
    withParquetRDD(data) { rdd =>
      // Structs are converted to `Row`s
      checkAnswer(rdd, data.map { case Tuple1(struct) =>
        Tuple1(Row(struct.productIterator.toSeq: _*))
      })
    }
  }

  test("nested map with struct as value type") {
    val data = (1 to 4).map(i => Tuple1(Map(i -> (i, s"val_$i"))))
    withParquetRDD(data) { rdd =>
      checkAnswer(rdd, data.map { case Tuple1(m) =>
        Tuple1(m.mapValues(struct => Row(struct.productIterator.toSeq: _*)))
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

    withParquetRDD(allNulls :: Nil) { rdd =>
      val rows = rdd.collect()
      assert(rows.size === 1)
      assert(rows.head === Row(Seq.fill(5)(null): _*))
    }
  }

  test("nones") {
    val allNones = (
      None.asInstanceOf[Option[Int]],
      None.asInstanceOf[Option[Long]],
      None.asInstanceOf[Option[String]])

    withParquetRDD(allNones :: Nil) { rdd =>
      val rows = rdd.collect()
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
        (i % 2 == 0, i, i.toLong, i.toFloat, i.toDouble)
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
}
