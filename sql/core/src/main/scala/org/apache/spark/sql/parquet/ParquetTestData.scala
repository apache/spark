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

import java.io.File

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.mapreduce.Job

import parquet.example.data.{GroupWriter, Group}
import parquet.example.data.simple.SimpleGroup
import parquet.hadoop.{ParquetReader, ParquetFileReader, ParquetWriter}
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.hadoop.example.GroupReadSupport
import parquet.hadoop.util.ContextUtil
import parquet.io.api.RecordConsumer
import parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.util.Utils

// Write support class for nested groups: ParquetWriter initializes GroupWriteSupport
// with an empty configuration (it is after all not intended to be used in this way?)
// and members are private so we need to make our own in order to pass the schema
// to the writer.
private class TestGroupWriteSupport(schema: MessageType) extends WriteSupport[Group] {
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

private[sql] object ParquetTestData {

  val testSchema =
    """message myrecord {
      |optional boolean myboolean;
      |optional int32 myint;
      |optional binary mystring;
      |optional int64 mylong;
      |optional float myfloat;
      |optional double mydouble;
      |}""".stripMargin

  // field names for test assertion error messages
  val testSchemaFieldNames = Seq(
    "myboolean:Boolean",
    "myint:Int",
    "mystring:String",
    "mylong:Long",
    "myfloat:Float",
    "mydouble:Double"
  )

  val subTestSchema =
    """
      |message myrecord {
      |optional boolean myboolean;
      |optional int64 mylong;
      |}
    """.stripMargin

  val testFilterSchema =
    """
      |message myrecord {
      |required boolean myboolean;
      |required int32 myint;
      |required binary mystring;
      |required int64 mylong;
      |required float myfloat;
      |required double mydouble;
      |}
    """.stripMargin

  // field names for test assertion error messages
  val subTestSchemaFieldNames = Seq(
    "myboolean:Boolean",
    "mylong:Long"
  )

  val testDir = Utils.createTempDir()
  val testFilterDir = Utils.createTempDir()

  lazy val testData = new ParquetRelation(testDir.toURI.toString)

  val testNestedSchema1 =
    // based on blogpost example, source:
    // https://blog.twitter.com/2013/dremel-made-simple-with-parquet
    // note: instead of string we have to use binary (?) otherwise
    // Parquet gives us:
    // IllegalArgumentException: expected one of [INT64, INT32, BOOLEAN,
    //   BINARY, FLOAT, DOUBLE, INT96, FIXED_LEN_BYTE_ARRAY]
    // Also repeated primitives seem tricky to convert (AvroParquet
    // only uses them in arrays?) so only use at most one in each group
    // and nothing else in that group (-> is mapped to array)!
    // The "values" inside ownerPhoneNumbers is a keyword currently
    // so that array types can be translated correctly.
    """
      |message AddressBook {
        |required binary owner;
        |optional group ownerPhoneNumbers {
          |repeated binary values;
        |}
        |repeated group contacts {
          |required binary name;
          |optional binary phoneNumber;
        |}
      |}
    """.stripMargin


  val testNestedSchema2 =
    """
      |message TestNested2 {
        |required int32 firstInt;
        |optional int32 secondInt;
        |optional group longs {
          |repeated int64 values;
        |}
        |repeated group entries {
          |required double value;
          |optional boolean truth;
        |}
        |optional group outerouter {
          |repeated group values {
            |repeated group values {
              |repeated int32 values;
            |}
          |}
        |}
      |}
    """.stripMargin

  val testNestedSchema3 =
    """
      |message TestNested3 {
      |required int32 x;
        |repeated group booleanNumberPairs {
          |required int32 key;
          |repeated group value {
            |required double nestedValue;
            |optional boolean truth;
          |}
        |}
      |}
    """.stripMargin

  val testNestedDir1 = Utils.createTempDir()
  val testNestedDir2 = Utils.createTempDir()
  val testNestedDir3 = Utils.createTempDir()

  lazy val testNestedData1 = new ParquetRelation(testNestedDir1.toURI.toString)
  lazy val testNestedData2 = new ParquetRelation(testNestedDir2.toURI.toString)

  // Implicit
  // TODO: get rid of this since it is confusing!
  implicit def makePath(dir: File): Path = {
    new Path(new Path(dir.toURI), new Path("part-r-0.parquet"))
  }

  def writeFile() = {
    testDir.delete()
    val path: Path = new Path(new Path(testDir.toURI), new Path("part-r-0.parquet"))
    val job = new Job()
    val configuration: Configuration = ContextUtil.getConfiguration(job)
    val schema: MessageType = MessageTypeParser.parseMessageType(testSchema)
    val writeSupport = new TestGroupWriteSupport(schema)
    val writer = new ParquetWriter[Group](path, writeSupport)

    for(i <- 0 until 15) {
      val record = new SimpleGroup(schema)
      if (i % 3 == 0) {
        record.add(0, true)
      } else {
        record.add(0, false)
      }
      if (i % 5 == 0) {
        record.add(1, 5)
      }
      record.add(2, "abc")
      record.add(3, i.toLong << 33)
      record.add(4, 2.5F)
      record.add(5, 4.5D)
      writer.write(record)
    }
    writer.close()
  }

  def writeFilterFile(records: Int = 200) = {
    // for microbenchmark use: records = 300000000
    testFilterDir.delete
    val path: Path = new Path(new Path(testFilterDir.toURI), new Path("part-r-0.parquet"))
    val schema: MessageType = MessageTypeParser.parseMessageType(testFilterSchema)
    val writeSupport = new TestGroupWriteSupport(schema)
    val writer = new ParquetWriter[Group](path, writeSupport)

    for(i <- 0 to records) {
      val record = new SimpleGroup(schema)
      if (i % 4 == 0) {
        record.add(0, true)
      } else {
        record.add(0, false)
      }
      record.add(1, i)
      record.add(2, i.toString)
      record.add(3, i.toLong)
      record.add(4, i.toFloat + 0.5f)
      record.add(5, i.toDouble + 0.5d)
      writer.write(record)
    }
    writer.close()
  }

  def writeNestedFile1() {
    // example data from https://blog.twitter.com/2013/dremel-made-simple-with-parquet
    testNestedDir1.delete()
    val path: Path = testNestedDir1
    val schema: MessageType = MessageTypeParser.parseMessageType(testNestedSchema1)

    val r1 = new SimpleGroup(schema)
    r1.add(0, "Julien Le Dem")
    r1.addGroup(1)
      .append("values", "555 123 4567")
      .append("values", "555 666 1337")
      .append("values", "XXX XXX XXXX")
    r1.addGroup(2)
    //  .addGroup(0)
      .append("name", "Dmitriy Ryaboy")
      .append("phoneNumber", "555 987 6543")
    r1.addGroup(2)
    //  .addGroup(0)
      .append("name", "Chris Aniszczyk")

    val r2 = new SimpleGroup(schema)
    r2.add(0, "A. Nonymous")

    val writeSupport = new TestGroupWriteSupport(schema)
    val writer = new ParquetWriter[Group](path, writeSupport)
    writer.write(r1)
    writer.write(r2)
    writer.close()
  }

  def writeNestedFile2() {
    testNestedDir2.delete()
    val path: Path = testNestedDir2
    val schema: MessageType = MessageTypeParser.parseMessageType(testNestedSchema2)

    val r1 = new SimpleGroup(schema)
    r1.add(0, 1)
    r1.add(1, 7)
    val longs = r1.addGroup(2)
    longs.add(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME , 1.toLong << 32)
    longs.add(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 1.toLong << 33)
    longs.add(CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME, 1.toLong << 34)
    val booleanNumberPairs = r1.addGroup(3)
    booleanNumberPairs.add("value", 2.5)
    booleanNumberPairs.add("truth", false)
    val top_level = r1.addGroup(4)
    val second_level_a = top_level.addGroup(0)
    val second_level_b = top_level.addGroup(0)
    val third_level_aa = second_level_a.addGroup(0)
    val third_level_ab = second_level_a.addGroup(0)
    val third_level_c = second_level_b.addGroup(0)
    third_level_aa.add(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      7)
    third_level_ab.add(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      8)
    third_level_c.add(
      CatalystConverter.ARRAY_ELEMENTS_SCHEMA_NAME,
      9)

    val writeSupport = new TestGroupWriteSupport(schema)
    val writer = new ParquetWriter[Group](path, writeSupport)
    writer.write(r1)
    writer.close()
  }

  def writeNestedFile3() {
    testNestedDir3.delete()
    val path: Path = testNestedDir3
    val schema: MessageType = MessageTypeParser.parseMessageType(testNestedSchema3)

    val r1 = new SimpleGroup(schema)
    r1.add(0, 1)
    val g1 = r1.addGroup(1)
    g1.add(0, 1)
    val ng1 = g1.addGroup(1)
    ng1.add(0, 1.5)
    ng1.add(1, false)
    val ng2 = g1.addGroup(1)
    ng2.add(0, 2.5)
    ng2.add(1, true)
    val g2 = r1.addGroup(1)
    g2.add(0, 2)
    val ng3 = g2.addGroup(1)
    ng3.add(0, 3.5)
    ng3.add(1, false)

    val writeSupport = new TestGroupWriteSupport(schema)
    val writer = new ParquetWriter[Group](path, writeSupport)
    writer.write(r1)
    writer.close()
  }

  def readNestedFile(path: File, schemaString: String): Unit = {
    val configuration = new Configuration()
    val fs: FileSystem = path.getFileSystem(configuration)
    val schema: MessageType = MessageTypeParser.parseMessageType(schemaString)
    assert(schema != null)
    val outputStatus: FileStatus = fs.getFileStatus(path)
    val footers = ParquetFileReader.readFooter(configuration, outputStatus)
    assert(footers != null)
    val reader = new ParquetReader(path, new GroupReadSupport())
    val first = reader.read()
    assert(first != null)
  }
}

