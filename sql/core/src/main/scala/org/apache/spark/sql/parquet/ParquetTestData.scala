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

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path

import parquet.example.data.{GroupWriter, Group}
import parquet.example.data.simple.SimpleGroup
import parquet.hadoop.ParquetWriter
import parquet.hadoop.api.WriteSupport
import parquet.hadoop.api.WriteSupport.WriteContext
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

  def writeFile() = {
    testDir.delete
    val path: Path = new Path(new Path(testDir.toURI), new Path("part-r-0.parquet"))
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
}

