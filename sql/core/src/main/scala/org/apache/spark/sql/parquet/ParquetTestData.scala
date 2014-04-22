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
import org.apache.hadoop.mapreduce.Job

import parquet.hadoop.ParquetWriter
import parquet.hadoop.util.ContextUtil
import parquet.schema.{MessageType, MessageTypeParser}

import org.apache.spark.sql.catalyst.expressions.GenericRow
import org.apache.spark.util.Utils
import parquet.hadoop.metadata.CompressionCodecName
import parquet.hadoop.api.WriteSupport
import parquet.example.data.{GroupWriter, Group}
import parquet.io.api.RecordConsumer
import parquet.hadoop.api.WriteSupport.WriteContext
import parquet.example.data.simple.SimpleGroup

// Write support class for nested groups:
// ParquetWriter initializes GroupWriteSupport with an empty configuration
// (it is after all not intended to be used in this way?)
// and members are private so we need to make our own
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

  // field names for test assertion error messages
  val subTestSchemaFieldNames = Seq(
    "myboolean:Boolean",
    "mylong:Long"
  )

  val testDir = Utils.createTempDir()

  lazy val testData = new ParquetRelation(testDir.toURI.toString)

  def writeFile() = {
    testDir.delete
    val path: Path = new Path(new Path(testDir.toURI), new Path("part-r-0.parquet"))
    val job = new Job()
    val configuration: Configuration = ContextUtil.getConfiguration(job)
    val schema: MessageType = MessageTypeParser.parseMessageType(testSchema)

    //val writeSupport = new MutableRowWriteSupport()
    //writeSupport.setSchema(schema, configuration)
    //val writer = new ParquetWriter(path, writeSupport)
    val writeSupport = new TestGroupWriteSupport(schema)
    //val writer = //new ParquetWriter[Group](path, writeSupport)
    val writer = new ParquetWriter[Group](path, writeSupport)

    for(i <- 0 until 15) {
      val record = new SimpleGroup(schema)
      //val data = new Array[Any](6)
      if (i % 3 == 0) {
        //data.update(0, true)
        record.add(0, true)
      } else {
        //data.update(0, false)
        record.add(0, false)
      }
      if (i % 5 == 0) {
        record.add(1, 5)
      //  data.update(1, 5)
      } else {
        if (i % 5 == 1) record.add(1, 4)
      }
       //else {
      //  data.update(1, null) // optional
      //}
      //data.update(2, "abc")
      record.add(2, "abc")
      //data.update(3, i.toLong << 33)
      record.add(3, i.toLong << 33)
      //data.update(4, 2.5F)
      record.add(4, 2.5F)
      //data.update(5, 4.5D)
      record.add(5, 4.5D)
      //writer.write(new GenericRow(data.toArray))
      writer.write(record)
    }
    writer.close()
  }
}

