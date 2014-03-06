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

import org.apache.hadoop.fs.Path
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapreduce.Job

import parquet.schema.{MessageTypeParser, MessageType}
import parquet.hadoop.util.ContextUtil
import parquet.hadoop.ParquetWriter

object ParquetTestData {

  val testSchema =
    """message myrecord {
      |optional boolean myboolean;
      |optional int32 myint;
      |optional binary mystring;
      |optional int64 mylong;
      |optional float myfloat;
      |optional double mydouble;
      |}""".stripMargin

  val subTestSchema =
    """
      |message myrecord {
      |optional boolean myboolean;
      |optional int64 mylong;
      |}
    """.stripMargin

  val testFile = new File("/tmp/testParquetFile").getAbsoluteFile

  lazy val testData = new ParquetRelation("testData", testFile.toURI.toString)

  def writeFile = {
    testFile.delete
    val path: Path = new Path(testFile.toURI)
    val job = new Job()
    val configuration: Configuration = ContextUtil.getConfiguration(job)
    val schema: MessageType = MessageTypeParser.parseMessageType(testSchema)

    val writeSupport = new RowWriteSupport()
    writeSupport.setSchema(schema, configuration)
    val writer = new ParquetWriter(path, writeSupport)
    for(i <- 0 until 15) {
      val data = new Array[Any](6)
      if(i % 3 ==0) {
        data.update(0, true)
      } else {
        data.update(0, false)
      }
      if(i % 5 == 0) {
        data.update(1, 5)
      } else {
        data.update(1, null) // optional
      }
      data.update(2, "abc")
      data.update(3, 1L<<33)
      data.update(4, 2.5F)
      data.update(5, 4.5D)
      writer.write(new ParquetRelation.RowType(data.toArray))
    }
    writer.close()
  }
}

