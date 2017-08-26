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

package org.apache.spark.sql.sources

import java.sql.{Date, Timestamp}

import org.apache.hadoop.fs.Path
import org.apache.hadoop.io.{IntWritable, NullWritable, Text}
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.{OrcConf, TypeDescription}
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.{OrcInputFormat, OrcOutputFormat}
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgumentFactory}
import org.scalatest.BeforeAndAfterAll

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSQLContext

/**
 * Data Source qualification as Apache Spark Data Sources.
 * - Apache Spark Data Type Value Limits
 * - Predicate Push Down
 */
class DataSourceSuite
  extends QueryTest
  with SharedSQLContext
  with BeforeAndAfterAll {

  import testImplicits._

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.session.timeZone", "GMT")
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.unset("spark.sql.session.timeZone")
    } finally {
      super.afterAll()
    }
  }

  Seq("parquet", "orc").foreach { dataSource =>
    test(s"$dataSource - data type value limit") {
      withTempPath { tempDir =>
        withTable("tab1") {
          val df = ((
            false,
            true,
            Byte.MinValue,
            Byte.MaxValue,
            Short.MinValue,
            Short.MaxValue,
            Int.MinValue,
            Int.MaxValue,
            Long.MinValue,
            Long.MaxValue,
            Float.MinValue,
            Float.MaxValue,
            Double.MinValue,
            Double.MaxValue,
            Date.valueOf("0001-01-01"),
            Date.valueOf("9999-12-31"),
            new Timestamp(-62135769600000L), // 0001-01-01 00:00:00.000
            new Timestamp(253402300799999L)  // 9999-12-31 23:59:59.999
          ) :: Nil).toDF()
          df.write.format(dataSource).save(tempDir.getCanonicalPath)
          sql(s"CREATE TABLE tab1 USING $dataSource LOCATION '${tempDir.toURI}'")
          checkAnswer(sql(s"SELECT ${df.schema.fieldNames.mkString(",")} FROM tab1"), df)
        }
      }
    }
  }

  // This is a port from TestMapreduceOrcOutputFormat.java of Apache ORC
  test("orc - predicate push down") {
    withTempDir { dir =>
      val conf = new JobConf()
      val id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0)
      val attemptContext = new TaskAttemptContextImpl(conf, id)
      val typeStr = "struct<i:int,s:string>"
      OrcConf.MAPRED_OUTPUT_SCHEMA.setString(conf, typeStr)
      conf.set("mapreduce.output.fileoutputformat.outputdir", dir.getCanonicalPath)
      conf.setInt(OrcConf.ROW_INDEX_STRIDE.getAttribute(), 1000)
      conf.setBoolean(OrcOutputFormat.SKIP_TEMP_DIRECTORY, true)
      val outputFormat = new OrcOutputFormat[OrcStruct]()
      val writer = outputFormat.getRecordWriter(attemptContext)

      // write 4000 rows with the integer and the binary string
      val row = OrcStruct.createValue(TypeDescription.fromString(typeStr)).asInstanceOf[OrcStruct]
      val nada = NullWritable.get()

      for(r <- 0 until 4000) {
        row.setFieldValue(0, new IntWritable(r))
        row.setFieldValue(1, new Text(Integer.toBinaryString(r)))
        writer.write(nada, row)
      }
      writer.close(attemptContext)

      OrcInputFormat.setSearchArgument(conf,
        SearchArgumentFactory.newBuilder()
          .between("i", PredicateLeaf.Type.LONG, 1500L, 1999L)
          .build(), Array[String](null, "i", "s"))

      val split = new FileSplit(
        new Path(dir.getCanonicalPath, "part-m-00000.orc"), 0, 1000000, Array[String]())
      val reader = new OrcInputFormat[OrcStruct]().createRecordReader(split, attemptContext)

      // the sarg should cause it to skip over the rows except 1000 to 2000
      for(r <- 1000 until 2000) {
        assert(reader.nextKeyValue())
        val row = reader.getCurrentValue
        assert(r == row.getFieldValue(0).asInstanceOf[IntWritable].get)
        assert(Integer.toBinaryString(r) == row.getFieldValue(1).toString)
      }
      assert(!reader.nextKeyValue())
      reader.close()
    }
  }
}
