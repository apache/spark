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
import org.apache.hadoop.io.LongWritable
import org.apache.hadoop.mapred.JobConf
import org.apache.hadoop.mapreduce._
import org.apache.hadoop.mapreduce.lib.input.FileSplit
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl
import org.apache.orc.OrcConf
import org.apache.orc.mapred.OrcStruct
import org.apache.orc.mapreduce.OrcInputFormat
import org.apache.orc.storage.ql.io.sarg.{PredicateLeaf, SearchArgumentFactory}

import org.apache.spark.sql.{Dataset, QueryTest, Row}
import org.apache.spark.sql.hive.test.TestHiveSingleton
import org.apache.spark.sql.test.SQLTestUtils

/**
 * Data Source qualification as Apache Spark Data Sources.
 * - Apache Spark Data Type Value Limits
 * - Predicate Push Down
 */
class DataSourceSuite
  extends QueryTest
  with SQLTestUtils
  with TestHiveSingleton {

  import testImplicits._

  var df: Dataset[Row] = _

  override def beforeAll(): Unit = {
    super.beforeAll()
    spark.conf.set("spark.sql.session.timeZone", "GMT")

    df = ((
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
  }

  override def afterAll(): Unit = {
    try {
      spark.conf.unset("spark.sql.session.timeZone")
    } finally {
      super.afterAll()
    }
  }

  Seq("parquet", "orc", "json", "csv").foreach { dataSource =>
    test(s"$dataSource - data type value limit") {
      withTempPath { tempDir =>
        df.write.format(dataSource).save(tempDir.getCanonicalPath)

        // Use the same schema for saving/loading
        checkAnswer(
          spark.read.format(dataSource).schema(df.schema).load(tempDir.getCanonicalPath),
          df)

        // Use schema inference, but skip text-based format due to its limitation
        if (Seq("parquet", "orc").contains(dataSource)) {
          withTable("tab1") {
            sql(s"CREATE TABLE tab1 USING $dataSource LOCATION '${tempDir.toURI}'")
            checkAnswer(sql(s"SELECT ${df.schema.fieldNames.mkString(",")} FROM tab1"), df)
          }
        }
      }
    }
  }

  test("orc - predicate push down") {
    withTempDir { dir =>
      dir.delete()

      // write 4000 rows with the integer and the string in a single orc file
      spark
        .range(4000)
        .map(i => (i, s"$i"))
        .toDF("i", "s")
        .repartition(1)
        .write
        .option(OrcConf.ROW_INDEX_STRIDE.getAttribute, 1000)
        .orc(dir.getCanonicalPath)
      val fileName = dir.list().find(_.endsWith(".orc"))
      assert(fileName.isDefined)

      // Predicate Push-down: BETWEEN 1500 AND 1999
      val conf = new JobConf()
      val id = new TaskAttemptID("jt", 0, TaskType.MAP, 0, 0)
      val attemptContext = new TaskAttemptContextImpl(conf, id)
      OrcInputFormat.setSearchArgument(conf,
        SearchArgumentFactory.newBuilder()
          .between("i", PredicateLeaf.Type.LONG, 1500L, 1999L)
          .build(), Array[String](null, "i", "s"))
      val path = new Path(dir.getCanonicalPath, fileName.get)
      val split = new FileSplit(path, 0, Int.MaxValue, Array[String]())
      val reader = new OrcInputFormat[OrcStruct]().createRecordReader(split, attemptContext)

      // the sarg should cause it to skip over the rows except 1000 to 2000
      for(r <- 1000 until 2000) {
        assert(reader.nextKeyValue())
        val row = reader.getCurrentValue
        assert(r == row.getFieldValue(0).asInstanceOf[LongWritable].get)
        assert(r.toString == row.getFieldValue(1).toString)
      }
      assert(!reader.nextKeyValue())
      reader.close()
    }
  }
}
