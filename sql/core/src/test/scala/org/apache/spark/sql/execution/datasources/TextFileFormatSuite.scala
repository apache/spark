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

package org.apache.spark.sql.execution.datasources

import org.apache.hadoop.fs.Path
import org.apache.hadoop.mapreduce.{JobID, TaskAttemptID, TaskID, TaskType}
import org.apache.hadoop.mapreduce.task.TaskAttemptContextImpl

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.InternalRow
import org.apache.spark.sql.execution.datasources.text.{TextFileFormat, TextOutputWriter}
import org.apache.spark.sql.test.SharedSQLContext
import org.apache.spark.sql.types.{StringType, StructType}

class TextFileFormatSuite extends QueryTest with SharedSQLContext {
  import testImplicits._


  test("wholetext mode shound not write \n") {
    withTempPath { path =>
      spark.range(100).map(_.toString).repartition(10).write.text(path.toString)
      val hadoopConf = spark.sparkContext.hadoopConfiguration
      val inputPath = new Path(path.getPath)
      val fs = inputPath.getFileSystem(hadoopConf)
      val fileStatus = fs.listStatus(inputPath)

      //  Get the generated 10 small files
      val files = fileStatus.filter(!_.getPath.getName.equals("_SUCCESS")).map(status =>
        new PartitionedFile(InternalRow.empty, status.getPath.toString, 0, status.getLen))


      def reader(file: PartitionedFile, options: Map[String, String]): Iterator[InternalRow] = {
        val format = new TextFileFormat
        format.buildReader(spark, null, null, format.inferSchema(null, null, null).get,
          Seq.empty, options, spark.sparkContext.hadoopConfiguration)(file)
      }

      def write(internalRows: Seq[InternalRow], suffix: String): Path = {
        val attemptId = new TaskAttemptID(new TaskID(new JobID(), TaskType.MAP, 0), 0)
        val hadoopAttemptContext = new TaskAttemptContextImpl(hadoopConf, attemptId)
        val targetPath = new Path(inputPath.toString, "part-0000" + suffix)
        val writer = new TextOutputWriter(targetPath.toString,
          new StructType().add("value", StringType), hadoopAttemptContext)
        internalRows.foreach(writer.write(_))
        writer.close()
        targetPath
      }

      // merge files by wholetext mode
      val wholeTextRows = files.flatMap(reader(_, Map("wholetext" -> "true"))).toSeq
      val wholeTextPath = write(wholeTextRows, "wholetext")

      // merge files by line-by-line mode
      val lineTextRows = files.flatMap(file => reader(file, Map.empty)).toSeq
      val lineTextPath = write(lineTextRows, "linetext")

      // count
      val wholeTextCount = spark.read.textFile(wholeTextPath.toString).count()
      val lineTextCount = spark.read.textFile(lineTextPath.toString).count()

      assert(wholeTextCount == lineTextCount)

      fs.delete(wholeTextPath, true)
      fs.delete(lineTextPath, true)

    }
  }




}
