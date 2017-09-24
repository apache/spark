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

import java.io.File
import java.io.FilenameFilter

import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.catalyst.catalog.CatalogTypes.TablePartitionSpec
import org.apache.spark.sql.execution.datasources.csv.CSVFileFormat
import org.apache.spark.sql.test.SharedSQLContext

class FileFormatWriterSuite extends QueryTest with SharedSQLContext {

  test("empty file should be skipped while write to file") {
    withTempPath { path =>
      spark.range(100).repartition(10).where("id = 50").write.parquet(path.toString)
      val partFiles = path.listFiles()
        .filter(f => f.isFile && !f.getName.startsWith(".") && !f.getName.startsWith("_"))
      assert(partFiles.length === 2)
    }
  }

  test("write should fail when output path is not specified") {
    val session = spark
    import session.implicits._

    val partitionCount = 5
    val ds = spark
      .range(100).as("id").repartition(partitionCount)
      .withColumn("partition", $"id" % partitionCount)

    val plan = ds.queryExecution.sparkPlan
    val output = null

    assertThrows[IllegalArgumentException] {
      FileFormatWriter.write(
        sparkSession = session,
        plan = plan,
        fileFormat = new CSVFileFormat(),
        committer = new SQLHadoopMapReduceCommitProtocol("job-1", output),
        outputSpec = FileFormatWriter.OutputSpec(output, Map.empty),
        hadoopConf = ds.sparkSession.sparkContext.hadoopConfiguration,
        partitionColumns = plan.outputSet.find(_.name == "partition").toSeq,
        bucketSpec = None,
        statsTrackers = Seq.empty,
        options = Map.empty
      )
    }
  }

  test("write should succeed when output path is specified") {
    withTempPath { path =>
      val session = spark
      import session.implicits._

      val partitionCount = 5
      val ds = spark
        .range(100).as("id").repartition(partitionCount)
        .withColumn("partition", $"id" % partitionCount)

      val plan = ds.queryExecution.sparkPlan
      val output = path.getAbsolutePath

      FileFormatWriter.write(
        sparkSession = session,
        plan = plan,
        fileFormat = new CSVFileFormat(),
        committer = new SQLHadoopMapReduceCommitProtocol("job-1", output),
        outputSpec = FileFormatWriter.OutputSpec(output, Map.empty),
        hadoopConf = ds.sparkSession.sparkContext.hadoopConfiguration,
        partitionColumns = plan.outputSet.find(_.name == "partition").toSeq,
        bucketSpec = None,
        statsTrackers = Seq.empty,
        options = Map.empty
      )

      val partitions = listPartitions(path)
      partitions.foreach(partition => assert(partition.listFiles().nonEmpty))

      val actualPartitions = partitions.map(_.getName)
      val expectedPartitions = (0 until partitionCount).map(partition => s"partition=$partition")
      assert(actualPartitions.toSet === expectedPartitions.toSet)
    }
  }

  test("write should succeed when custom partition locations are specified") {
    withTempPath { path =>
      val session = spark
      import session.implicits._

      val partitionCount = 5
      val ds = spark
        .range(100).as("id").repartition(partitionCount)
        .withColumn("partition", $"id" % partitionCount)

      val customPartitionOutputs = (0 until partitionCount)
        .foldLeft(Map[TablePartitionSpec, String]()) { (acc, partition) =>
        acc + (
          Map("partition" -> partition.toString) ->
          new File(path, partition.toString).getAbsolutePath
        )
      }

      val plan = ds.queryExecution.sparkPlan
      val output = path.getAbsolutePath

      FileFormatWriter.write(
        sparkSession = session,
        plan = plan,
        fileFormat = new CSVFileFormat(),
        committer = new SQLHadoopMapReduceCommitProtocol("job-1", output),
        outputSpec = FileFormatWriter.OutputSpec(output, customPartitionOutputs),
        hadoopConf = ds.sparkSession.sparkContext.hadoopConfiguration,
        partitionColumns = plan.outputSet.find(_.name == "partition").toSeq,
        bucketSpec = None,
        statsTrackers = Seq.empty,
        options = Map.empty
      )

      val partitions = listPartitions(path)
      partitions.foreach(partition => assert(partition.listFiles().nonEmpty))

      val actualPartitions = partitions.map(_.getName)
      val expectedPartitions = (0 until partitionCount).map(_.toString)
      assert(actualPartitions.toSet === expectedPartitions.toSet)
    }
  }

  private def listPartitions(path: File): Array[File] = {
    path.listFiles(new FilenameFilter {
      override def accept(dir: File, name: String): Boolean =
        !(name.startsWith(".") || name.startsWith("_"))
    })
  }

}
