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

package org.apache.spark.shuffle.example

import java.nio.file.Files

import com.fasterxml.jackson.databind.ObjectMapper
import org.apache.spark.{SparkConf, SparkContext, SparkFunSuite}
import org.apache.spark.internal.config.SHUFFLE_IO_PLUGIN_CLASS
import org.apache.spark.shuffle.api.{ShuffleExecutorComponents, ShuffleMapOutputWriter, ShuffleDataIO}
import org.apache.spark.util.Utils

class JsonShuffleDataIOSuite extends SparkFunSuite {

  test("JsonShuffleDataIO end-to-end map output writer writes valid JSON files") {
    val tempDir = Utils.createTempDir()
    try {
      val conf = new SparkConf()
        .setAppName("JsonShuffleDataIOTest")
        .setMaster("local")
        .set(SHUFFLE_IO_PLUGIN_CLASS, "org.apache.spark.shuffle.example.JsonShuffleDataIO")
        .set("spark.shuffle.sort.io.json.outputDir", tempDir.getAbsolutePath)

      val sc = new SparkContext(conf)
      try {
        val dataIO = sc.conf.get(SHUFFLE_IO_PLUGIN_CLASS.key)
        assert(dataIO == "org.apache.spark.shuffle.example.JsonShuffleDataIO")

        val shuffleDataIO = Utils.loadExtensions(
          classOf[ShuffleDataIO], Seq(dataIO), sc.conf).head
        val driverComponents = shuffleDataIO.driver()
        val extraConfigs = driverComponents.initializeApplication()
        assert(extraConfigs.get("spark.shuffle.sort.io.json.outputDir") == tempDir.getAbsolutePath)

        val executorComponents: ShuffleExecutorComponents = shuffleDataIO.executor()
        executorComponents.initializeExecutor("app", "exec-0", extraConfigs)

        val mapOutputWriter: ShuffleMapOutputWriter =
          executorComponents.createMapOutputWriter(1, 42L, 3)

        val bytes = Array[Array[Byte]](
          Array[Byte](1, 2, 3),
          Array.emptyByteArray,
          Array[Byte](4, 5, 6, 7)
        )

        for (partitionId <- bytes.indices) {
          val writer = mapOutputWriter.getPartitionWriter(partitionId)
          val output = writer.openStream()
          output.write(bytes(partitionId))
          output.close()
        }

        val commitMessage = mapOutputWriter.commitAllPartitions(Array.emptyLongArray)
        assert(commitMessage.getPartitionLengths.sameElements(Array(3L, 0L, 4L)))

        val partitionFiles = (0 until 3).map(partitionId =>
          tempDir.toPath.resolve(s"shuffle-1").resolve(s"map-42").resolve(s"partition-$partitionId.json"))

        partitionFiles.foreach { file =>
          assert(Files.exists(file), s"Expected JSON file: $file")
          val bytes = Files.readAllBytes(file)
          val mapper = new ObjectMapper()
          val node = mapper.readTree(bytes)
          assert(node.get("partitionId").asInt() == partitionFiles.indexOf(file))
          assert(node.get("bytesWritten").asLong() == Seq(3L, 0L, 4L)(partitionFiles.indexOf(file)))
          assert(node.has("data"))
        }
      } finally {
        sc.stop()
      }
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
