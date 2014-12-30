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

package org.apache.spark.api.python

import scala.io.Source

import java.io.{PrintWriter, File}

import org.scalatest.{Matchers, FunSuite}

import org.apache.spark.{SharedSparkContext, SparkConf}
import org.apache.spark.serializer.KryoSerializer
import org.apache.spark.util.Utils

// This test suite uses SharedSparkContext because we need a SparkEnv in order to deserialize
// a PythonBroadcast:
class PythonBroadcastSuite extends FunSuite with Matchers with SharedSparkContext {
  test("PythonBroadcast can be serialized with Kryo (SPARK-4882)") {
    val tempDir = Utils.createTempDir()
    val broadcastedString = "Hello, world!"
    def assertBroadcastIsValid(broadcast: PythonBroadcast): Unit = {
      val source = Source.fromFile(broadcast.path)
      val contents = source.mkString
      source.close()
      contents should be (broadcastedString)
    }
    try {
      val broadcastDataFile: File = {
        val file = new File(tempDir, "broadcastData")
        val printWriter = new PrintWriter(file)
        printWriter.write(broadcastedString)
        printWriter.close()
        file
      }
      val broadcast = new PythonBroadcast(broadcastDataFile.getAbsolutePath)
      assertBroadcastIsValid(broadcast)
      val conf = new SparkConf().set("spark.kryo.registrationRequired", "true")
      val deserializedBroadcast =
        Utils.clone[PythonBroadcast](broadcast, new KryoSerializer(conf).newInstance())
      assertBroadcastIsValid(deserializedBroadcast)
    } finally {
      Utils.deleteRecursively(tempDir)
    }
  }
}
