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

package org.apache.spark.sql.hive.execution

import java.io.{BufferedReader, InputStreamReader}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.execution._
import org.apache.spark.sql.hive.HiveContext

/* Implicit conversions */
import scala.collection.JavaConversions._

/**
 * :: DeveloperApi ::
 * Transforms the input by forking and running the specified script.
 *
 * @param input the set of expression that should be passed to the script.
 * @param script the command that should be executed.
 * @param output the attributes that are produced by the script.
 */
@DeveloperApi
case class ScriptTransformation(
    input: Seq[Expression],
    script: String,
    output: Seq[Attribute],
    child: SparkPlan)(@transient sc: HiveContext)
  extends UnaryNode {

  override def otherCopyArgs = sc :: Nil

  def execute() = {
    child.execute().mapPartitions { iter =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))

      // TODO: This should be exposed as an iterator instead of reading in all the data at once.
      val outputLines = collection.mutable.ArrayBuffer[Row]()
      val readerThread = new Thread("Transform OutputReader") {
        override def run() {
          var curLine = reader.readLine()
          while (curLine != null) {
            // TODO: Use SerDe
            outputLines += new GenericRow(curLine.split("\t").asInstanceOf[Array[Any]])
            curLine = reader.readLine()
          }
        }
      }
      readerThread.start()
      val outputProjection = new InterpretedProjection(input, child.output)
      iter
        .map(outputProjection)
        // TODO: Use SerDe
        .map(_.mkString("", "\t", "\n").getBytes("utf-8")).foreach(outputStream.write)
      outputStream.close()
      readerThread.join()
      outputLines.toIterator
    }
  }
}
