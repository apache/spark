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
    child: SparkPlan,
    inputFormat: Seq[(String, String)],
    outputFormat: Seq[(String, String)])(@transient sc: HiveContext, schemaLess: Boolean)
  extends UnaryNode {

  override def otherCopyArgs = sc :: Nil

  val defaultFormat = Map(("TOK_TABLEROWFORMATFIELD", "\t"),
                          ("TOK_TABLEROWFORMATLINES", "\n"))

  val inputFormatMap = inputFormat.toMap.withDefault((k) => defaultFormat(k))
  val outputFormatMap = outputFormat.toMap.withDefault((k) => defaultFormat(k))

  def execute() = {
    child.execute().mapPartitions { iter =>
      val cmd = List("/bin/bash", "-c", script)
      val builder = new ProcessBuilder(cmd)
      val proc = builder.start()
      val inputStream = proc.getInputStream
      val outputStream = proc.getOutputStream
      val reader = new BufferedReader(new InputStreamReader(inputStream))

      val iterator: Iterator[Row] = new Iterator[Row] {
        var curLine: String = null
        override def hasNext: Boolean = {
          if (curLine == null) {
            curLine = reader.readLine()
            curLine != null
          } else {
            true
          }
        }
        override def next(): Row = {
          if (!hasNext) {
            throw new NoSuchElementException
          }
          val prevLine = curLine
          curLine = reader.readLine()
          // TODO: Use SerDe
          if (!schemaLess) {
            new GenericRow(
              prevLine.split(outputFormatMap("TOK_TABLEROWFORMATFIELD")).asInstanceOf[Array[Any]])
          } else {
            new GenericRow(
              prevLine.split(outputFormatMap("TOK_TABLEROWFORMATFIELD"), 2).asInstanceOf[Array[Any]])
          }
        }
      }

      val outputProjection = new InterpretedProjection(input, child.output)
      iter
        .map(outputProjection)
        // TODO: Use SerDe
        .map(_.mkString("", inputFormatMap("TOK_TABLEROWFORMATFIELD"),
          inputFormatMap("TOK_TABLEROWFORMATLINES")).getBytes("utf-8"))
        .foreach(outputStream.write)
      outputStream.close()
      iterator
    }
  }
}
