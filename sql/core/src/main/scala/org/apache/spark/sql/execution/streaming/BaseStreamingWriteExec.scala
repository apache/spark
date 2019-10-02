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

package org.apache.spark.sql.execution.streaming

import scala.collection.JavaConverters._

import org.apache.spark.sql.catalyst.expressions.Attribute
import org.apache.spark.sql.catalyst.streaming.InternalOutputModes.{Append, Complete, Update}
import org.apache.spark.sql.connector.catalog.SupportsWrite
import org.apache.spark.sql.connector.write.SupportsTruncate
import org.apache.spark.sql.execution.{SparkPlan, UnaryExecNode}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.util.CaseInsensitiveStringMap
import org.apache.spark.util.Utils

trait BaseStreamingWriteExec extends UnaryExecNode {
  def table: SupportsWrite
  def query: SparkPlan
  def queryId: String
  def querySchema: StructType
  def outputMode: OutputMode
  def options: Map[String, String]

  override def child: SparkPlan = query
  override def output: Seq[Attribute] = Nil

  protected lazy val inputRDD = query.execute()
  lazy val streamWrite = {
    val writeBuilder = table.newWriteBuilder(new CaseInsensitiveStringMap(options.asJava))
      .withQueryId(queryId)
      .withInputDataSchema(querySchema)
    outputMode match {
      case Append =>
        writeBuilder.buildForStreaming()

      case Complete =>
        // TODO: we should do this check earlier when we have capability API.
        require(writeBuilder.isInstanceOf[SupportsTruncate],
          table.name + " does not support Complete mode.")
        writeBuilder.asInstanceOf[SupportsTruncate].truncate().buildForStreaming()

      case Update =>
        // Although no v2 sinks really support Update mode now, but during tests we do want them
        // to pretend to support Update mode, and treat Update mode same as Append mode.
        if (Utils.isTesting) {
          writeBuilder.buildForStreaming()
        } else {
          throw new IllegalArgumentException(
            "Data source v2 streaming sinks does not support Update mode.")
        }
    }
  }
}
