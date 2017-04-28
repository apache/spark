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

import java.io.{BufferedReader, InputStreamReader, PrintWriter}
import java.net.{URL, URLConnection}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.types.{StringType, StructType}


class HttpStreamSink extends StreamSinkProvider with DataSourceRegister{
  override def createSink(
      sqlContext: SQLContext,
      parameters: Map[String, String],
      partitionColumns: Seq[String],
      outputMode: OutputMode): Sink = {
    if (!parameters.contains("url")) {
      throw new AnalysisException("Http url should be set: .option(\"url\", \"...\").")
    }
    new HttpSink(parameters)
  }

  override def shortName(): String = "http"
}

/**
 * A sink that outputs streaming query results through sending http post request. Each [[Row]]
 * in batch will be post to a http url.
 * Each [[Row]] in batch must only have one single column, and the column type should be
 * [[StringType]].
 */
class HttpSink(options: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    verifySchema(data.schema)
    data.collect().foreach(dataSeq => {
        post(dataSeq.get(0).toString)
    })
  }
  private def verifySchema(schema: StructType): Unit = {
    if (schema.size != 1) {
      throw new AnalysisException(
        s"Http data sink supports only a single column, and you have ${schema.size} columns.")
    }
    val tpe = schema(0).dataType
    if (tpe != StringType) {
      throw new AnalysisException(
        s"Http data sink supports only a string column, but you have ${tpe.simpleString}.")
    }
  }
  private def post(data: String): Unit = {
    val url: URL = new URL(options.get("url").get)
    val connection: URLConnection = url.openConnection
    connection.setDoInput(true)
    connection.setDoOutput(true)
    val writer = new PrintWriter(connection.getOutputStream)
    try {
      writer.print(data)
      writer.flush()
    } catch {
      case cause: Throwable => logError("Post http request error: ", cause)
    } finally {
      writer.close()
    }
    val reader = new BufferedReader(new InputStreamReader(connection.getInputStream))
    try {
      val it = reader.lines().iterator()
      var lines: String = ""
      while (it.hasNext()) {
        lines += it.next()
      }
      logTrace(s"Http request post result: ${lines}.")
    } catch {
      case cause: Throwable => logError("Read http result error: ", cause)
    } finally {
      reader.close()
    }
  }
}
