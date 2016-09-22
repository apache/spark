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
import java.net.{UnknownHostException, URL, URLConnection}

import org.apache.spark.internal.Logging
import org.apache.spark.sql._
import org.apache.spark.sql.sources.{DataSourceRegister, StreamSinkProvider}
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.util.Utils

trait HttpDataFormat{
  def format(data: Seq[Any]): String
}

class HttpSink(options: Map[String, String]) extends Sink with Logging {
  override def addBatch(batchId: Long, data: DataFrame): Unit = synchronized {
    val dataFormat: HttpDataFormat = {
      val className = options.getOrElse("format.class",
        "org.apache.spark.sql.execution.streaming.HttpDataToStringDefault")
      createObject[HttpDataFormat](className)
    }
    data.collect().foreach(dataSeq => {
        post(dataFormat.format(dataSeq.toSeq))
    })
  }

  private def createObject[T<:AnyRef](className: String, args: AnyRef*): T = {
    val klass = Utils.classForName(className).asInstanceOf[Class[T]]
    val constructor = klass.getConstructor(args.map(_.getClass): _*)
    constructor.newInstance(args: _*)
  }

  private def post(param: String): Unit = {
    val url: URL = new URL(options.get("url").get)
    val connection: URLConnection = url.openConnection
    connection.setDoInput(true)
    connection.setDoOutput(true)
    val writer = new PrintWriter(connection.getOutputStream)
    try {
      writer.print(param)
      writer.flush()
    } catch {
      case cause: Throwable => {
        logError("Post http request error: ", cause)
      }
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
      logError("Http request post result: " + lines)
    } catch {
      case cause: Throwable => {
        logError("Read http result error: ", cause)
      }
    } finally {
      reader.close()
    }
  }
}

class HttpDataToStringDefault extends HttpDataFormat {
  def format(data: Seq[Any]) : String = {
    return data.mkString(", ")
  }
}

class HttpStreamSink extends StreamSinkProvider with DataSourceRegister{
  def createSink(
                  sqlContext: SQLContext,
                  parameters: Map[String, String],
                  partitionColumns: Seq[String],
                  outputMode: OutputMode): Sink = {
    if (!parameters.contains("url")) {
      throw new AnalysisException("Http url should be set: .option(\"url\", \"...\").")
    }
    new HttpSink(parameters)
  }

  def shortName(): String = "http"
}

