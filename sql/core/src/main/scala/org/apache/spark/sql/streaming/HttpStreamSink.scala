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

/**
 * Created by zhangxinyu on 2016/9/18.
 */
trait HttpDataFormat{
  def format(data : Seq[Any]) : String
}

class HttpSink(options: Map[String, String]) extends Sink with Logging {
  val url : String = options.get("url").get
  val dataGenerateClass : String = options.getOrElse(
    "format.class", "org.apache.spark.sql.execution.streaming.HttpDataToStringDefault")

  override def addBatch(batchId : Long, data : DataFrame) : Unit = synchronized {
      data.collect().foreach(dataSeq => {
        val transObject = createObject[HttpDataFormat](dataGenerateClass)
        post(transObject.format(dataSeq.toSeq))
      })
  }

  def createObject[T<:AnyRef](className: String, args: AnyRef*): T = {
    val klass = Utils.classForName(className).asInstanceOf[Class[T]]
    val constructor = klass.getConstructor(args.map(_.getClass): _*)
    constructor.newInstance(args: _*)
  }

  def post(param : String) : Unit = {
    var out : PrintWriter = null
    var in : BufferedReader = null
    var result : String = ""
    try {
      val realUrl: URL = new URL(url)
      val connection: URLConnection = realUrl.openConnection
      connection.setRequestProperty("accept", "*/*")
      connection.setRequestProperty("connection", "Keep-Alive")
      connection.setRequestProperty("user-agent",
        "Mozilla/4.0 (compatible; MSIE 6.0; Windows NT 5.1;SV1)")
      connection.setDoInput(true)
      connection.setDoOutput(true)
      out = new PrintWriter(connection.getOutputStream)
      out.print(param)
      out.flush()
      in = new BufferedReader(new InputStreamReader(connection.getInputStream))
      var line: String = in.readLine()
      while (line != null) {
        result += line
        line = in.readLine()
      }
    } catch {
      case e: UnknownHostException => throw new UnknownHostException("Unknown host: " + url)
      case e: Exception => e.printStackTrace
    } finally {
      try {
        if (out != null) {
          out.close()
        }
        if (in != null) {
          in.close
        }
      } catch {
        case e2: Exception => e2.printStackTrace
      }
    }
  }
}

class HttpDataToStringDefault extends HttpDataFormat {
  def format(data : Seq[Any]) : String = {
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
      throw new AnalysisException("Set a http url to read from with option(\"url\", ...).")
    }
    new HttpSink(parameters)
  }

  def shortName(): String = "http"
}
