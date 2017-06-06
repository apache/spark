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

package org.apache.spark.scheduler.cluster.nomad

import java.io.File
import java.nio.charset.StandardCharsets.UTF_8

import com.google.common.io.Files
import org.apache.http.client.methods.HttpPut
import org.apache.http.entity.StringEntity
import org.apache.http.impl.client.HttpClientBuilder

import org.apache.spark.internal.Logging

private[spark] trait TestApplication extends Logging {

  def checkArgs(args: Array[String])(expectedArgs: String*): Unit = {
    if (args.length != expectedArgs.size) {
      usageError(s"expected ${expectedArgs.size} argument(s) but got ${args.length}: $args") {
        expectedArgs.map(arg => s"<$arg>").mkString(" ")
      }
    }
  }

  def usageError(m: String)(argsUsage: String): Nothing = {
    fatalError(
      s"""Error: $m
         |
         |Usage: ${getClass.getSimpleName} $argsUsage
         |""".stripMargin
    )
  }

  def fatalError(m: String, ex: Throwable = null): Nothing = {
    logError(m, ex)

    // scalastyle:off println
    System.out.println(m)
    // scalastyle:on println
    if (ex != null) {
      ex.printStackTrace(System.out)
    }

    sys.exit(1)
  }

  def httpPut(url: String)(computeContents: => String): Unit = {
    try {
      logInfo(s"Preparing contents to PUT to $url")
      val contentsToPut =
        try computeContents
        catch { case e: Throwable => s"ERROR computing contents: $e" }

      logInfo(s"Putting to $url: $contentsToPut")
      val put = new HttpPut(url)
      put.setEntity(new StringEntity(contentsToPut, UTF_8))
      val http = HttpClientBuilder.create().build()
      try {
        val response = http.execute(put)
        if (response.getStatusLine.getStatusCode / 100 != 2) {
          sys.error(s"Got non-2xx response: $response")
        }
        logInfo(s"PUT to $url complete")
      } finally http.close()
    } catch {
      case e: Throwable =>
        throw new RuntimeException(s"ERROR putting to $url: $e", e)
    }
  }

  def writeReadAndPutFileContents(contents: String, outputUrl: String): Unit =
    httpPut(outputUrl) {
      val dir = new File("target")
      dir.mkdir()
      val file = new File(dir, "temp.txt")
      Files.write(contents, file, UTF_8)
      Files.toString(file, UTF_8)
    }

  def assertEquals[A](a: A, b: A): Unit = {
    if (a != b) {
      throw new RuntimeException(s"$a is not equal to $b")
    }
  }

}
