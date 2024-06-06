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
package org.apache.spark.sql.application

import java.io.{InputStream, OutputStream}
import java.util.concurrent.Semaphore

import scala.util.control.NonFatal

import ammonite.compiler.CodeClassWrapper
import ammonite.compiler.iface.CodeWrapper
import ammonite.util.{Bind, Imports, Name, Util}

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.internal.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkConnectClientParser}

/**
 * REPL for spark connect.
 */
@DeveloperApi
object ConnectRepl {
  private val name = "Spark Connect REPL"

  private val splash =
    """
      |Spark session available as 'spark'.
      |   _____                  __      ______                            __
      |  / ___/____  ____ ______/ /__   / ____/___  ____  ____  ___  _____/ /_
      |  \__ \/ __ \/ __ `/ ___/ //_/  / /   / __ \/ __ \/ __ \/ _ \/ ___/ __/
      | ___/ / /_/ / /_/ / /  / ,<    / /___/ /_/ / / / / / / /  __/ /__/ /_
      |/____/ .___/\__,_/_/  /_/|_|   \____/\____/_/ /_/_/ /_/\___/\___/\__/
      |    /_/
      |""".stripMargin

  def main(args: Array[String]): Unit = doMain(args)

  private[application] def doMain(
      args: Array[String],
      semaphore: Option[Semaphore] = None,
      inputStream: InputStream = System.in,
      outputStream: OutputStream = System.out,
      errorStream: OutputStream = System.err): Unit = {
    // For interpreters, structured logging is disabled by default to avoid generating mixed
    // plain text and structured logs on the same console.
    Logging.disableStructuredLogging()

    // Build the client.
    val client =
      try {
        SparkConnectClient
          .builder()
          .loadFromEnvironment()
          .userAgent(name)
          .parse(args)
          .build()
      } catch {
        case NonFatal(e) =>
          // scalastyle:off println
          println(s"""
             |$name
             |${e.getMessage}
             |${SparkConnectClientParser.usage()}
             |""".stripMargin)
          // scalastyle:on println
          sys.exit(1)
      }

    // Build the session.
    val spark = SparkSession.builder().client(client).getOrCreate()
    val sparkBind = new Bind("spark", spark)

    // Add the proper imports and register a [[ClassFinder]].
    val predefCode =
      """
        |import org.apache.spark.sql.functions._
        |import spark.implicits._
        |import spark.sql
        |import org.apache.spark.sql.connect.client.AmmoniteClassFinder
        |
        |spark.registerClassFinder(new AmmoniteClassFinder(repl.sess))
        |""".stripMargin
    // Please note that we make ammonite generate classes instead of objects.
    // Classes tend to have superior serialization behavior when using UDFs.
    val main = ammonite.Main(
      welcomeBanner = Option(splash),
      predefCode = predefCode,
      replCodeWrapper = ExtendedCodeClassWrapper,
      scriptCodeWrapper = ExtendedCodeClassWrapper,
      inputStream = inputStream,
      outputStream = outputStream,
      errorStream = errorStream)
    if (semaphore.nonEmpty) {
      // Used for testing.
      main.run(sparkBind, new Bind[Semaphore]("semaphore", semaphore.get))
    } else {
      main.run(sparkBind)
    }
  }
}

/**
 * [[CodeWrapper]] that makes sure new Helper classes are always registered as an outer scope.
 */
@DeveloperApi
object ExtendedCodeClassWrapper extends CodeWrapper {
  override def wrapperPath: Seq[Name] = CodeClassWrapper.wrapperPath
  override def apply(
      code: String,
      source: Util.CodeSource,
      imports: Imports,
      printCode: String,
      indexedWrapper: Name,
      extraCode: String): (String, String, Int) = {
    val (top, bottom, level) =
      CodeClassWrapper(code, source, imports, printCode, indexedWrapper, extraCode)
    // Make sure we register the Helper before anything else, so outer scopes work as expected.
    val augmentedTop = top +
      "\norg.apache.spark.sql.catalyst.encoders.OuterScopes.addOuterScope(this)\n"
    (augmentedTop, bottom, level)
  }
}
