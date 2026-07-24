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

import java.io.{BufferedReader, InputStream, InputStreamReader, OutputStream, PrintStream}
import java.nio.charset.StandardCharsets
import java.nio.file.{Files, Paths}

import scala.util.control.NonFatal

import org.apache.spark.annotation.DeveloperApi
import org.apache.spark.sql.Row
import org.apache.spark.sql.connect.SparkSession
import org.apache.spark.sql.connect.SparkSession.withLocalConnectServer
import org.apache.spark.sql.connect.client.{SparkConnectClient, SparkConnectClientParser}

/**
 * A minimal `spark-sql` CLI that runs against a Spark Connect server (`spark-sql --remote
 * sc://...`). It mirrors [[ConnectRepl]] (SPARK-48936) for the SQL command shell rather than the
 * Scala REPL: it builds a remote [[SparkSession]] from the Connect client, executes statements
 * via `spark.sql(...)`, and prints tab-separated output collected on the client side.
 *
 * This is an intentionally minimal MVP. It supports `-e` / `-f` and an interactive loop. Full
 * `hiveResultString` output parity (types, complex/nested values, timestamps, aligned columns),
 * the remaining CLI flags (`--database`, `--help`, init files, ...), and prompt / `USE db`
 * semantics are deferred to follow-ups. The existing local
 * [[org.apache.spark.sql.hive.thriftserver.SparkSQLCLIDriver]] is untouched and remains the
 * default; there is no dependency on `sql/hive-thriftserver` here.
 */
@DeveloperApi
object SparkConnectSQLCLIDriver {
  // scalastyle:off println
  // This is a console CLI whose purpose is to print query results and diagnostics to
  // stdout/stderr, mirroring ConnectRepl. println is intentional here.
  private val name = "Spark Connect SQL CLI"

  /**
   * Driver options (`-e` statements and `-f` files) split from the remaining Connect client
   * arguments (e.g. `--remote`), which are forwarded verbatim to [[SparkConnectClientParser]].
   */
  private[application] case class CliArgs(
      statements: Seq[String],
      files: Seq[String],
      clientArgs: Array[String])

  def main(args: Array[String]): Unit = doMain(args)

  private[application] def doMain(
      args: Array[String],
      inputStream: InputStream = System.in,
      outputStream: OutputStream = System.out,
      errorStream: OutputStream = System.err): Unit = withLocalConnectServer {
    val out = new PrintStream(outputStream, true, StandardCharsets.UTF_8.name())
    val err = new PrintStream(errorStream, true, StandardCharsets.UTF_8.name())
    val cli = parseArgs(args)

    // Build the Connect client from the Connect arguments only. The client parser rejects
    // unknown flags, so the CLI options (-e / -f) must already have been stripped out.
    val client =
      try {
        SparkConnectClient
          .builder()
          .loadFromEnvironment()
          .userAgent(name)
          .parse(cli.clientArgs)
          .build()
      } catch {
        case NonFatal(e) =>
          err.println(s"$name\n${e.getMessage}\n${SparkConnectClientParser.usage()}")
          sys.exit(1)
      }

    val spark = SparkSession.builder().client(client).getOrCreate()
    try {
      val batch = cli.statements.flatMap(splitStatements) ++
        cli.files.flatMap(file => splitStatements(readFile(file)))
      if (batch.nonEmpty) {
        // Non-interactive mode (-e / -f): exit non-zero on the first failure (Hive semantics).
        batch.foreach { statement =>
          try {
            run(spark, statement, out)
          } catch {
            case NonFatal(e) =>
              err.println(s"Error in statement: $statement\n${e.getMessage}")
              sys.exit(1)
          }
        }
      } else {
        interactiveLoop(spark, inputStream, out, err)
      }
    } finally {
      spark.close()
    }
  }

  /** Execute a single statement and print its result as tab-separated rows. */
  private def run(spark: SparkSession, statement: String, out: PrintStream): Unit = {
    spark.sql(statement).collect().foreach(row => out.println(formatRow(row)))
  }

  /** Format a [[Row]] as tab-separated values, rendering nulls as "NULL". */
  private[application] def formatRow(row: Row): String = {
    val fields = new Array[String](row.length)
    var i = 0
    while (i < row.length) {
      val value = row.get(i)
      fields(i) = if (value == null) "NULL" else value.toString
      i += 1
    }
    fields.mkString("\t")
  }

  private def isExit(statement: String): Boolean =
    statement.equalsIgnoreCase("quit") || statement.equalsIgnoreCase("exit")

  private def interactiveLoop(
      spark: SparkSession,
      inputStream: InputStream,
      out: PrintStream,
      err: PrintStream): Unit = {
    val reader = new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
    val prompt = "spark-sql (connect)> "
    val buffer = new StringBuilder
    var running = true
    out.print(prompt)
    var line = reader.readLine()
    while (running && line != null) {
      buffer.append(line).append('\n')
      if (line.contains(";")) {
        val statements = splitStatements(buffer.toString)
        buffer.clear()
        val iter = statements.iterator
        while (running && iter.hasNext) {
          val statement = iter.next()
          if (isExit(statement)) {
            running = false
          } else {
            try {
              run(spark, statement, out)
            } catch {
              case NonFatal(e) => err.println(e.getMessage)
            }
          }
        }
      }
      if (running) {
        out.print(prompt)
        line = reader.readLine()
      }
    }
  }

  /**
   * Split a block of text into individual statements on ';'.
   *
   * This MVP uses a naive split and does not handle ';' inside string literals or comments. A
   * robust quote/comment-aware splitter exists (`StringUtils.splitSemiColon`) but only in the
   * catalyst module, which this Connect client must not depend on. A follow-up could relocate
   * that helper to a shared low-level module (e.g. sql-api) for reuse here, or give the client
   * its own. Guidance on the preferred layering is welcome.
   */
  private[application] def splitStatements(text: String): Seq[String] = {
    text.split(';').map(_.trim).filter(_.nonEmpty).toSeq
  }

  private def readFile(path: String): String = {
    new String(Files.readAllBytes(Paths.get(path)), StandardCharsets.UTF_8)
  }

  /**
   * Split raw CLI arguments into driver options (`-e` / `-f`) and the remaining Connect client
   * arguments, which are forwarded verbatim to [[SparkConnectClientParser]].
   */
  private[application] def parseArgs(args: Array[String]): CliArgs = {
    val statements = Seq.newBuilder[String]
    val files = Seq.newBuilder[String]
    val clientArgs = Array.newBuilder[String]
    var i = 0
    while (i < args.length) {
      args(i) match {
        case "-e" if i + 1 < args.length =>
          statements += args(i + 1)
          i += 2
        case "-f" if i + 1 < args.length =>
          files += args(i + 1)
          i += 2
        case other =>
          clientArgs += other
          i += 1
      }
    }
    CliArgs(statements.result(), files.result(), clientArgs.result())
  }
  // scalastyle:on println
}
