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

package org.apache.spark.repl

import java.io.{BufferedReader, FileReader}

import Predef.{println => _, _}
import scala.util.Properties.{jdkHome, javaVersion, versionString, javaVmName}

import scala.tools.nsc.interpreter.{JPrintWriter, ILoop}
import scala.tools.nsc.Settings
import scala.tools.nsc.util.stringFromStream

/**
 *  A Spark-specific interactive shell.
 */
class SparkILoop(in0: Option[BufferedReader], out: JPrintWriter)
    extends ILoop(in0, out) {
  def this(in0: BufferedReader, out: JPrintWriter) = this(Some(in0), out)
  def this() = this(None, new JPrintWriter(Console.out, true))

  def initializeSpark() {
    intp.beQuietDuring {
      processLine("""
         @transient val sc = {
           val _sc = org.apache.spark.repl.Main.createSparkContext()
           println("Spark context available as sc.")
           _sc
         }
        """)
      processLine("""
         @transient val sqlContext = {
           val _sqlContext = org.apache.spark.repl.Main.createSQLContext()
           println("SQL context available as sqlContext.")
           _sqlContext
         }
        """)
      processLine("import org.apache.spark.SparkContext._")
      processLine("import sqlContext.implicits._")
      processLine("import sqlContext.sql")
      processLine("import org.apache.spark.sql.functions._")
    }
  }

  /** Print a welcome message */
  override def printWelcome() {
    import org.apache.spark.SPARK_VERSION
    echo("""Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version %s
      /_/
         """.format(SPARK_VERSION))
    val welcomeMsg = "Using Scala %s (%s, Java %s)".format(
      versionString, javaVmName, javaVersion)
    echo(welcomeMsg)
    echo("Type in expressions to have them evaluated.")
    echo("Type :help for more information.")
  }

  import LoopCommand.{ cmd, nullary }

  private val blockedCommands = Set("implicits", "javap", "power", "type", "kind")

  /** Standard commands **/
  lazy val sparkStandardCommands: List[SparkILoop.this.LoopCommand] =
    standardCommands.filter(cmd => !blockedCommands(cmd.name))

  /** Available commands */
  override def commands: List[LoopCommand] = sparkStandardCommands

  /** 
   * We override `loadFiles` because we need to initialize Spark *before* the REPL
   * sees any files, so that the Spark context is visible in those files. This is a bit of a
   * hack, but there isn't another hook available to us at this point.
   */
  override def loadFiles(settings: Settings): Unit = {
    initializeSpark()
    super.loadFiles(settings)
  }
}

object SparkILoop {

  /** 
   * Creates an interpreter loop with default settings and feeds
   * the given code to it as input.
   */
  def run(code: String, sets: Settings = new Settings): String = {
    import java.io.{ BufferedReader, StringReader, OutputStreamWriter }

    stringFromStream { ostream =>
      Console.withOut(ostream) {
        val input = new BufferedReader(new StringReader(code))
        val output = new JPrintWriter(new OutputStreamWriter(ostream), true)
        val repl = new SparkILoop(input, output)

        if (sets.classpath.isDefault)
          sets.classpath.value = sys.props("java.class.path")

        repl process sets
      }
    }
  }
  def run(lines: List[String]): String = run(lines.map(_ + "\n").mkString)
}
