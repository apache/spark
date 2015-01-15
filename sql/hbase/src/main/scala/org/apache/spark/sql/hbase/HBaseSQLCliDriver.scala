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

package org.apache.spark.sql.hbase

import java.io.File

import jline._
import org.apache.spark.{Logging, SparkConf, SparkContext}

/**
 * HBaseSQLCliDriver
 *
 */
object HBaseSQLCliDriver extends Logging {
  private val prompt = "spark-hbaseql"
  private val continuedPrompt = "".padTo(prompt.length, ' ')
  private val conf = new SparkConf()
  private val sc = new SparkContext(conf)
  private val hbaseCtx = new HBaseSQLContext(sc)

  private val QUIT = "QUIT"
  private val EXIT = "EXIT"
  private val HELP = "HELP"

  def getCompletors: Seq[Completor] = {
    val sc: SimpleCompletor = new SimpleCompletor(new Array[String](0))

    // add keywords, including lower-cased versions
    HBaseSQLParser.getKeywords.foreach { kw =>
      sc.addCandidateString(kw)
      sc.addCandidateString(kw.toLowerCase)
    }


    Seq(sc)
  }

  def main(args: Array[String]) {

    val reader = new ConsoleReader()
    reader.setBellEnabled(false)
    getCompletors.foreach(reader.addCompletor)

    val historyDirectory = System.getProperty("user.home")

    try {
      if (new File(historyDirectory).exists()) {
        val historyFile = historyDirectory + File.separator + ".hbaseqlhistory"
        reader.setHistory(new History(new File(historyFile)))
      } else {
        System.err.println("WARNING: Directory for hbaseql history file: " + historyDirectory +
          " does not exist.   History will not be available during this session.")
      }
    } catch {
      case e: Exception =>
        System.err.println("WARNING: Encountered an error while trying to initialize hbaseql's " +
          "history file.  History will not be available during this session.")
        System.err.println(e.getMessage)
    }

    println("Welcome to hbaseql CLI")
    var prefix = ""

    def promptPrefix = s"$prompt"
    var currentPrompt = promptPrefix
    var line = reader.readLine(currentPrompt + "> ")
    var ret = 0

    while (line != null) {
      if (prefix.nonEmpty) {
        prefix += '\n'
      }

      if (line.trim.endsWith(";") && !line.trim.endsWith("\\;")) {
        line = prefix + line
        processLine(line, allowInterrupting = true)
        prefix = ""
        currentPrompt = promptPrefix
      } else {
        prefix = prefix + line
        currentPrompt = continuedPrompt
      }

      line = reader.readLine(currentPrompt + "> ")
    }

    System.exit(0)
  }

  private def processLine(line: String, allowInterrupting: Boolean) = {

    // TODO: handle multiple command separated by ;

    // Since we are using SqlParser and it does not handle ';', just work around to omit the ';'
    val input = line.trim.substring(0, line.length - 1)

    try {
      process(input)
    } catch {
      case e: Exception =>
        e.printStackTrace()
    }

  }

  private def process(input: String) = {
    val token = input.split("\\s")
    token(0).toUpperCase match {
      case QUIT => System.exit(0)
      case EXIT => System.exit(0)
      case HELP => printHelp(token)
      case "!" => // TODO: add support for bash command start with !
      case _ =>
        logInfo(s"Processing $input")
        val start = System.currentTimeMillis()
        val res = hbaseCtx.sql(input).collect()
        val end = System.currentTimeMillis()
        res.foreach(println)
        val timeTaken: Double = (end - start) / 1000.0
        println(s"Time taken: $timeTaken seconds")
    }
  }

  private def printHelp(token: Array[String]) = {
    if (token.length > 1) {
      token(1).toUpperCase match {
        case "CREATE" =>
          println( """CREATE TABLE table_name (col_name data_type, ..., PRIMARY KEY(col_name, ...))
                MAPPED BY (htable_name, COLS=[col_name=family_name.qualifier])""".stripMargin)
        case "DROP" =>
          println("DROP TABLE table_name")
        case "ALTER" =>
          println("ALTER TABLE table_name ADD (col_name data_type, ...) MAPPED BY (expression)")
          println("ALTER TABLE table_name DROP col_name")
        case "LOAD" =>
          println( """LOAD DATA [LOCAL] INPATH file_path [OVERWRITE] INTO TABLE
                table_name [FIELDS TERMINATED BY char]""".stripMargin)
        case "SELECT" =>
          println( """SELECT [ALL | DISTINCT] select_expr, select_expr, ...
                     |FROM table_reference
                     |[WHERE where_condition]
                     |[GROUP BY col_list]
                     |[CLUSTER BY col_list
                     |  | [DISTRIBUTE BY col_list] [SORT BY col_list]
                     |]
                     |[LIMIT number]""")
        case "INSERT" =>
          println("INSERT INTO table_name SELECT clause")
          println("INSERT INTO table_name VALUES (value, ...)")
        case "DESCRIBE" =>
          println("DESCRIBE table_name")
        case "SHOW" =>
          println("SHOW TABLES")
        case _ =>
          printHelpUsage()
      }
    } else {
      printHelpUsage()
    }
  }
 
  private def printHelpUsage() = {
    println("""Usage: HELP Statement    
      Statement:
        CREATE | DROP | ALTER | LOAD | SELECT | INSERT | DESCRIBE | SHOW""")    
  }
}

