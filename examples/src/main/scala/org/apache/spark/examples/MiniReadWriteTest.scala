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

// scalastyle:off println
package org.apache.spark.examples

import java.io.File
import java.io.PrintWriter

import scala.io.Source._

import org.apache.spark.sql.SparkSession
import org.apache.spark.util.Utils

/**
 * Simple test for reading and writing to a distributed
 * file system.  This example does the following:
 *
 *   1. Reads local file
 *   2. Computes word count on local file
 *   3. Writes local file to a local dir on each executor
 *   4. Reads the file back from each exec
 *   5. Computes word count on the file using Spark
 *   6. Compares the word count results
 */
object MiniReadWriteTest {

  private val NPARAMS = 1

  private def readFile(filename: String): List[String] = {
    Utils.tryWithResource(fromFile(filename))(_.getLines().toList)
  }

  private def printUsage(): Unit = {
    val usage = """Mini Read-Write Test
    |Usage: localFile
    |localFile - (string) location of local file to distribute to executors.""".stripMargin

    println(usage)
  }

  private def parseArgs(args: Array[String]): File = {
    if (args.length != NPARAMS) {
      printUsage()
      System.exit(1)
    }

    val filePath = args(0)

    val localFilePath = new File(filePath)
    if (!localFilePath.exists) {
      System.err.println(s"Given path ($filePath) does not exist")
      printUsage()
      System.exit(1)
    }

    if (!localFilePath.isFile) {
      System.err.println(s"Given path ($filePath) is not a file")
      printUsage()
      System.exit(1)
    }
    localFilePath
  }

  def runLocalWordCount(fileContents: List[String]): Int = {
    fileContents.flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .groupBy(w => w)
      .transform((_, v) => v.size)
      .values
      .sum
  }

  def main(args: Array[String]): Unit = {
    val localFilePath = parseArgs(args)

    println(s"Performing local word count from ${localFilePath}")
    val fileContents = readFile(localFilePath.toString())
    println(s"File contents are ${fileContents}")
    val localWordCount = runLocalWordCount(fileContents)

    println("Creating SparkSession")
    val spark = SparkSession
      .builder()
      .appName("Mini Read Write Test")
      .getOrCreate()

    println("Writing local file to executors")

    // uses the fact default parallelism is greater than num execs
    val misc = spark.sparkContext.parallelize(1.to(10))
    misc.foreachPartition {
      x =>
        new PrintWriter(localFilePath) {
          try {
            write(fileContents.mkString("\n"))
          } finally {
            close()
          }}
    }

    println("Reading file from execs and running Word Count")
    val readFileRDD = spark.sparkContext.textFile(localFilePath.toString())

    val dWordCount = readFileRDD
      .flatMap(_.split(" "))
      .flatMap(_.split("\t"))
      .filter(_.nonEmpty)
      .map(w => (w, 1))
      .countByKey()
      .values
      .sum

    spark.stop()
    if (localWordCount == dWordCount) {
      println(s"Success! Local Word Count $localWordCount and " +
        s"D Word Count $dWordCount agree.")
    } else {
      println(s"Failure! Local Word Count $localWordCount " +
        s"and D Word Count $dWordCount disagree.")
    }
  }
}
// scalastyle:on println
