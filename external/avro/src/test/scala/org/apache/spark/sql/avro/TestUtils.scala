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

package org.apache.spark.sql.avro

import java.io.{File, IOException}
import java.nio.ByteBuffer

import scala.collection.immutable.HashSet
import scala.collection.mutable.ArrayBuffer
import scala.util.Random

import com.google.common.io.Files
import java.util

import org.apache.spark.sql.SparkSession

private[avro] object TestUtils {

  /**
   * This function checks that all records in a file match the original
   * record.
   */
  def checkReloadMatchesSaved(spark: SparkSession, testFile: String, avroDir: String): Unit = {

    def convertToString(elem: Any): String = {
      elem match {
        case null => "NULL" // HashSets can't have null in them, so we use a string instead
        case arrayBuf: ArrayBuffer[_] =>
          arrayBuf.asInstanceOf[ArrayBuffer[Any]].toArray.deep.mkString(" ")
        case arrayByte: Array[Byte] => arrayByte.deep.mkString(" ")
        case other => other.toString
      }
    }

    val originalEntries = spark.read.avro(testFile).collect()
    val newEntries = spark.read.avro(avroDir).collect()

    assert(originalEntries.length == newEntries.length)

    val origEntrySet = Array.fill(originalEntries(0).size)(new HashSet[Any]())
    for (origEntry <- originalEntries) {
      var idx = 0
      for (origElement <- origEntry.toSeq) {
        origEntrySet(idx) += convertToString(origElement)
        idx += 1
      }
    }

    for (newEntry <- newEntries) {
      var idx = 0
      for (newElement <- newEntry.toSeq) {
        assert(origEntrySet(idx).contains(convertToString(newElement)))
        idx += 1
      }
    }
  }

  def withTempDir(f: File => Unit): Unit = {
    val dir = Files.createTempDir()
    dir.delete()
    try f(dir) finally deleteRecursively(dir)
  }

  /**
   * This function deletes a file or a directory with everything that's in it. This function is
   * copied from Spark with minor modifications made to it. See original source at:
   * github.com/apache/spark/blob/master/core/src/main/scala/org/apache/spark/util/Utils.scala
   */

  def deleteRecursively(file: File) {
    def listFilesSafely(file: File): Seq[File] = {
      if (file.exists()) {
        val files = file.listFiles()
        if (files == null) {
          throw new IOException("Failed to list files for dir: " + file)
        }
        files
      } else {
        List()
      }
    }

    if (file != null) {
      try {
        if (file.isDirectory) {
          var savedIOException: IOException = null
          for (child <- listFilesSafely(file)) {
            try {
              deleteRecursively(child)
            } catch {
              // In case of multiple exceptions, only last one will be thrown
              case ioe: IOException => savedIOException = ioe
            }
          }
          if (savedIOException != null) {
            throw savedIOException
          }
        }
      } finally {
        if (!file.delete()) {
          // Delete can also fail if the file simply did not exist
          if (file.exists()) {
            throw new IOException("Failed to delete: " + file.getAbsolutePath)
          }
        }
      }
    }
  }

  /**
   * This function generates a random map(string, int) of a given size.
   */
  private[avro] def generateRandomMap(rand: Random, size: Int): java.util.Map[String, Int] = {
    val jMap = new util.HashMap[String, Int]()
    for (i <- 0 until size) {
      jMap.put(rand.nextString(5), i)
    }
    jMap
  }

  /**
   * This function generates a random array of booleans of a given size.
   */
  private[avro] def generateRandomArray(rand: Random, size: Int): util.ArrayList[Boolean] = {
    val vec = new util.ArrayList[Boolean]()
    for (i <- 0 until size) {
      vec.add(rand.nextBoolean())
    }
    vec
  }

  /**
   * This function generates a random ByteBuffer of a given size.
   */
  private[avro] def generateRandomByteBuffer(rand: Random, size: Int): ByteBuffer = {
    val bb = ByteBuffer.allocate(size)
    val arrayOfBytes = new Array[Byte](size)
    rand.nextBytes(arrayOfBytes)
    bb.put(arrayOfBytes)
  }
}
