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

package org.apache.spark.rdd

import java.io.PrintWriter
import java.util.StringTokenizer

import scala.collection.Map
import scala.collection.JavaConversions._
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.reflect.ClassTag

import org.apache.spark.{SparkEnv, Partition, TaskContext}
import org.apache.spark.broadcast.Broadcast


/**
 * An RDD that pipes the contents of each parent partition through an external command
 * (printing them one per line) and returns the output as a collection of strings.
 */
class PipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    envVars: Map[String, String],
    printPipeContext: (String => Unit) => Unit,
    printRDDElement: (T, String => Unit) => Unit)
  extends RDD[String](prev) {

  // Similar to Runtime.exec(), if we are given a single string, split it into words
  // using a standard StringTokenizer (i.e. by spaces)
  def this(
      prev: RDD[T],
      command: String,
      envVars: Map[String, String] = Map(),
      printPipeContext: (String => Unit) => Unit = null,
      printRDDElement: (T, String => Unit) => Unit = null) =
    this(prev, PipedRDD.tokenize(command), envVars, printPipeContext, printRDDElement)


  override def getPartitions: Array[Partition] = firstParent[T].partitions

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val pb = new ProcessBuilder(command)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    val proc = pb.start()
    val env = SparkEnv.get

    // Start a thread to print the process's stderr to ours
    new Thread("stderr reader for " + command) {
      override def run() {
        for (line <- Source.fromInputStream(proc.getErrorStream).getLines) {
          System.err.println(line)
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread("stdin writer for " + command) {
      override def run() {
        SparkEnv.set(env)
        val out = new PrintWriter(proc.getOutputStream)

        // input the pipe context firstly
        if (printPipeContext != null) {
          printPipeContext(out.println(_))
        }
        for (elem <- firstParent[T].iterator(split, context)) {
          if (printRDDElement != null) {
            printRDDElement(elem, out.println(_))
          } else {
            out.println(elem)
          }
        }
        out.close()
      }
    }.start()

    // Return an iterator that read lines from the process's stdout
    val lines = Source.fromInputStream(proc.getInputStream).getLines
    return new Iterator[String] {
      def next() = lines.next()
      def hasNext = {
        if (lines.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          if (exitStatus != 0) {
            throw new Exception("Subprocess exited with status " + exitStatus)
          }
          false
        }
      }
    }
  }
}

object PipedRDD {
  // Split a string into words using a standard StringTokenizer
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf
  }
}
