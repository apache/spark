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

import java.io.BufferedWriter
import java.io.File
import java.io.FilenameFilter
import java.io.IOException
import java.io.OutputStreamWriter
import java.io.PrintWriter
import java.util.StringTokenizer
import java.util.concurrent.atomic.AtomicReference

import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Source
import scala.jdk.CollectionConverters._
import scala.reflect.ClassTag

import org.apache.spark.{Partition, TaskContext}
import org.apache.spark.errors.SparkCoreErrors
import org.apache.spark.internal.LogKey.{COMMAND, ERROR, PATH}
import org.apache.spark.internal.MDC
import org.apache.spark.util.Utils


/**
 * An RDD that pipes the contents of each parent partition through an external command
 * (printing them one per line) and returns the output as a collection of strings.
 */
private[spark] class PipedRDD[T: ClassTag](
    prev: RDD[T],
    command: Seq[String],
    envVars: Map[String, String],
    printPipeContext: (String => Unit) => Unit,
    printRDDElement: (T, String => Unit) => Unit,
    separateWorkingDir: Boolean,
    bufferSize: Int,
    encoding: String)
  extends RDD[String](prev) {

  override def getPartitions: Array[Partition] = firstParent[T].partitions

  /**
   * A FilenameFilter that accepts anything that isn't equal to the name passed in.
   * @param filterName of file or directory to leave out
   */
  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[String] = {
    val pb = new ProcessBuilder(command.asJava)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // for compatibility with Hadoop which sets these env variables
    // so the user code can access the input filename
    split match {
      case hadoopSplit: HadoopPartition =>
        currentEnvVars.putAll(hadoopSplit.getPipeEnvVars().asJava)
      case _ => // do nothing
    }

    // When spark.worker.separated.working.directory option is turned on, each
    // task will be run in separate directory. This should be resolve file
    // access conflict issue
    val taskDirectory = "tasks" + File.separator + java.util.UUID.randomUUID.toString
    var workInTaskDirectory = false
    logDebug("taskDirectory = " + taskDirectory)
    if (separateWorkingDir) {
      val currentDir = new File(".")
      logDebug("currentDir = " + currentDir.getAbsolutePath())
      val taskDirFile = new File(taskDirectory)
      taskDirFile.mkdirs()

      try {
        val tasksDirFilter = new NotEqualsFileNameFilter("tasks")

        // Need to add symlinks to jars, files, and directories.  On Yarn we could have
        // directories and other files not known to the SparkContext that were added via the
        // Hadoop distributed cache.  We also don't want to symlink to the /tasks directories we
        // are creating here.
        for (file <- currentDir.list(tasksDirFilter)) {
          val fileWithDir = new File(currentDir, file)
          Utils.symlink(new File(fileWithDir.getAbsolutePath()),
            new File(taskDirectory + File.separator + fileWithDir.getName()))
        }
        pb.directory(taskDirFile)
        workInTaskDirectory = true
      } catch {
        case e: Exception =>
          logError(log"Unable to setup task working directory: ${MDC(ERROR, e.getMessage)}" +
          log" (${MDC(PATH, taskDirectory)})", e)
      }
    }

    val proc = pb.start()
    val childThreadException = new AtomicReference[Throwable](null)

    // Start a thread to print the process's stderr to ours
    val stderrReaderThread = new Thread(s"${PipedRDD.STDERR_READER_THREAD_PREFIX} $command") {
      override def run(): Unit = {
        val err = proc.getErrorStream
        try {
          for (line <- Source.fromInputStream(err)(encoding).getLines()) {
            // scalastyle:off println
            System.err.println(line)
            // scalastyle:on println
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          err.close()
        }
      }
    }
    stderrReaderThread.start()

    // Start a thread to feed the process input from our parent's iterator
    val stdinWriterThread = new Thread(s"${PipedRDD.STDIN_WRITER_THREAD_PREFIX} $command") {
      override def run(): Unit = {
        TaskContext.setTaskContext(context)
        val out = new PrintWriter(new BufferedWriter(
          new OutputStreamWriter(proc.getOutputStream, encoding), bufferSize))
        try {
          // scalastyle:off println
          // input the pipe context firstly
          if (printPipeContext != null) {
            printPipeContext(out.println)
          }
          for (elem <- firstParent[T].iterator(split, context)) {
            if (printRDDElement != null) {
              printRDDElement(elem, out.println)
            } else {
              out.println(elem)
            }
          }
          // scalastyle:on println
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          out.close()
        }
      }
    }
    stdinWriterThread.start()

    // interrupts stdin writer and stderr reader threads when the corresponding task is finished.
    // Otherwise, these threads could outlive the task's lifetime. For example:
    //   val pipeRDD = sc.range(1, 100).pipe(Seq("cat"))
    //   val abnormalRDD = pipeRDD.mapPartitions(_ => Iterator.empty)
    // the iterator generated by PipedRDD is never involved. If the parent RDD's iterator takes a
    // long time to generate(ShuffledRDD's shuffle operation for example), the stdin writer thread
    // may consume significant memory and CPU time even if task is already finished.
    context.addTaskCompletionListener[Unit] { _ =>
      if (proc.isAlive) {
        proc.destroy()
      }

      if (stdinWriterThread.isAlive) {
        stdinWriterThread.interrupt()
      }
      if (stderrReaderThread.isAlive) {
        stderrReaderThread.interrupt()
      }
    }

    // Return an iterator that read lines from the process's stdout
    val lines = Source.fromInputStream(proc.getInputStream)(encoding).getLines()
    new Iterator[String] {
      def next(): String = {
        if (!hasNext) {
          throw SparkCoreErrors.noSuchElementError()
        }
        lines.next()
      }

      def hasNext: Boolean = {
        val result = if (lines.hasNext) {
          true
        } else {
          val exitStatus = proc.waitFor()
          cleanup()
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
          false
        }
        propagateChildException()
        result
      }

      private def cleanup(): Unit = {
        // cleanup task working directory if used
        if (workInTaskDirectory) {
          scala.util.control.Exception.ignoring(classOf[IOException]) {
            Utils.deleteRecursively(new File(taskDirectory))
          }
          logDebug(s"Removed task working directory $taskDirectory")
        }
      }

      private def propagateChildException(): Unit = {
        val t = childThreadException.get()
        if (t != null) {
          val commandRan = command.mkString(" ")
          logError(log"Caught exception while running pipe() operator. Command ran: " +
            log"${MDC(COMMAND, commandRan)}. Exception: ${MDC(ERROR, t.getMessage)}")
          proc.destroy()
          cleanup()
          throw t
        }
      }
    }
  }
}

private object PipedRDD {
  // Split a string into words using a standard StringTokenizer
  def tokenize(command: String): Seq[String] = {
    val buf = new ArrayBuffer[String]
    val tok = new StringTokenizer(command)
    while(tok.hasMoreElements) {
      buf += tok.nextToken()
    }
    buf.toSeq
  }

  val STDIN_WRITER_THREAD_PREFIX = "stdin writer for"
  val STDERR_READER_THREAD_PREFIX = "stderr reader for"
}
