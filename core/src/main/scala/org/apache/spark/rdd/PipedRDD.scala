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

import java.io._
import java.util
import java.util.StringTokenizer
import java.util.concurrent.atomic.AtomicReference

import scala.collection.JavaConverters._
import scala.collection.Map
import scala.collection.mutable.ArrayBuffer
import scala.io.Codec
import scala.reflect.ClassTag

import com.google.common.io.ByteStreams

import org.apache.spark.{Partition, SparkEnv, TaskContext}
import org.apache.spark.util.Utils

/**
 * An RDD that pipes the contents of each parent partition through an
 * external command and returns the output.
 */
private[spark] class PipedRDD[I: ClassTag, O: ClassTag](
    prev: RDD[I],
    command: Seq[String],
    envVars: Map[String, String],
    separateWorkingDir: Boolean,
    bufferSize: Int,
    inputWriter: InputWriter[I],
    outputReader: OutputReader[O]
) extends RDD[O](prev) {

  override def getPartitions: Array[Partition] = firstParent[I].partitions

  /**
   * A FilenameFilter that accepts anything that isn't equal to the name passed in.
   * @param filterName of file or directory to leave out
   */
  class NotEqualsFileNameFilter(filterName: String) extends FilenameFilter {
    def accept(dir: File, name: String): Boolean = {
      !name.equals(filterName)
    }
  }

  override def compute(split: Partition, context: TaskContext): Iterator[O] = {
    val pb = new ProcessBuilder(command.asJava)
    // Add the environmental variables to the process.
    val currentEnvVars = pb.environment()
    envVars.foreach { case (variable, value) => currentEnvVars.put(variable, value) }

    // for compatibility with Hadoop which sets these env variables
    // so the user code can access the input filename
    if (split.isInstanceOf[HadoopPartition]) {
      val hadoopSplit = split.asInstanceOf[HadoopPartition]
      currentEnvVars.putAll(hadoopSplit.getPipeEnvVars().asJava)
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
        case e: Exception => logError("Unable to setup task working directory: " + e.getMessage +
          " (" + taskDirectory + ")", e)
      }
    }

    val proc = pb.start()
    val env = SparkEnv.get
    val childThreadException = new AtomicReference[Throwable](null)

    // Start a thread to print the process's stderr to ours
    new Thread(s"stderr reader for $command") {
      override def run(): Unit = {
        val os = System.err
        try {
          ByteStreams.copy(proc.getErrorStream, os)
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          os.close()
        }
      }
    }.start()

    // Start a thread to feed the process input from our parent's iterator
    new Thread(s"stdin writer for $command") {
      override def run(): Unit = {
        TaskContext.setTaskContext(context)
        val dos = new DataOutputStream(
          new BufferedOutputStream(proc.getOutputStream, bufferSize))
        try {
          for (elem <- firstParent[I].iterator(split, context)) {
            inputWriter.write(dos, elem)
          }
        } catch {
          case t: Throwable => childThreadException.set(t)
        } finally {
          dos.close()
        }
      }
    }.start()

    val dis = new DataInputStream(
      new BufferedInputStream(proc.getInputStream, bufferSize))
    new Iterator[O] {
      def next(): O = {
        if (!hasNext()) {
          throw new NoSuchElementException()
        }

        outputReader.read(dis)
      }

      def hasNext(): Boolean = {
        dis.mark(1)
        val eof = dis.read() < 0
        dis.reset()

        if (eof) {
          val exitStatus = proc.waitFor()
          cleanup()
          if (exitStatus != 0) {
            throw new IllegalStateException(s"Subprocess exited with status $exitStatus. " +
              s"Command ran: " + command.mkString(" "))
          }
        }

        propagateChildException()
        !eof
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
          logError("Caught exception while running pipe() operator. " +
              s"Command ran: $commandRan.", t)
          cleanup()
          proc.destroy()
          throw t
        }
      }
    }
  }
}

/** Specifies how to write the elements of the input [[RDD]] into the pipe. */
trait InputWriter[T] extends Serializable {
  def write(dos: DataOutput, elem: T): Unit
}

/** Specifies how to read the elements from the pipe into the output [[RDD]]. */
trait OutputReader[T] extends Serializable {
  /**
   * Reads the next element.
   *
   * The input is guaranteed to have at least one byte.
   */
  def read(dis: DataInput): T
}

class TextInputWriter[I](
    encoding: String = Codec.defaultCharsetCodec.name,
    printPipeContext: (String => Unit) => Unit = null,
    printRDDElement: (I, String => Unit) => Unit = null
) extends InputWriter[I] {

  private[this] val lineSeparator = System.lineSeparator().getBytes(encoding)
  private[this] var initialized = printPipeContext == null

  private def writeLine(dos: DataOutput, s: String): Unit = {
    dos.write(s.getBytes(encoding))
    dos.write(lineSeparator)
  }

  override def write(dos: DataOutput, elem: I): Unit = {
    if (!initialized) {
      printPipeContext(writeLine(dos, _))
      initialized = true
    }

    if (printRDDElement == null) {
      writeLine(dos, String.valueOf(elem))
    } else {
      printRDDElement(elem, writeLine(dos, _))
    }
  }
}

class TextOutputReader(
    encoding: String = Codec.defaultCharsetCodec.name
) extends OutputReader[String] {

  private[this] val lf = "\n".getBytes(encoding)
  private[this] val cr = "\r".getBytes(encoding)
  private[this] val crlf = cr ++ lf
  private[this] var buf = Array.ofDim[Byte](64)
  private[this] var used = 0

  @inline
  /** Checks that the suffix of [[buf]] matches [[other]]. */
  private def endsWith(other: Array[Byte]): Boolean = {
    var i = used - 1
    var j = other.length - 1
    (j <= i) && {
      while (j >= 0) {
        if (buf(i) != other(j)) {
          return false
        }
        i -= 1
        j -= 1
      }
      true
    }
  }

  override def read(dis: DataInput): String = {
    used = 0

    try {
      do {
        val ch = dis.readByte()
        if (buf.length <= used) {
          buf = util.Arrays.copyOf(buf, used + (used >>> 1))  // 1.5x
        }

        buf(used) = ch
        used += 1
      } while (!(endsWith(lf) || endsWith(cr)))

      if (endsWith(crlf)) {
        used -= crlf.length
      } else {  // endsWith(lf) || endsWith(cr)
        used -= lf.length
      }
    } catch {
      case _: EOFException =>
    }

    new String(buf, 0, used, encoding)
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
    buf
  }
}
