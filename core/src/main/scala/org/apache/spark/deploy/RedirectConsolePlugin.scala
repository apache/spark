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

package org.apache.spark.deploy

import java.io.{ByteArrayOutputStream, PrintStream}
import java.util.{Collections, Map => JMap}

import org.apache.spark.SparkContext
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.internal.{Logging, SparkLoggerFactory}
import org.apache.spark.internal.config._

/**
 * A built-in plugin to allow redirecting stdout/stderr to logging system (SLF4J).
 */
class RedirectConsolePlugin extends SparkPlugin {
  override def driverPlugin(): DriverPlugin = new DriverRedirectConsolePlugin()

  override def executorPlugin(): ExecutorPlugin = new ExecRedirectConsolePlugin()
}

object RedirectConsolePlugin {

  def redirectStdoutToLog(): Unit = {
    val stdoutLogger = SparkLoggerFactory.getLogger("stdout")
    System.setOut(new LoggingPrintStream(stdoutLogger.info))
  }

  def redirectStderrToLog(): Unit = {
    val stderrLogger = SparkLoggerFactory.getLogger("stderr")
    System.setErr(new LoggingPrintStream(stderrLogger.error))
  }
}

class DriverRedirectConsolePlugin extends DriverPlugin with Logging {

  override def init(sc: SparkContext, ctx: PluginContext): JMap[String, String] = {
    val outputs = sc.conf.get(DRIVER_REDIRECT_CONSOLE_OUTPUTS)
    if (outputs.contains("stdout")) {
      logInfo("Redirect driver's stdout to logging system.")
      RedirectConsolePlugin.redirectStdoutToLog()
    }
    if (outputs.contains("stderr")) {
      logInfo("Redirect driver's stderr to logging system.")
      RedirectConsolePlugin.redirectStderrToLog()
    }
    Collections.emptyMap
  }
}

class ExecRedirectConsolePlugin extends ExecutorPlugin with Logging {

  override def init(ctx: PluginContext, extraConf: JMap[String, String]): Unit = {
    val outputs = ctx.conf.get(EXEC_REDIRECT_CONSOLE_OUTPUTS)
    if (outputs.contains("stdout")) {
      logInfo("Redirect executor's stdout to logging system.")
      RedirectConsolePlugin.redirectStdoutToLog()
    }
    if (outputs.contains("stderr")) {
      logInfo("Redirect executor's stderr to logging system.")
      RedirectConsolePlugin.redirectStderrToLog()
    }
  }
}

private[spark] class LoggingPrintStream(redirect: String => Unit)
  extends PrintStream(new LineBuffer(4 * 1024 * 1024)) {

  override def write(b: Int): Unit = {
    super.write(b)
    tryLogCurrentLine()
  }

  override def write(buf: Array[Byte], off: Int, len: Int): Unit = {
    super.write(buf, off, len)
    tryLogCurrentLine()
  }

  private def tryLogCurrentLine(): Unit = this.synchronized {
    out.asInstanceOf[LineBuffer].tryGenerateContext.foreach { logContext =>
      redirect(logContext)
    }
  }
}

/**
 * Cache bytes before line ending. When current line is ended or the bytes size reaches the
 * threshold, it can generate the line.
 */
private[spark] object LineBuffer {
  private val LF_BYTES = System.lineSeparator.getBytes
  private val LF_LENGTH = LF_BYTES.length
}

private[spark] class LineBuffer(lineMaxBytes: Long) extends ByteArrayOutputStream {

  import LineBuffer._

  def tryGenerateContext: Option[String] =
    if (isLineEnded) {
      try Some(new String(buf, 0, count - LF_LENGTH)) finally reset()
    } else if (count >= lineMaxBytes) {
      try Some(new String(buf, 0, count)) finally reset()
    } else {
      None
    }

  private def isLineEnded: Boolean = {
    if (count < LF_LENGTH) return false
    // fast return in UNIX-like OS when LF is single char '\n'
    if (LF_LENGTH == 1) return LF_BYTES(0) == buf(count - 1)

    var i = 0
    do {
      if (LF_BYTES(i) != buf(count - LF_LENGTH + i)) {
        return false
      }
      i = i + 1
    } while (i < LF_LENGTH)
    true
  }
}
