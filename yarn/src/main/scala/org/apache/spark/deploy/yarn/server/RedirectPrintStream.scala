package org.apache.spark.deploy.yarn.server

import java.io.{OutputStream, PrintStream}

class RedirectPrintStream(logger: ChannelMessageLogger, out: OutputStream) extends PrintStream(out) {

  override def println(x: String): Unit = {
    super.println(x)
    logger.logInfo(x)
  }

  override def print(x: String): Unit = {
    super.print(x)
    logger.logInfo(x)
  }

}
