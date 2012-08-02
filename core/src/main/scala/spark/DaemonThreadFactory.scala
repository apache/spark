package spark

import java.util.concurrent.ThreadFactory

/**
 * A ThreadFactory that creates daemon threads
 */
private object DaemonThreadFactory extends ThreadFactory {
  override def newThread(r: Runnable): Thread = {
    val t = new Thread(r)
    t.setDaemon(true)
    return t
  }
}