package spark

import java.util.concurrent.ThreadFactory

/**
 * A ThreadFactory that creates daemon threads
 */
private object DaemonThreadFactory extends ThreadFactory {
  override def newThread(r: Runnable): Thread = new DaemonThread(r)
}

private class DaemonThread(r: Runnable = null) extends Thread {
  override def run() {
    if (r != null) {
      r.run()
    }
  }
}