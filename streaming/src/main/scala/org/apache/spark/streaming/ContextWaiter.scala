package org.apache.spark.streaming

private[streaming] class ContextWaiter {
  private var error: Throwable = null
  private var stopped: Boolean = false

  def notifyError(e: Throwable) = synchronized {
    error = e
    notifyAll()
  }

  def notifyStop() = synchronized {
    notifyAll()
  }

  def waitForStopOrError(timeout: Long = -1) = synchronized {
    // If already had error, then throw it
    if (error != null) {
      throw error
    }

    // If not already stopped, then wait
    if (!stopped) {
      if (timeout < 0) wait() else wait(timeout)
      if (error != null) throw error
    }
  }
}
