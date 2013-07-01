package spark.api.python

import java.io.{DataInputStream, IOException}
import java.net.{Socket, SocketException, InetAddress}

import scala.collection.JavaConversions._

import spark._

private[spark] class PythonWorkerFactory(pythonExec: String, envVars: Map[String, String])
    extends Logging {
  var daemon: Process = null
  val daemonHost = InetAddress.getByAddress(Array(127, 0, 0, 1))
  var daemonPort: Int = 0

  def create(): Socket = {
    synchronized {
      // Start the daemon if it hasn't been started
      startDaemon()

      // Attempt to connect, restart and retry once if it fails
      try {
        new Socket(daemonHost, daemonPort)
      } catch {
        case exc: SocketException => {
          logWarning("Python daemon unexpectedly quit, attempting to restart")
          stopDaemon()
          startDaemon()
          new Socket(daemonHost, daemonPort)
        }
        case e => throw e
      }
    }
  }

  def stop() {
    stopDaemon()
  }

  private def startDaemon() {
    synchronized {
      // Is it already running?
      if (daemon != null) {
        return
      }

      try {
        // Create and start the daemon
        val sparkHome = new ProcessBuilder().environment().get("SPARK_HOME")
        val pb = new ProcessBuilder(Seq(pythonExec, sparkHome + "/python/pyspark/daemon.py"))
        val workerEnv = pb.environment()
        workerEnv.putAll(envVars)
        daemon = pb.start()

        // Redirect the stderr to ours
        new Thread("stderr reader for " + pythonExec) {
          override def run() {
            scala.util.control.Exception.ignoring(classOf[IOException]) {
              // FIXME HACK: We copy the stream on the level of bytes to
              // attempt to dodge encoding problems.
              val in = daemon.getErrorStream
              var buf = new Array[Byte](1024)
              var len = in.read(buf)
              while (len != -1) {
                System.err.write(buf, 0, len)
                len = in.read(buf)
              }
            }
          }
        }.start()

        val in = new DataInputStream(daemon.getInputStream)
        daemonPort = in.readInt()

        // Redirect further stdout output to our stderr
        new Thread("stdout reader for " + pythonExec) {
          override def run() {
            scala.util.control.Exception.ignoring(classOf[IOException]) {
              // FIXME HACK: We copy the stream on the level of bytes to
              // attempt to dodge encoding problems.
              var buf = new Array[Byte](1024)
              var len = in.read(buf)
              while (len != -1) {
                System.err.write(buf, 0, len)
                len = in.read(buf)
              }
            }
          }
        }.start()
      } catch {
        case e => {
          stopDaemon()
          throw e
        }
      }

      // Important: don't close daemon's stdin (daemon.getOutputStream) so it can correctly
      // detect our disappearance.
    }
  }

  private def stopDaemon() {
    synchronized {
      // Request shutdown of existing daemon by sending SIGTERM
      if (daemon != null) {
        daemon.destroy()
      }

      daemon = null
      daemonPort = 0
    }
  }
}
