package edu.berkeley.cs.amplab.sparkr

import java.io._
import java.net.URI
import java.util.concurrent.Semaphore
import java.util.concurrent.TimeUnit

import scala.collection.mutable.ArrayBuffer
import scala.collection.JavaConversions._

import org.apache.hadoop.fs.Path

/**
 * Main class used to launch SparkR applications using spark-submit. It executes R as a
 * subprocess and then has it connect back to the JVM to access system properties etc.
 */
object SparkRRunner {
  def main(args: Array[String]) {
    val rFile = args(0)

    val otherArgs = args.slice(1, args.length)

    // Time to wait for SparkR backend to initialize in seconds
    val backendTimeout = sys.env.getOrElse("SPARKR_BACKEND_TIMEOUT", "120").toInt
    val rCommand = "Rscript"

    // Check if the file path exists.
    // If not, change directory to current working directory for YARN cluster mode
    val rF = new File(rFile)
    val rFileNormalized = if (!rF.exists()) {
      new Path(rFile).getName
    } else {
      rFile
    }


    // Launch a SparkR backend server for the R process to connect to; this will let it see our
    // Java system properties etc.
    val sparkRBackend = new SparkRBackend()
    @volatile var sparkRBackendPort = 0
    val initialized = new Semaphore(0)
    val sparkRBackendThread = new Thread("SparkR backend") {
      override def run() {
        sparkRBackendPort = sparkRBackend.init()
        initialized.release()
        sparkRBackend.run()
      }
    }

    sparkRBackendThread.start()
    // Wait for SparkRBackend initialization to finish
    if (initialized.tryAcquire(backendTimeout, TimeUnit.SECONDS)) {
      // Launch R
      val returnCode = try {
        val builder = new ProcessBuilder(Seq(rCommand, rFileNormalized) ++ otherArgs)
        val env = builder.environment()
        env.put("EXISTING_SPARKR_BACKEND_PORT", sparkRBackendPort.toString)
        builder.redirectErrorStream(true) // Ugly but needed for stdout and stderr to synchronize
        val process = builder.start()

        new RedirectThread(process.getInputStream, System.out, "redirect output").start()

        process.waitFor()
      } finally {
        sparkRBackend.close()
      }
      System.exit(returnCode)
    } else {
      System.err.println("SparkR backend did not initialize in " + backendTimeout + " seconds")
      System.exit(-1)
    }
  }

  private class RedirectThread(
      in: InputStream,
      out: OutputStream,
      name: String,
      propagateEof: Boolean = false)
    extends Thread(name) {

    setDaemon(true)
    override def run() {
      // FIXME: We copy the stream on the level of bytes to avoid encoding problems.
      try {
        val buf = new Array[Byte](1024)
        var len = in.read(buf)
        while (len != -1) {
          out.write(buf, 0, len)
          out.flush()
          len = in.read(buf)
        }
      } finally {
        if (propagateEof) {
          out.close()
        }
      }
    }
  }
}
