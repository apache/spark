package spark.deploy.worker

import spark.util.IntParam
import spark.util.MemoryParam
import spark.Utils
import java.lang.management.ManagementFactory

/**
 * Command-line parser for the master.
 */
private[spark] class WorkerArguments(args: Array[String]) {
  var host = Utils.localHostName()
  var port = 0
  var webUiPort = 8081
  var cores = inferDefaultCores()
  var memory = inferDefaultMemory()
  var master: String = null
  var workDir: String = null
  
  // Check for settings in environment variables 
  if (System.getenv("SPARK_WORKER_PORT") != null) {
    port = System.getenv("SPARK_WORKER_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_CORES") != null) {
    cores = System.getenv("SPARK_WORKER_CORES").toInt
  }
  if (System.getenv("SPARK_WORKER_MEMORY") != null) {
    memory = Utils.memoryStringToMb(System.getenv("SPARK_WORKER_MEMORY"))
  }
  if (System.getenv("SPARK_WORKER_WEBUI_PORT") != null) {
    webUiPort = System.getenv("SPARK_WORKER_WEBUI_PORT").toInt
  }
  if (System.getenv("SPARK_WORKER_DIR") != null) {
    workDir = System.getenv("SPARK_WORKER_DIR")
  }
  
  parse(args.toList)

  def parse(args: List[String]): Unit = args match {
    case ("--ip" | "-i") :: value :: tail =>
      Utils.checkHost(value, "ip no longer supported, please use hostname " + value)
      host = value
      parse(tail)

    case ("--host" | "-h") :: value :: tail =>
      Utils.checkHost(value, "Please use hostname " + value)
      host = value
      parse(tail)

    case ("--port" | "-p") :: IntParam(value) :: tail =>
      port = value
      parse(tail)

    case ("--cores" | "-c") :: IntParam(value) :: tail =>
      cores = value
      parse(tail)

    case ("--memory" | "-m") :: MemoryParam(value) :: tail =>
      memory = value
      parse(tail)

    case ("--work-dir" | "-d") :: value :: tail =>
      workDir = value
      parse(tail)
      
    case "--webui-port" :: IntParam(value) :: tail =>
      webUiPort = value
      parse(tail)

    case ("--help" | "-h") :: tail =>
      printUsageAndExit(0)

    case value :: tail =>
      if (master != null) {  // Two positional arguments were given
        printUsageAndExit(1)
      }
      master = value
      parse(tail)

    case Nil =>
      if (master == null) {  // No positional argument was given
        printUsageAndExit(1)
      }

    case _ =>
      printUsageAndExit(1)
  }

  /**
   * Print usage and exit JVM with the given exit code.
   */
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: Worker [options] <master>\n" +
      "\n" +
      "Master must be a URL of the form spark://hostname:port\n" +
      "\n" +
      "Options:\n" +
      "  -c CORES, --cores CORES  Number of cores to use\n" +
      "  -m MEM, --memory MEM     Amount of memory to use (e.g. 1000M, 2G)\n" +
      "  -d DIR, --work-dir DIR   Directory to run apps in (default: SPARK_HOME/work)\n" +
      "  -i HOST, --ip IP         Hostname to listen on (deprecated, please use --host or -h)\n" +
      "  -h HOST, --host HOST     Hostname to listen on\n" +
      "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
      "  --webui-port PORT        Port for web UI (default: 8081)")
    System.exit(exitCode)
  }

  def inferDefaultCores(): Int = {
    Runtime.getRuntime.availableProcessors()
  }

  def inferDefaultMemory(): Int = {
    val ibmVendor = System.getProperty("java.vendor").contains("IBM")
    var totalMb = 0
    try {
      val bean = ManagementFactory.getOperatingSystemMXBean()
      if (ibmVendor) {
        val beanClass = Class.forName("com.ibm.lang.management.OperatingSystemMXBean")
        val method = beanClass.getDeclaredMethod("getTotalPhysicalMemory")
        totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      } else {
        val beanClass = Class.forName("com.sun.management.OperatingSystemMXBean")
        val method = beanClass.getDeclaredMethod("getTotalPhysicalMemorySize")
        totalMb = (method.invoke(bean).asInstanceOf[Long] / 1024 / 1024).toInt
      }
    } catch {
      case e: Exception => {
        totalMb = 2*1024
        System.out.println("Failed to get total physical memory. Using " + totalMb + " MB")
      }
    }
    // Leave out 1 GB for the operating system, but don't return a negative memory size
    math.max(totalMb - 1024, 512)
  }
}
