package spark.deploy.worker

import spark.util.IntParam
import spark.util.MemoryParam
import spark.Utils
import java.lang.management.ManagementFactory

/**
 * Command-line parser for the master.
 */
private[spark] class WorkerArguments(args: Array[String]) {
  var ip = Utils.localIpAddress()
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
      ip = value
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
      "  -d DIR, --work-dir DIR   Directory to run jobs in (default: SPARK_HOME/work)\n" +
      "  -i IP, --ip IP           IP address or DNS name to listen on\n" +
      "  -p PORT, --port PORT     Port to listen on (default: random)\n" +
      "  --webui-port PORT        Port for web UI (default: 8081)")
    System.exit(exitCode)
  }

  def inferDefaultCores(): Int = {
    Runtime.getRuntime.availableProcessors()
  }

  def inferDefaultMemory(): Int = {
    val bean = ManagementFactory.getOperatingSystemMXBean
                                .asInstanceOf[com.sun.management.OperatingSystemMXBean]
    val totalMb = (bean.getTotalPhysicalMemorySize / 1024 / 1024).toInt
    // Leave out 1 GB for the operating system, but don't return a negative memory size
    math.max(totalMb - 1024, 512)
  }
}