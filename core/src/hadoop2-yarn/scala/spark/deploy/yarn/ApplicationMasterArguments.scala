package spark.deploy.yarn

import spark.util.IntParam
import collection.mutable.ArrayBuffer

class ApplicationMasterArguments(val args: Array[String]) {
  var userJar: String = null
  var userClass: String = null
  var userArgs: Seq[String] = Seq[String]()
  var workerMemory = 1024
  var workerCores = 1
  var numWorkers = 2

  parseArgs(args.toList)
  
  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer = new ArrayBuffer[String]()

    var args = inputArgs

    while (! args.isEmpty) {

      args match {
        case ("--jar") :: value :: tail =>
          userJar = value
          args = tail

        case ("--class") :: value :: tail =>
          userClass = value
          args = tail

        case ("--args") :: value :: tail =>
          userArgsBuffer += value
          args = tail

        case ("--num-workers") :: IntParam(value) :: tail =>
          numWorkers = value
          args = tail

        case ("--worker-memory") :: IntParam(value) :: tail =>
          workerMemory = value
          args = tail

        case ("--worker-cores") :: IntParam(value) :: tail =>
          workerCores = value
          args = tail

        case Nil =>
          if (userJar == null || userClass == null) {
            printUsageAndExit(1)
          }

        case _ =>
          printUsageAndExit(1, args)
      }
    }

    userArgs = userArgsBuffer.readOnly
  }
  
  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println(
      "Usage: spark.deploy.yarn.ApplicationMaster [options] \n" +
      "Options:\n" +
      "  --jar JAR_PATH       Path to your application's JAR file (required)\n" +
      "  --class CLASS_NAME   Name of your application's main class (required)\n" +
      "  --args ARGS          Arguments to be passed to your application's main class.\n" +
      "                       Mutliple invocations are possible, each will be passed in order.\n" +
      "                       Note that first argument will ALWAYS be yarn-standalone : will be added if missing.\n" +
      "  --num-workers NUM    Number of workers to start (Default: 2)\n" +
      "  --worker-cores NUM   Number of cores for the workers (Default: 1)\n" +
      "  --worker-memory MEM  Memory per Worker (e.g. 1000M, 2G) (Default: 1G)\n")
    System.exit(exitCode)
  }
}
