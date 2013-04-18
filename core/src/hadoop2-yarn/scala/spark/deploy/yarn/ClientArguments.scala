package spark.deploy.yarn

import spark.util.MemoryParam
import spark.util.IntParam
import collection.mutable.{ArrayBuffer, HashMap}
import spark.scheduler.{InputFormatInfo, SplitInfo}

// TODO: Add code and support for ensuring that yarn resource 'asks' are location aware !
class ClientArguments(val args: Array[String]) {
  var userJar: String = null
  var userClass: String = null
  var userArgs: Seq[String] = Seq[String]()
  var workerMemory = 1024
  var workerCores = 1
  var numWorkers = 2
  var amUser = System.getProperty("user.name")
  var amQueue = System.getProperty("QUEUE", "default")
  var amMemory: Int = 512
  // TODO
  var inputFormatInfo: List[InputFormatInfo] = null

  parseArgs(args.toList)

  private def parseArgs(inputArgs: List[String]): Unit = {
    val userArgsBuffer: ArrayBuffer[String] = new ArrayBuffer[String]()
    val inputFormatMap: HashMap[String, InputFormatInfo] = new HashMap[String, InputFormatInfo]()

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

        case ("--master-memory") :: MemoryParam(value) :: tail =>
          amMemory = value
          args = tail

        case ("--num-workers") :: IntParam(value) :: tail =>
          numWorkers = value
          args = tail

        case ("--worker-memory") :: MemoryParam(value) :: tail =>
          workerMemory = value
          args = tail

        case ("--worker-cores") :: IntParam(value) :: tail =>
          workerCores = value
          args = tail

        case ("--user") :: value :: tail =>
          amUser = value
          args = tail

        case ("--queue") :: value :: tail =>
          amQueue = value
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
    inputFormatInfo = inputFormatMap.values.toList
  }

  
  def printUsageAndExit(exitCode: Int, unknownParam: Any = null) {
    if (unknownParam != null) {
      System.err.println("Unknown/unsupported param " + unknownParam)
    }
    System.err.println(
      "Usage: spark.deploy.yarn.Client [options] \n" +
      "Options:\n" +
      "  --jar JAR_PATH       Path to your application's JAR file (required)\n" +
      "  --class CLASS_NAME   Name of your application's main class (required)\n" +
      "  --args ARGS          Arguments to be passed to your application's main class.\n" +
      "                       Mutliple invocations are possible, each will be passed in order.\n" +
      "                       Note that first argument will ALWAYS be yarn-standalone : will be added if missing.\n" +
      "  --num-workers NUM    Number of workers to start (Default: 2)\n" +
      "  --worker-cores NUM   Number of cores for the workers (Default: 1). This is unsused right now.\n" +
      "  --master-memory MEM  Memory for Master (e.g. 1000M, 2G) (Default: 512 Mb)\n" +
      "  --worker-memory MEM  Memory per Worker (e.g. 1000M, 2G) (Default: 1G)\n" +
      "  --queue QUEUE        The hadoop queue to use for allocation requests (Default: 'default')\n" +
      "  --user USERNAME      Run the ApplicationMaster (and slaves) as a different user\n"
      )
    System.exit(exitCode)
  }
  
}
