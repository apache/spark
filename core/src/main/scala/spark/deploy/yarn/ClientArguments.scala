package spark.deploy.yarn

import spark.util.MemoryParam
import spark.util.IntParam

class ClientArguments(val args: Array[String]) {
  var userJar : String = null
  var userClass : String = null
  var userArgs = ""
  var workerMemory = 1024
  var numWorkers = 2
  var amUser = System.getProperty("user.name")
  var amMemory = 512
  
  
  parse(args.toList)
  
  def parse(args: List[String]) : Unit = args match {
    case ("--jar") :: value :: tail =>
      userJar = value
      parse(tail)
    
    case ("--class") :: value :: tail =>
      userClass = value
      parse(tail)
      
    case ("--args") :: value :: tail =>
      userArgs = value
      parse(tail)
      
    case ("--num-workers") :: IntParam(value) :: tail =>
      numWorkers = value
      parse(tail)
    
    case ("--worker-memory") :: MemoryParam(value) :: tail =>
      workerMemory = value
      parse(tail)
      
    case ("--user") :: value :: tail =>
      amUser = value
      parse(tail)
    
    case Nil =>
      if (userJar == null || userClass == null) {
        printUsageAndExit(1)
      }
      
    case _ =>
      printUsageAndExit(1)
  }
  
  def printUsageAndExit(exitCode: Int) {
    System.err.println(
      "Usage: spark.deploy.yarn.Client [options] \n" +
      "Options:\n" +
      "  --jar JAR_PATH       Path to your application's JAR file (required)\n" +
      "  --class CLASS_NAME   Name of your application's main class (required)\n" +
      "  --args ARGS          Arguments to be passed to your application's main class\n" + 
      "  --num-workers NUM    Number of workers to start (Default: 2)\n" +
      "  --worker-memory MEM  Memory per Worker (e.g. 1000M, 2G) (Default: 1G)\n" +
      "  --user USERNAME      Run the ApplicationMaster as a different user\n"
      )
    System.exit(exitCode)
  }
  
}
