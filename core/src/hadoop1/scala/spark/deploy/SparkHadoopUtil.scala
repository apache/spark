package spark.deploy

/**
 * Contains util methods to interact with Hadoop from spark.
 */
object SparkHadoopUtil {

  def getUserNameFromEnvironment(): String = {
    // defaulting to -D ...
    System.getProperty("user.name")
  }

  def runAsUser(func: (Product) => Unit, args: Product) {

    // Add support, if exists - for now, simply run func !
    func(args)
  }
}
