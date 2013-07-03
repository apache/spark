package spark.deploy
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.mapred.JobConf


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

  // Return an appropriate (subclass) of Configuration. Creating config can initializes some hadoop subsystems
  def newConfiguration(): Configuration = new Configuration()

  // add any user credentials to the job conf which are necessary for running on a secure Hadoop cluster
  def addCredentials(conf: JobConf) {}
}
