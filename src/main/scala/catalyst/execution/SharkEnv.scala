package catalyst.execution

import scala.collection.mutable.{HashMap, HashSet}

import org.apache.spark.SparkContext
import org.apache.spark.scheduler.StatsReportListener


/** A singleton object for the master program. The slaves should not access this. */
object SharkEnv {

  def initWithSharkContext(jobName: String, master: String): SharkContext = {
    if (sc != null) {
      sc.stop()
    }

    sc = new SharkContext(
      if (master == null) "local" else master,
      jobName,
      System.getenv("SPARK_HOME"),
      Nil,
      executorEnvVars)
    sc.addSparkListener(new StatsReportListener())
    sc.asInstanceOf[SharkContext]
  }

  val executorEnvVars = new HashMap[String, String]
  executorEnvVars.put("SCALA_HOME", getEnv("SCALA_HOME"))
  executorEnvVars.put("SPARK_MEM", getEnv("SPARK_MEM"))
  executorEnvVars.put("SPARK_CLASSPATH", getEnv("SPARK_CLASSPATH"))
  executorEnvVars.put("HADOOP_HOME", getEnv("HADOOP_HOME"))
  executorEnvVars.put("JAVA_HOME", getEnv("JAVA_HOME"))
  executorEnvVars.put("MESOS_NATIVE_LIBRARY", getEnv("MESOS_NATIVE_LIBRARY"))
  executorEnvVars.put("TACHYON_MASTER", getEnv("TACHYON_MASTER"))
  executorEnvVars.put("TACHYON_WAREHOUSE_PATH", getEnv("TACHYON_WAREHOUSE_PATH"))

  val activeSessions = new HashSet[String]

  var sc: SparkContext = _

  /** Return the value of an environmental variable as a string. */
  def getEnv(varname: String) = if (System.getenv(varname) == null) "" else System.getenv(varname)
}
