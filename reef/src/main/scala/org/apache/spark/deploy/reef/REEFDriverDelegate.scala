package org.apache.spark.deploy.reef

import java.io.File
import java.net.URL
import java.util.concurrent.atomic.AtomicReference

import org.apache.spark.executor.ExecutorURLClassLoader
import org.apache.spark.scheduler.cluster.CoarseGrainedSchedulerBackend
import org.apache.spark.util.Utils
import org.apache.spark.{Logging, SparkConf, SparkContext}

import scala.beans.BeanProperty

/**
 * Spark-REEF API
 * REEFDriverDelegate halts Spark task scheduler
 * until SparkContext is initialized
 */
object REEFDriverDelegate extends Logging {

  val sparkContextRef: AtomicReference[SparkContext] = new AtomicReference[SparkContext](null)
  @BeanProperty var driverURL: String = null
  var sparkConf: SparkConf = null
  // Variable used to notify the REEFClusterScheduler that it should stop waiting
  // for the initial set of executors to be started and get on with its business.
  val doneWithREEFEvaluatorInitMonitor = new Object() // lock for REEF Evaluator
  val doneWithSparkContextInitMonitor = new Object() //lock for SparkContext
  @volatile var isDoneWithSparkContextInit = false
  @volatile var isDoneWithREEFEvaluatorInit = false
  var isFinished: Boolean = false

  def startUserClass(userClass: String, userArgs: Array[String]): Thread = {
    logInfo("Starting the user class in a separate Thread")

    val loader = new ExecutorURLClassLoader(new Array[URL](0),
      Thread.currentThread.getContextClassLoader)
    Thread.currentThread.setContextClassLoader(loader)

    val jars = System.getProperty("spark.jars").split(",")
    for (jar <- jars) {
      addJarToClasspath(jar, loader)
    }

    val mainClass = Class.forName(userClass, true, loader)
    val mainMethod = mainClass.getMethod("main", classOf[Array[String]])

    val t = new Thread {
      override def run() {
        var succeeded = false
        try {
          mainMethod.invoke(null, userArgs)
          // Some apps have "System.exit(0)" at the end.  The user thread will stop here unless
          // it has an uncaught exception thrown out.  It needs a shutdown hook to set SUCCEEDED.
          succeeded = true
        } finally {
          logDebug("Finishing main")
          if (succeeded) {
            REEFDriverDelegate.this.finishApplication(true)
          } else {
            REEFDriverDelegate.this.finishApplication(false)
          }
        }
      }
    }
    t.setName("User Class Main")
    t.start()
    t
  }

  private def addJarToClasspath(localJar: String, loader: ExecutorURLClassLoader) {
    val uri = Utils.resolveURI(localJar)
    uri.getScheme match {
      case "file" | "local" =>
        val file = new File(uri.getPath)
        if (file.exists()) {
          loader.addURL(file.toURI.toURL)
        } else {
          logWarning(s"Local jar $file does not exist, skipping.")
        }
      case _ =>
        logWarning(s"Skip remote jar $uri.")
    }
  }

  def finishApplication(status: Boolean) {
    synchronized {
      if (status) {
        return
      } else{
        logError("Could not finish User Class")
        return
      }
    }
  }

  // This a bit hacky, but we need to wait until the spark.driver.port property has
  // been set by the Thread executing the user class.
  def acquiredSparkContext() {
    logInfo("Waiting for Spark context initialization")
    try {
      var sparkContext: SparkContext = null
      REEFDriverDelegate.sparkContextRef.synchronized {
        var numTries = 0
        val waitTime = 10000L
        val maxNumTries = 10
        while (REEFDriverDelegate.sparkContextRef.get() == null && numTries < maxNumTries
          && !isFinished) {
          logInfo("Waiting for Spark context initialization ... " + numTries)
          numTries = numTries + 1
          REEFDriverDelegate.sparkContextRef.wait(waitTime)
        }
        sparkContext = REEFDriverDelegate.sparkContextRef.get()
        assert(sparkContext != null || numTries >= maxNumTries)

        if (sparkContext != null) {
          sparkConf = sparkContext.conf
          driverURL = "akka.tcp://spark@%s:%s/user/%s".format(
              sparkConf.get("spark.driver.host"),
              sparkConf.get("spark.driver.port"),
              CoarseGrainedSchedulerBackend.ACTOR_NAME)
        } else {
          logWarning("Unable to retrieve SparkContext in spite of waiting for %d, maxNumTries = %d".
            format(numTries * waitTime, maxNumTries))
          throw new RuntimeException("SparkContext not initialized")
        }
      }
    } finally {
      // notify REEFDriver
      REEFDriverDelegate.doneWithSparkContextInit()
    }
  }

  /**
   * REEFClusterScheduler release lock to REEFDriver to notify that
   * SparkContext is alive.
   */
  def doneWithSparkContextInit() {
    isDoneWithSparkContextInit = true
    doneWithSparkContextInitMonitor.synchronized {
      // to wake threads off wait ...
      doneWithSparkContextInitMonitor.notifyAll()
    }
  }

  /**
   * REEFDriver has to acquire lock before it
   * launches evaluators
   */
  def waitForSparkContextInit() {
    doneWithSparkContextInitMonitor.synchronized {
      while (!isDoneWithSparkContextInit) {
        doneWithSparkContextInitMonitor.wait(1000L)
      }
    }
  }

  /**
   * REEFDriver release lock to notify that
   * Evaluators are up
   */
  def doneWithREEFEvaluatorInit() {
    isDoneWithREEFEvaluatorInit = true
    doneWithREEFEvaluatorInitMonitor.synchronized {
      // to wake threads off wait ...
      doneWithREEFEvaluatorInitMonitor.notifyAll()
    }
  }

  /**
   * REEFClusterScheduler has to acquire lock before it
   * proceeds with user class
   */
  def waitForREEFEvaluatorInit() {
    doneWithREEFEvaluatorInitMonitor.synchronized {
      while (!isDoneWithREEFEvaluatorInit) {
        doneWithREEFEvaluatorInitMonitor.wait(1000L)
      }
    }
  }

  /**
   * Called from REEFClusterScheduler to notify that a SparkContext has been
   * initialized in the user code.
   */
  def sparkContextInitialized(sc: SparkContext): Boolean = {
    var modified = false
    sparkContextRef.synchronized {
      modified = sparkContextRef.compareAndSet(null, sc)
      sparkContextRef.notifyAll()
    }

    // Add a shutdown hook - as a best effort in case users do not call sc.stop or do
    // System.exit.
    // Should not really have to do this, but it helps REEF to evict resources earlier.
    // Not to mention, prevent the Client from declaring failure even though we exited properly.
    // Note that this will unfortunately not properly clean up the staging files because it gets
    // called too late, after the filesystem is already shutdown.
    if (modified) {
      Runtime.getRuntime().addShutdownHook(new Thread with Logging {
        // This is not only logs, but also ensures that log system is initialized for this instance
        // when we are actually 'run'-ing.
        logInfo("Adding shutdown hook for context " + sc)

        override def run() {
          logInfo("Invoking sc stop from shutdown hook")
          sc.stop()
        }
      })
    }

    // Wait for initialization to complete and at least 'some' nodes to get allocated.
    modified
  }

}