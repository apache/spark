package spark.ui

import scala.util.Random

import spark.SparkContext
import spark.SparkContext._

/**
 * Continuously generates jobs that expose various features of the WebUI (internal testing tool).
 *
 * Usage: ./run spark.ui.UIWorkloadGenerator [master]
 */
private[spark] object UIWorkloadGenerator {
  val NUM_PARTITIONS = 100
  val INTER_JOB_WAIT_MS = 500

  def main(args: Array[String]) {
    val master = args(0)
    val appName = "Spark UI Tester"
    val sc = new SparkContext(master, appName)

    // NOTE: Right now there is no easy way for us to show spark.job.annotation for a given phase,
    //       but we pass it here anyways since it will be useful once we do.
    def setName(s: String) = {
      sc.addLocalProperties("spark.job.annotation", s)
    }
    val baseData = sc.makeRDD(1 to NUM_PARTITIONS * 10, NUM_PARTITIONS)
    def nextFloat() = (new Random()).nextFloat()

    val jobs = Seq[(String, () => Long)](
      ("Count", baseData.count),
      ("Cache and Count", baseData.map(x => x).cache.count),
      ("Single Shuffle", baseData.map(x => (x % 10, x)).reduceByKey(_ + _).count),
      ("Entirely failed phase", baseData.map(x => throw new Exception).count),
      ("Partially failed phase", {
        baseData.map{x =>
          val probFailure = (4.0 / NUM_PARTITIONS)
          if (nextFloat() < probFailure) {
            throw new Exception("This is a task failure")
          }
          1
        }.count
      }),
      ("Job with delays", baseData.map(x => Thread.sleep(1000)).count)
    )

    while (true) {
      for ((desc, job) <- jobs) {
        try {
          setName(desc)
          job()
          println("Job funished: " + desc)
        } catch {
          case e: Exception =>
            println("Job Failed: " + desc)
        }
        Thread.sleep(INTER_JOB_WAIT_MS)
      }
    }
  }
}
