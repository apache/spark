package spark

import java.io.File

import org.scalatest.FunSuite
import org.scalatest.concurrent.Timeouts
import org.scalatest.prop.TableDrivenPropertyChecks._
import org.scalatest.time.SpanSugar._

class DriverSuite extends FunSuite with Timeouts {
  test("driver should exit after finishing") {
    assert(System.getenv("SPARK_HOME") != null)
    // Regression test for SPARK-530: "Spark driver process doesn't exit after finishing"
    val masters = Table(("master"), ("local"), ("local-cluster[2,1,512]"))
    forAll(masters) { (master: String) =>
      failAfter(30 seconds) {
        Utils.execute(Seq("./run", "spark.DriverWithoutCleanup", master),
          new File(System.getenv("SPARK_HOME")))
      }
    }
  }
}

/**
 * Program that creates a Spark driver but doesn't call SparkContext.stop() or
 * Sys.exit() after finishing.
 */
object DriverWithoutCleanup {
  def main(args: Array[String]) {
    val sc = new SparkContext(args(0), "DriverWithoutCleanup")
    sc.parallelize(1 to 100, 4).count()
  }
}
