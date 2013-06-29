package spark

import org.scalatest.Suite
import org.scalatest.BeforeAndAfterAll

/** Shares a local `SparkContext` between all tests in a suite and closes it at the end */
trait SharedSparkContext extends BeforeAndAfterAll { self: Suite =>

  @transient private var _sc: SparkContext = _

  def sc: SparkContext = _sc

  override def beforeAll() {
    _sc = new SparkContext("local", "test")
    super.beforeAll()
  }

  override def afterAll() {
    if (_sc != null) {
      LocalSparkContext.stop(_sc)
      _sc = null
    }
    super.afterAll()
  }
}
