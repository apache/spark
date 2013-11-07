package org.apache.spark.deploy.worker

import org.scalatest.FunSuite
import org.apache.spark.deploy.{ExecutorState, Command, ApplicationDescription}
import java.io.File

class ExecutorRunnerTest extends FunSuite {

  test("command includes appId") {
    def f(s:String) = new File(s)
    val sparkHome = sys.props("user.dir")
    val appDesc = new ApplicationDescription("app name", 8, 500, Command("foo", Seq(),Map()), sparkHome, "appUiUrl")
    val appId = "12345-worker321-9876"
    val er = new ExecutorRunner(appId, 1, appDesc, 8, 500, null, "blah", "worker321", f(sparkHome), f("ooga"), ExecutorState.RUNNING)

    assert(er.buildCommandSeq().last === appId)
  }
}
