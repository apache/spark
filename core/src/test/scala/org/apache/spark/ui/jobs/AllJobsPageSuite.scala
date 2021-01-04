package org.apache.spark.ui.jobs

import javax.servlet.http.HttpServletRequest
import org.apache.spark.SparkFunSuite
import org.apache.spark.status.AppStatusStore
import org.apache.spark.status.api.v1
import org.mockito.Mockito.{mock, when}

class AllJobsPageSuite extends SparkFunSuite {

  val appStatusStore = mock(classOf[AppStatusStore])
  val request = mock(classOf[HttpServletRequest])
  val appEnv = mock(classOf[v1.ApplicationEnvironmentInfo])
  val sparkProperties: Seq[(String, String)] = Seq(("spark.scheduler.mode", "fair"))
  when(appStatusStore.environmentInfo()).thenReturn(appEnv)
  when(appStatusStore.environmentInfo().sparkProperties).thenReturn(sparkProperties)

  private val jobPage = new AllJobsPage(null, appStatusStore)


  test("Enumeration conversion") {
    assert(jobPage.getSchedulingMode.equals("FAIR"))
  }
}
