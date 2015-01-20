package org.apache.spark.scheduler.mesos

import java.nio.ByteBuffer

import org.apache.spark.scheduler.cluster.mesos.MesosTaskLaunchData
import org.scalatest.FunSuite

class MesosTaskLaunchDataSuite extends FunSuite {
  test("serialize and deserialize data must be same") {
    val serializedTask = ByteBuffer.allocate(40)
    (Range(100, 110).map(serializedTask.putInt(_)))
    serializedTask.rewind
    val attemptNumber = 100
    val byteString = MesosTaskLaunchData(serializedTask, attemptNumber).toByteString
    serializedTask.rewind
    val mesosTaskLaunchData = MesosTaskLaunchData.fromByteString(byteString)
    assert(mesosTaskLaunchData.attemptNumber == attemptNumber)
    assert(mesosTaskLaunchData.serializedTask.equals(serializedTask))
  }
}
