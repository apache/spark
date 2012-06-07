package spark

import org.scalatest.FunSuite

import spark.scheduler.mesos.MesosScheduler

class MesosSchedulerSuite extends FunSuite {
  test("memoryStringToMb"){

    assert(MesosScheduler.memoryStringToMb("1") == 0)
    assert(MesosScheduler.memoryStringToMb("1048575") == 0)
    assert(MesosScheduler.memoryStringToMb("3145728") == 3)

    assert(MesosScheduler.memoryStringToMb("1024k") == 1)
    assert(MesosScheduler.memoryStringToMb("5000k") == 4)
    assert(MesosScheduler.memoryStringToMb("4024k") == MesosScheduler.memoryStringToMb("4024K"))

    assert(MesosScheduler.memoryStringToMb("1024m") == 1024)
    assert(MesosScheduler.memoryStringToMb("5000m") == 5000)
    assert(MesosScheduler.memoryStringToMb("4024m") == MesosScheduler.memoryStringToMb("4024M"))

    assert(MesosScheduler.memoryStringToMb("2g") == 2048)
    assert(MesosScheduler.memoryStringToMb("3g") == MesosScheduler.memoryStringToMb("3G"))

    assert(MesosScheduler.memoryStringToMb("2t") == 2097152)
    assert(MesosScheduler.memoryStringToMb("3t") == MesosScheduler.memoryStringToMb("3T"))


  }
}
