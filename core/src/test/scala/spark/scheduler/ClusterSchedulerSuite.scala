package spark.scheduler

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import spark._
import spark.scheduler._
import spark.scheduler.cluster._

import java.util.Properties

class DummyTaskSetManager(
    initPriority: Int,
    initStageId: Int,
    initNumTasks: Int) 
  extends Schedulable {
  
  var parent: Schedulable = null
  var weight = 1
  var minShare = 2
  var runningTasks = 0
  var priority = initPriority
  var stageId = initStageId
  var name = "TaskSet_"+stageId
  var numTasks = initNumTasks
  var tasksFinished = 0

  def increaseRunningTasks(taskNum: Int) {
    runningTasks += taskNum
    if (parent != null) {
      parent.increaseRunningTasks(taskNum)
    }
  }

  def decreaseRunningTasks(taskNum: Int) {
    runningTasks -= taskNum
    if (parent != null) {
      parent.decreaseRunningTasks(taskNum)
    }
  }

  def addSchedulable(schedulable: Schedulable) {
  } 
  
  def removeSchedulable(schedulable: Schedulable) {
  }
  
  def getSchedulableByName(name: String): Schedulable = {
    return null
  }

  def executorLost(executorId: String, host: String): Unit = {
  }

  def receiveOffer(execId: String, host: String, avaiableCpus: Double): Option[TaskDescription] = {
    if (tasksFinished + runningTasks < numTasks) {
      increaseRunningTasks(1)
      return Some(new TaskDescription(0, stageId.toString, execId, "task 0:0", null))
    }
    return None
  }

  def checkSpeculatableTasks(): Boolean = {
    return true
  }

  def taskFinished() {
    decreaseRunningTasks(1)
    tasksFinished +=1
    if (tasksFinished == numTasks) {
      parent.removeSchedulable(this)
    }
  }

  def abort() {
    decreaseRunningTasks(runningTasks)
    parent.removeSchedulable(this)
  }
}

class ClusterSchedulerSuite extends FunSuite with BeforeAndAfter {
  
  def receiveOffer(rootPool: Pool) : Option[TaskDescription] = {
    rootPool.receiveOffer("execId_1", "hostname_1", 1)
  }

  def checkTaskSetId(rootPool: Pool, expectedTaskSetId: Int) {
    receiveOffer(rootPool) match {
      case Some(task) =>
        assert(task.taskSetId.toInt === expectedTaskSetId)
      case _ =>
    }
  }

  test("FIFO Scheduler Test") {
    val rootPool = new Pool("", SchedulingMode.FIFO, 0, 0)
    val schedulableBuilder = new FIFOSchedulableBuilder(rootPool)
    schedulableBuilder.buildPools()

    val taskSetManager0 = new DummyTaskSetManager(0, 0, 2)
    val taskSetManager1 = new DummyTaskSetManager(0, 1, 2)
    val taskSetManager2 = new DummyTaskSetManager(0, 2, 2)
    schedulableBuilder.addTaskSetManager(taskSetManager0, null)
    schedulableBuilder.addTaskSetManager(taskSetManager1, null)
    schedulableBuilder.addTaskSetManager(taskSetManager2, null)
    
    checkTaskSetId(rootPool, 0)
    receiveOffer(rootPool)
    checkTaskSetId(rootPool, 1)
    receiveOffer(rootPool)
    taskSetManager1.abort()
    checkTaskSetId(rootPool, 2)
  }

  test("Fair Scheduler Test") {
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    System.setProperty("spark.fairscheduler.allocation.file", xmlPath)
    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val schedulableBuilder = new FairSchedulableBuilder(rootPool)
    schedulableBuilder.buildPools()
    
    assert(rootPool.getSchedulableByName("default") != null)
    assert(rootPool.getSchedulableByName("1") != null)
    assert(rootPool.getSchedulableByName("2") != null)
    assert(rootPool.getSchedulableByName("3") != null)
    assert(rootPool.getSchedulableByName("1").minShare === 2)
    assert(rootPool.getSchedulableByName("1").weight === 1)
    assert(rootPool.getSchedulableByName("2").minShare === 3)
    assert(rootPool.getSchedulableByName("2").weight === 1)
    assert(rootPool.getSchedulableByName("3").minShare === 2)
    assert(rootPool.getSchedulableByName("3").weight === 1)

    val properties1 = new Properties()
    properties1.setProperty("spark.scheduler.cluster.fair.pool","1")
    val properties2 = new Properties()
    properties2.setProperty("spark.scheduler.cluster.fair.pool","2")
    
    val taskSetManager10 = new DummyTaskSetManager(1, 0, 1)
    val taskSetManager11 = new DummyTaskSetManager(1, 1, 1)
    val taskSetManager12 = new DummyTaskSetManager(1, 2, 2)
    schedulableBuilder.addTaskSetManager(taskSetManager10, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager11, properties1)
    schedulableBuilder.addTaskSetManager(taskSetManager12, properties1)
    
    val taskSetManager23 = new DummyTaskSetManager(2, 3, 2)
    val taskSetManager24 = new DummyTaskSetManager(2, 4, 2)
    schedulableBuilder.addTaskSetManager(taskSetManager23, properties2)
    schedulableBuilder.addTaskSetManager(taskSetManager24, properties2)

    checkTaskSetId(rootPool, 0)
    checkTaskSetId(rootPool, 3)
    checkTaskSetId(rootPool, 3)
    checkTaskSetId(rootPool, 1)
    checkTaskSetId(rootPool, 4)
    checkTaskSetId(rootPool, 2)
    checkTaskSetId(rootPool, 2)
    checkTaskSetId(rootPool, 4)

    taskSetManager12.taskFinished()
    assert(rootPool.getSchedulableByName("1").runningTasks === 3)
    taskSetManager24.abort()
    assert(rootPool.getSchedulableByName("2").runningTasks === 2)
  }

  test("Nested Pool Test") {
    val rootPool = new Pool("", SchedulingMode.FAIR, 0, 0)
    val pool0 = new Pool("0", SchedulingMode.FAIR, 3, 1)
    val pool1 = new Pool("1", SchedulingMode.FAIR, 4, 1)
    rootPool.addSchedulable(pool0)
    rootPool.addSchedulable(pool1)

    val pool00 = new Pool("00", SchedulingMode.FAIR, 2, 2)
    val pool01 = new Pool("01", SchedulingMode.FAIR, 1, 1)
    pool0.addSchedulable(pool00)
    pool0.addSchedulable(pool01)

    val pool10 = new Pool("10", SchedulingMode.FAIR, 2, 2)
    val pool11 = new Pool("11", SchedulingMode.FAIR, 2, 1)
    pool1.addSchedulable(pool10)
    pool1.addSchedulable(pool11)
    
    val taskSetManager000 = new DummyTaskSetManager(0, 0, 5)
    val taskSetManager001 = new DummyTaskSetManager(0, 1, 5)
    pool00.addSchedulable(taskSetManager000)
    pool00.addSchedulable(taskSetManager001)
    
    val taskSetManager010 = new DummyTaskSetManager(1, 2, 5)
    val taskSetManager011 = new DummyTaskSetManager(1, 3, 5)
    pool01.addSchedulable(taskSetManager010)
    pool01.addSchedulable(taskSetManager011)
  
    val taskSetManager100 = new DummyTaskSetManager(2, 4, 5)
    val taskSetManager101 = new DummyTaskSetManager(2, 5, 5)
    pool10.addSchedulable(taskSetManager100)
    pool10.addSchedulable(taskSetManager101)

    val taskSetManager110 = new DummyTaskSetManager(3, 6, 5)
    val taskSetManager111 = new DummyTaskSetManager(3, 7, 5)
    pool11.addSchedulable(taskSetManager110)
    pool11.addSchedulable(taskSetManager111)
    
    checkTaskSetId(rootPool, 0)
    checkTaskSetId(rootPool, 4)
    checkTaskSetId(rootPool, 6)
    checkTaskSetId(rootPool, 2)
  }
}
