package spark.scheduler

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import spark._
import spark.scheduler._
import spark.scheduler.cluster._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{ConcurrentMap, HashMap}
import java.util.concurrent.Semaphore
import java.util.concurrent.atomic.AtomicBoolean
import java.util.concurrent.atomic.AtomicInteger

import java.util.Properties

class Lock() {
  var finished = false
  def jobWait() = {
    synchronized {
      while(!finished) {
        this.wait()
      }
    }
  }

  def jobFinished() = {
    synchronized {
      finished = true
      this.notifyAll()
    }
  }
}

object TaskThreadInfo {
  val threadToLock = HashMap[Int, Lock]()
  val threadToRunning = HashMap[Int, Boolean]()
}


class LocalSchedulerSuite extends FunSuite with LocalSparkContext {

  def createThread(threadIndex: Int, poolName: String, sc: SparkContext, sem: Semaphore) {
   
    TaskThreadInfo.threadToRunning(threadIndex) = false
    val nums = sc.parallelize(threadIndex to threadIndex, 1)
    TaskThreadInfo.threadToLock(threadIndex) = new Lock()
    new Thread {
        if (poolName != null) {
          sc.addLocalProperties("spark.scheduler.cluster.fair.pool",poolName)
        }
        override def run() {
          val ans = nums.map(number => {
            TaskThreadInfo.threadToRunning(number) = true
            TaskThreadInfo.threadToLock(number).jobWait()
            number
          }).collect()
          assert(ans.toList === List(threadIndex))
          sem.release()
          TaskThreadInfo.threadToRunning(threadIndex) = false
        }
    }.start()
    Thread.sleep(2000)
  }
  
  test("Local FIFO scheduler end-to-end test") {
    System.setProperty("spark.cluster.schedulingmode", "FIFO")
    sc = new SparkContext("local[4]", "test")
    val sem = new Semaphore(0)

    createThread(1,null,sc,sem)
    createThread(2,null,sc,sem)
    createThread(3,null,sc,sem)
    createThread(4,null,sc,sem)
    createThread(5,null,sc,sem)
    createThread(6,null,sc,sem)
    assert(TaskThreadInfo.threadToRunning(1) === true)
    assert(TaskThreadInfo.threadToRunning(2) === true)
    assert(TaskThreadInfo.threadToRunning(3) === true)
    assert(TaskThreadInfo.threadToRunning(4) === true)
    assert(TaskThreadInfo.threadToRunning(5) === false)
    assert(TaskThreadInfo.threadToRunning(6) === false)
    
    TaskThreadInfo.threadToLock(1).jobFinished()
    Thread.sleep(1000)
   
    assert(TaskThreadInfo.threadToRunning(1) === false)
    assert(TaskThreadInfo.threadToRunning(2) === true)
    assert(TaskThreadInfo.threadToRunning(3) === true)
    assert(TaskThreadInfo.threadToRunning(4) === true)
    assert(TaskThreadInfo.threadToRunning(5) === true)
    assert(TaskThreadInfo.threadToRunning(6) === false)
    
    TaskThreadInfo.threadToLock(3).jobFinished()
    Thread.sleep(1000)

    assert(TaskThreadInfo.threadToRunning(1) === false)
    assert(TaskThreadInfo.threadToRunning(2) === true)
    assert(TaskThreadInfo.threadToRunning(3) === false)
    assert(TaskThreadInfo.threadToRunning(4) === true)
    assert(TaskThreadInfo.threadToRunning(5) === true)
    assert(TaskThreadInfo.threadToRunning(6) === true)
   
    TaskThreadInfo.threadToLock(2).jobFinished()
    TaskThreadInfo.threadToLock(4).jobFinished()
    TaskThreadInfo.threadToLock(5).jobFinished()
    TaskThreadInfo.threadToLock(6).jobFinished()
    sem.acquire(6)
  }

  test("Local fair scheduler end-to-end test") {
    sc = new SparkContext("local[8]", "LocalSchedulerSuite")
    val sem = new Semaphore(0)
    System.setProperty("spark.cluster.schedulingmode", "FAIR")
    val xmlPath = getClass.getClassLoader.getResource("fairscheduler.xml").getFile()
    System.setProperty("spark.fairscheduler.allocation.file", xmlPath)

    createThread(10,"1",sc,sem)
    createThread(20,"2",sc,sem)
    createThread(30,"3",sc,sem)
   
    assert(TaskThreadInfo.threadToRunning(10) === true)
    assert(TaskThreadInfo.threadToRunning(20) === true)
    assert(TaskThreadInfo.threadToRunning(30) === true)
    
    createThread(11,"1",sc,sem)
    createThread(21,"2",sc,sem)
    createThread(31,"3",sc,sem)
    
    assert(TaskThreadInfo.threadToRunning(11) === true)
    assert(TaskThreadInfo.threadToRunning(21) === true)
    assert(TaskThreadInfo.threadToRunning(31) === true)

    createThread(12,"1",sc,sem)
    createThread(22,"2",sc,sem)
    createThread(32,"3",sc,sem)

    assert(TaskThreadInfo.threadToRunning(12) === true)
    assert(TaskThreadInfo.threadToRunning(22) === true)
    assert(TaskThreadInfo.threadToRunning(32) === false)
    
    TaskThreadInfo.threadToLock(10).jobFinished()
    Thread.sleep(1000)
    assert(TaskThreadInfo.threadToRunning(32) === true)

    createThread(23,"2",sc,sem)
    createThread(33,"3",sc,sem)
    
    TaskThreadInfo.threadToLock(11).jobFinished()
    Thread.sleep(1000)

    assert(TaskThreadInfo.threadToRunning(23) === true)
    assert(TaskThreadInfo.threadToRunning(33) === false)

    TaskThreadInfo.threadToLock(12).jobFinished()
    Thread.sleep(1000)
    
    assert(TaskThreadInfo.threadToRunning(33) === true)

    TaskThreadInfo.threadToLock(20).jobFinished()
    TaskThreadInfo.threadToLock(21).jobFinished()
    TaskThreadInfo.threadToLock(22).jobFinished()
    TaskThreadInfo.threadToLock(23).jobFinished()
    TaskThreadInfo.threadToLock(30).jobFinished()
    TaskThreadInfo.threadToLock(31).jobFinished()
    TaskThreadInfo.threadToLock(32).jobFinished()
    TaskThreadInfo.threadToLock(33).jobFinished()
    
    sem.acquire(11) 
  }
}
