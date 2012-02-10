package spark

import java.util.concurrent.Semaphore

import org.scalatest.FunSuite

import SparkContext._

class ThreadingSuite extends FunSuite {
  test("accessing SparkContext form a different thread") {
    val sc = new SparkContext("local", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var answer1: Int = 0
    @volatile var answer2: Int = 0
    new Thread {
      override def run() {
        answer1 = nums.reduce(_ + _)
        answer2 = nums.first    // This will run "locally" in the current thread
        sem.release()
      }
    }.start()
    sem.acquire()
    assert(answer1 === 55)
    assert(answer2 === 1)
    sc.stop()
  }

  test("accessing SparkContext form multiple threads") {
    val sc = new SparkContext("local", "test")
    val nums = sc.parallelize(1 to 10, 2)
    val sem = new Semaphore(0)
    @volatile var ok = true
    for (i <- 0 until 10) {
      new Thread {
        override def run() {
          val answer1 = nums.reduce(_ + _)
          if (answer1 != 55) {
            printf("In thread %d: answer1 was %d\n", i, answer1);
            ok = false;
          }
          val answer2 = nums.first    // This will run "locally" in the current thread
          if (answer2 != 1) {
            printf("In thread %d: answer2 was %d\n", i, answer2);
            ok = false;
          }
          sem.release()
        }
      }.start()
    }
    sem.acquire(10)
    if (!ok) {
      fail("One or more threads got the wrong answer from an RDD operation")
    }
    sc.stop()
  }
}
