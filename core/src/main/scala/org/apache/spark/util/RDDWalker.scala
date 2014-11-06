package org.apache.spark.util

import java.util

import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * This class allows execution of a function on an RDD and all of its dependencies. This is accomplished by
 * walking the object graph linking these RDDs. This is useful for debugging internal RDD references.
 */
object RDDWalker {

  val walkQueue = new util.ArrayDeque[RDD[_]]
  var visited = new util.HashSet[RDD[_]]


  /**
   *
   * Execute the passed function on the underlying RDD
   * * @param rddToWalk - The RDD to traverse along with its dependencies
   * @param func - The function to execute on
   * TODO Can there be cycles in RDD dependencies?
   */
  def walk(rddToWalk : RDD[_], func : (RDD[_])=>Unit): Unit ={
    
    //Implement as a queue to perform a BFS
    walkQueue.addFirst(rddToWalk)

    while(!walkQueue.isEmpty){
      //Pop from the queue
      val rddToProcess : RDD[_] = walkQueue.pollFirst()
      if(!visited.contains(rddToProcess)){
        rddToProcess.equals()
        rddToProcess.dependencies.foreach(s => walkQueue.addFirst(s.rdd))
        func(rddToProcess)
        visited.add(rddToProcess)
      }
    }
  }

  def getRddString(rddToWalk : RDD[_]): Unit ={
    rddToWalk.toDebugString
  }

  def toString(rddToWalk : RDD[_]) : Unit = {

  }
}
