package org.apache.spark.scheduler.eagle

import java.io._

class EagleTriple[T,U,V](first: T, second: U, third: V) extends Serializable {
  
  override def toString(): String = {
      "(" + first + ", " + second + ", " + third + ')';
  }

  def getFirst(): T = {
    return first
  }

}