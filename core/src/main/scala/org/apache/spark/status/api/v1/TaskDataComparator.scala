package org.apache.spark.status.api.v1

import java.util.Comparator

class TaskDataComparator(var index: Int) extends Comparator[TaskData] {
  override def compare(o1: TaskData, o2: TaskData): Int = {

    if (o1.index == o2.index) return 0
    if (o1.index < o2.index) -1
    else 1
  }
}
