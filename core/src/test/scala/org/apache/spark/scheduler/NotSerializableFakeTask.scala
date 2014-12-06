package org.apache.spark.scheduler

import java.io.{ObjectInputStream, ObjectOutputStream, IOException}

import org.apache.spark.TaskContext

/**
 * A Task implementation that fails to serialize.
 */
class NotSerializableFakeTask(myId: Int, stageId: Int) extends Task[Array[Byte]](stageId, 0) {
  override def runTask(context: TaskContext): Array[Byte] = Array.empty[Byte]
  override def preferredLocations: Seq[TaskLocation] = Seq[TaskLocation]()

  @throws(classOf[IOException])
  private def writeObject(out: ObjectOutputStream): Unit = {
    if (stageId == 0) {
      throw new IllegalStateException("Cannot serialize")
    }
  }

  @throws(classOf[IOException])
  private def readObject(in: ObjectInputStream): Unit = {}
}
