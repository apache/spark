/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.spark.util.collection

import java.io._

import org.apache.spark.util.TaskCompletionListener
import org.apache.spark.{TaskContext, ExecutorCleaner, SparkEnv}

import scala.reflect.ClassTag
import scala.collection.generic.Growable
import scala.collection.mutable.ArrayBuffer

import com.esotericsoftware.kryo.io.{Output, Input}
import com.esotericsoftware.kryo.{Kryo, Serializer => KSerializer}

import org.apache.spark.util.collection.ExternalList._
import org.apache.spark.serializer.DeserializationStream
import org.apache.spark.storage.{DiskBlockObjectWriter, BlockId}


/**
 * List that can spill some of its contents to disk if its contents cannot be held in memory.
 * Implementation is based heavily on `org.apache.spark.util.collection.ExternalAppendOnlyMap}`
 */
@SerialVersionUID(1L)
private[spark] class ExternalList[T](implicit var tag: ClassTag[T])
    extends Growable[T]
    with Iterable[T]
    with SpillableCollection[T, SizeTrackingCompactBuffer[T]]
    with Serializable {

  // Var to allow rebuilding it during Java serialization
  private var spilledLists = new ArrayBuffer[DiskListIterable]
  private var currentInMemoryList = new SizeTrackingCompactBuffer[T]()
  private var numItems = 0

  // We don't know up front what files will need to be cleaned up from this list.
  // So check after the task is completed, after which this ExternalList will be
  // completely built.
  private var context = TaskContext.get
  if (context != null) {
    context.addTaskCompletionListener(new ScheduleCleanExternalList(this))
  }

  override def size(): Int = numItems

  override def +=(value: T): this.type = {
    currentInMemoryList += value
    if (maybeSpill(currentInMemoryList, currentInMemoryList.estimateSize())) {
      currentInMemoryList = new SizeTrackingCompactBuffer
    }
    numItems += 1
    this
  }

  override def clear(): Unit = {
    spilledLists.foreach(_.deleteBackingFile())
    spilledLists.clear()
    currentInMemoryList = new SizeTrackingCompactBuffer[T]()
  }

  def getBackingFileLocations(): Iterable[String] = {
    val locations = new ArrayBuffer[String]
    for (diskList <- spilledLists) {
      locations.append(diskList.backingFilePath())
    }
    return locations
  }

  def registerForCleanup(): Unit = {
    if (spilledLists.size > 0) {
      executorCleaner.registerExternalListForCleanup(this)
    }
  }

  override def iterator: Iterator[T] = {
    val myIt = currentInMemoryList.iterator
    val allIts = spilledLists.map(_.iterator) ++ Seq(myIt)
    allIts.foldLeft(Iterator[T]())(_ ++ _)
  }

  private class DiskListIterable(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
      extends Iterable[T] {
    override def iterator: Iterator[T] = {
      new DiskListIterator(file, blockId, batchSizes)
    }
    def deleteBackingFile(): Unit = {
      if (file.exists()) {
        file.delete()
      }
    }
    def backingFilePath(): String = file.getAbsolutePath()
  }

  private class DiskListIterator(file: File, blockId: BlockId, batchSizes: ArrayBuffer[Long])
      extends DiskIterator(file, blockId, batchSizes) {
    override protected def readNextItemFromStream(deserializeStream: DeserializationStream): T = {
      deserializeStream.readKey[Int]()
      deserializeStream.readValue[T]()
    }

    // Need to be able to iterate multiple times, so don't clean up the file every time
    override protected def shouldCleanupFileAfterOneIteration(): Boolean = false
  }

  @throws(classOf[IOException])
  private def writeObject(stream: ObjectOutputStream): Unit = {
    stream.writeObject(tag)
    stream.writeInt(this.size)
    val it = this.iterator
    while (it.hasNext) {
      stream.writeObject(it.next)
    }
  }

  @throws(classOf[IOException])
  private def readObject(stream: ObjectInputStream): Unit = {
    tag = stream.readObject().asInstanceOf[ClassTag[T]]
    val listSize = stream.readInt()
    spilledLists = new ArrayBuffer[DiskListIterable]
    currentInMemoryList = new SizeTrackingCompactBuffer[T]
    for(i <- 0L until listSize) {
      val newItem = stream.readObject().asInstanceOf[T]
      this.+=(newItem)
    }
    // Upon serialization, the context might have changed. So we can't just hold a single context,
    // but we must retrieving the current context every time.
    // Notice that in Kryo serialization this object is constructed from scratch
    // and thus will look for the current TaskContext that way.
    context = TaskContext.get()
    if (context != null) {
      context.addTaskCompletionListener(new ScheduleCleanExternalList(this))
    }
  }

  override protected def getIteratorForCurrentSpillable(): Iterator[T] = {
    currentInMemoryList.iterator
  }

  override protected def recordNextSpilledPart(
      file: File,
      blockId: BlockId,
      batchSizes: ArrayBuffer[Long]): Unit = {
    spilledLists += new DiskListIterable(file, blockId, batchSizes)
  }
  override protected def writeNextObject(c: T, writer: DiskBlockObjectWriter): Unit = {
    writer.write(0, c)
  }
}

/**
 * Companion object for constants and singleton-references that we don't want to lose when
 * Java-serializing
 */
private[spark] object ExternalList {

  private class ScheduleCleanExternalList(private var list: ExternalList[_])
      extends TaskCompletionListener {
    override def onTaskCompletion(context: TaskContext): Unit = {
      if (list != null) {
        executorCleaner.registerExternalListForCleanup(list)
        // Release reference to allow GC to clean it up
        list = null
      }
    }
  }

  def apply[T: ClassTag](): ExternalList[T] = new ExternalList[T]

  def apply[T: ClassTag](value: T): ExternalList[T] = {
    val buf = new ExternalList[T]
    buf += value
    buf
  }

  private val executorCleaner: ExecutorCleaner = SparkEnv.get.executorCleaner
}

private[spark] class ExternalListSerializer[T: ClassTag] extends KSerializer[ExternalList[T]] {
  override def write(kryo: Kryo, output: Output, list: ExternalList[T]): Unit = {
    output.writeInt(list.size)
    val it = list.iterator
    while (it.hasNext) {
      kryo.writeClassAndObject(output, it.next())
    }
  }

  override def read(kryo: Kryo, input: Input, clazz: Class[ExternalList[T]]): ExternalList[T] = {
    val listToRead = new ExternalList[T]
    val listSize = input.readInt()
    for (i <- 0L until listSize) {
      val newItem = kryo.readClassAndObject(input).asInstanceOf[T]
      listToRead += newItem
    }
    listToRead
  }
}
