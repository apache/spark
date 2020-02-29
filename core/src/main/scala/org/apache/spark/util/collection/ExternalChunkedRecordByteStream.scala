package org.apache.spark.util.collection


/**
  * An OutputStream that writes to fixed-size chunks of byte arrays.
  *
  * @param recordSize size of each chunk, in bytes.
  */
private[spark] class ExternalChunkedRecordByteStream[T](recordSize: Int)
  extends Iterator[T] {

  private[this]  var valueArray: Array[Any] = new Array(recordSize)
  private[this] var currentIndex: Int = 0

  /** Index of the last chunk. Starting with -1 when the chunks array is empty. */
  private[this] var lastRecordIndex: Int = 0

  /**
    * Next position to write in the last chunk.
    *
    * If this equals recordSize, it means for next write we need to allocate a new chunk.
    * This can also never be 0.
    */
  private[this] val maxRecordSize = recordSize
  private[this] var _size: Long = 0L
  private[this] var closed: Boolean = false
  private[this] var _used: Boolean = false
  private[this] var _isConsumeKill: Boolean = false

  private  var value: T = null.asInstanceOf[T]

  def readBytes: Long = _size

  def readRecords: Long = lastRecordIndex.toLong

  def getUsed: Boolean = _used

  def setUsed(use: Boolean): Unit = {
    _used = use
  }

  def getConsumeKill: Boolean = _isConsumeKill

  def setConsumeKill(use: Boolean): Unit = {
    _isConsumeKill = use
  }

  def init(): Unit = {
    currentIndex = 0
    lastRecordIndex = 0
    _size = 0
  }

  def close(): Unit = {
    if (!closed) {
      closed = true
    }
  }

  def write(recordValue: T): Boolean = {
    require(!closed, "cannot write to a closed ChunkedRecordByteStream")
    valueArray(lastRecordIndex) = recordValue
    lastRecordIndex += 1

    if (lastRecordIndex < maxRecordSize) {
      true
    } else {
      false
    }
  }

  override def hasNext: Boolean = {
    currentIndex < lastRecordIndex
  }

  override def next(): T = {
    value = valueArray(currentIndex).asInstanceOf[T]
    currentIndex += 1
    value
  }

  @inline
  def allocateNeededRecord(): Unit = {
    (0 until maxRecordSize).foreach { index =>
      // valueArray(index) = new ExternalByteArrayOutputStream(1024)
    }
  }
}

