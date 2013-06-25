package spark.util

/** Provides a basic/boilerplate Iterator implementation. */
private[spark] abstract class NextIterator[U] extends Iterator[U] {
  
  private var gotNext = false
  private var nextValue: U = _
  private var closed = false
  protected var finished = false

  /**
   * Method for subclasses to implement to provide the next element.
   *
   * If no next element is available, the subclass should set `finished`
   * to `true` and may return any value (it will be ignored).
   *
   * This convention is required because `null` may be a valid value,
   * and using `Option` seems like it might create unnecessary Some/None
   * instances, given some iterators might be called in a tight loop.
   * 
   * @return U, or set 'finished' when done
   */
  protected def getNext(): U

  /**
   * Method for subclasses to implement when all elements have been successfully
   * iterated, and the iteration is done.
   *
   * <b>Note:</b> `NextIterator` cannot guarantee that `close` will be
   * called because it has no control over what happens when an exception
   * happens in the user code that is calling hasNext/next.
   *
   * Ideally you should have another try/catch, as in HadoopRDD, that
   * ensures any resources are closed should iteration fail.
   */
  protected def close()

  /**
   * Calls the subclass-defined close method, but only once.
   *
   * Usually calling `close` multiple times should be fine, but historically
   * there have been issues with some InputFormats throwing exceptions.
   */
  def closeIfNeeded() {
    if (!closed) {
      close()
      closed = true
    }
  }

  override def hasNext: Boolean = {
    if (!finished) {
      if (!gotNext) {
        nextValue = getNext()
        if (finished) {
          closeIfNeeded()
        }
        gotNext = true
      }
    }
    !finished
  }

  override def next(): U = {
    if (!hasNext) {
      throw new NoSuchElementException("End of stream")
    }
    gotNext = false
    nextValue
  }
}