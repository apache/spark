package spark.partial

class PartialResult[R](initialVal: R, isFinal: Boolean) {
  private var finalValue: Option[R] = if (isFinal) Some(initialVal) else None
  private var failure: Option[Exception] = None
  private var completionHandler: Option[R => Unit] = None
  private var failureHandler: Option[Exception => Unit] = None

  def initialValue: R = initialVal

  def isInitialValueFinal: Boolean = isFinal

  /**
   * Blocking method to wait for and return the final value.
   */
  def getFinalValue(): R = synchronized {
    while (finalValue == None && failure == None) {
      this.wait()
    }
    if (finalValue != None) {
      return finalValue.get
    } else {
      throw failure.get
    }
  }

  /** 
   * Set a handler to be called when this PartialResult completes. Only one completion handler
   * is supported per PartialResult.
   */
  def onComplete(handler: R => Unit): PartialResult[R] = synchronized {
    if (completionHandler != None) {
      throw new UnsupportedOperationException("onComplete cannot be called twice")
    }
    completionHandler = Some(handler)
    if (finalValue != None) {
      // We already have a final value, so let's call the handler
      handler(finalValue.get)
    }
    return this
  }

  /** 
   * Set a handler to be called if this PartialResult's job fails. Only one failure handler
   * is supported per PartialResult.
   */
  def onFail(handler: Exception => Unit) {
    synchronized {
      if (failureHandler != None) {
        throw new UnsupportedOperationException("onFail cannot be called twice")
      }
      failureHandler = Some(handler)
      if (failure != None) {
        // We already have a failure, so let's call the handler
        handler(failure.get)
      }
    }
  }

  /**
   * Transform this PartialResult into a PartialResult of type T.
   */
  def map[T](f: R => T) : PartialResult[T] = {
    new PartialResult[T](f(initialVal), isFinal) {
       override def getFinalValue() : T = synchronized {
         f(PartialResult.this.getFinalValue())
       }
       override def onComplete(handler: T => Unit): PartialResult[T] = synchronized {
         PartialResult.this.onComplete(handler.compose(f)).map(f)
       }
      override def onFail(handler: Exception => Unit) {
        synchronized {
          PartialResult.this.onFail(handler)
        }
      }
      override def toString : String = synchronized {
        PartialResult.this.getFinalValueInternal() match {
          case Some(value) => "(final: " + f(value) + ")"
          case None => "(partial: " + initialValue + ")"
        }
      }
      def getFinalValueInternal() = PartialResult.this.getFinalValueInternal().map(f)
    }
  }

  private[spark] def setFinalValue(value: R) {
    synchronized {
      if (finalValue != None) {
        throw new UnsupportedOperationException("setFinalValue called twice on a PartialResult")
      }
      finalValue = Some(value)
      // Call the completion handler if it was set
      completionHandler.foreach(h => h(value))
      // Notify any threads that may be calling getFinalValue()
      this.notifyAll()
    }
  }

  private def getFinalValueInternal() = finalValue

  private[spark] def setFailure(exception: Exception) {
    synchronized {
      if (failure != None) {
        throw new UnsupportedOperationException("setFailure called twice on a PartialResult")
      }
      failure = Some(exception)
      // Call the failure handler if it was set
      failureHandler.foreach(h => h(exception))
      // Notify any threads that may be calling getFinalValue()
      this.notifyAll()
    }
  }

  override def toString: String = synchronized {
    finalValue match {
      case Some(value) => "(final: " + value + ")"
      case None => "(partial: " + initialValue + ")"
    }
  }
}
