package spark

import java.io._

import scala.collection.mutable.Map

class Accumulator[T] (
  @transient initialValue: T, param: AccumulatorParam[T]) extends Serializable
{
  val id = Accumulators.newId
  @transient var value_ = initialValue // Current value on master
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  Accumulators.register(this, true)

  def += (term: T) { value_ = param.addInPlace(value_, term) }
  def value = this.value_
  def value_= (t: T) {
    if (!deserialized) value_ = t
    else throw new UnsupportedOperationException("Can't use value_= in task")
  }
 
  // Called by Java when deserializing an object
  private def readObject(in: ObjectInputStream) {
    in.defaultReadObject
    value_ = zero
    deserialized = true
    Accumulators.register(this, false)
  }

  override def toString = value_.toString
}

trait AccumulatorParam[T] extends Serializable {
  def addInPlace(t1: T, t2: T): T
  def zero(initialValue: T): T
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators
{
  // TODO: Use soft references? => need to make readObject work properly then
  val originals = Map[Long, Accumulator[_]]()
  val localAccums = Map[Thread, Map[Long, Accumulator[_]]]()
  var lastId: Long = 0
  
  def newId: Long = synchronized { lastId += 1; return lastId }

  def register(a: Accumulator[_], original: Boolean): Unit = synchronized {
    if (original) {
      originals(a.id) = a
    } else {
      val accums = localAccums.getOrElseUpdate(Thread.currentThread, Map())
      accums(a.id) = a
    }
  }

  // Clear the local (non-original) accumulators for the current thread
  def clear: Unit = synchronized { 
    localAccums.remove(Thread.currentThread)
  }

  // Get the values of the local accumulators for the current thread (by ID)
  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for ((id, accum) <- localAccums.getOrElse(Thread.currentThread, Map()))
      ret(id) = accum.value
    return ret
  }

  // Add values to the original accumulators with some given IDs
  def add(values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (originals.contains(id)) {
        originals(id).asInstanceOf[Accumulator[Any]] += value
      }
    }
  }
}
