package spark

import java.io._

import scala.collection.mutable.Map

@serializable class Accumulator[T](
  @transient initialValue: T, param: AccumulatorParam[T])
{
  val id = Accumulators.newId
  @transient var value_ = initialValue // Current value on master
  val zero = param.zero(initialValue)  // Zero value to be passed to workers
  var deserialized = false

  Accumulators.register(this)

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
    Accumulators.register(this)
  }

  override def toString = value_.toString
}

@serializable trait AccumulatorParam[T] {
  def addInPlace(t1: T, t2: T): T
  def zero(initialValue: T): T
}

// TODO: The multi-thread support in accumulators is kind of lame; check
// if there's a more intuitive way of doing it right
private object Accumulators
{
  // TODO: Use soft references? => need to make readObject work properly then
  val accums = Map[(Thread, Long), Accumulator[_]]()
  var lastId: Long = 0 
  
  def newId: Long = synchronized { lastId += 1; return lastId }

  def register(a: Accumulator[_]): Unit = synchronized { 
    accums((currentThread, a.id)) = a 
  }

  def clear: Unit = synchronized { 
    accums.retain((key, accum) => key._1 != currentThread)
  }

  def values: Map[Long, Any] = synchronized {
    val ret = Map[Long, Any]()
    for(((thread, id), accum) <- accums if thread == currentThread)
      ret(id) = accum.value
    return ret
  }

  def add(thread: Thread, values: Map[Long, Any]): Unit = synchronized {
    for ((id, value) <- values) {
      if (accums.contains((thread, id))) {
        val accum = accums((thread, id))
        accum.asInstanceOf[Accumulator[Any]] += value
      }
    }
  }
}
