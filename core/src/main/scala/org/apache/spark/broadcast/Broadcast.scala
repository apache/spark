package org.apache.spark.broadcast

import java.io.Serializable
import scala.reflect.ClassTag
import org.apache.spark.SparkException
import org.apache.spark.internal.Logging
import org.apache.spark.util.Utils

/**
 * A broadcast variable. Broadcast variables allow the programmer to keep a read-only variable
 * cached on each machine rather than shipping a copy of it with tasks. They can be used, for
 * example, to give every node a copy of a large input dataset in an efficient manner. Spark also
 * attempts to distribute broadcast variables using efficient broadcast algorithms to reduce
 * communication cost.
 * 广播变量。广播变量使程序员可以在每台计算机上保留一个只读变量，而不用随任务一起发送它的副本。
 * 例如，可以使用它们以有效的方式为每个节点提供大型输入数据集的副本。 Spark还尝试使用有效的广播算法分配广播变量，以降低通信成本
 * Broadcast variables are created from a variable `v` by calling
 * [[org.apache.spark.SparkContext#broadcast]].
 * The broadcast variable is a wrapper around `v`, and its value can be accessed by calling the
 * `value` method. The interpreter session below shows this:
 *
 * 使用方法:
 * scala> val broadcastVar = sc.broadcast(Array(1, 2, 3))
 * broadcastVar: org.apache.spark.broadcast.Broadcast[Array[Int]] = Broadcast(0)
 *
 * 获取广播变量的值
 * scala> broadcastVar.value
 * res0: Array[Int] = Array(1, 2, 3)
 *
 *
 * After the broadcast variable is created, it should be used instead of the value `v` in any
 * functions run on the cluster so that `v` is not shipped to the nodes more than once.
 * In addition, the object `v` should not be modified after it is broadcast in order to ensure
 * that all nodes get the same value of the broadcast variable (e.g. if the variable is shipped
 * to a new node later).
 *
 * @param id A unique identifier for the broadcast variable.
 * @tparam T Type of the data contained in the broadcast variable.
 */
abstract class Broadcast[T: ClassTag](val id: Long) extends Serializable with Logging {

  /**
   * Flag signifying whether the broadcast variable is valid
   * (that is, not already destroyed) or not.
   * 表示广播变量是否有效的标志
   */
  @volatile private var _isValid = true

  private var _destroySite = ""

  /** Get the broadcasted value.
   * 获取广播变量的值 */
  def value: T = {
    assertValid()
    getValue()
  }

  /**
   * Asynchronously delete cached copies of this broadcast on the executors.
   * 异步删除执行程序上此广播的缓存副本。
   * If the broadcast is used after this is called, it will need to be re-sent to each executor.
   * 如果在调用广播后使用广播，则需要将其重新发送给每个执行者。
   */
  def unpersist() {
    unpersist(blocking = false)
  }

  /**
   * Delete cached copies of this broadcast on the executors. If the broadcast is used after
   * this is called, it will need to be re-sent to each executor.
   * @param blocking Whether to block until unpersisting has completed
   */
  def unpersist(blocking: Boolean) {
    assertValid()
    doUnpersist(blocking)
  }


  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * This method blocks until destroy has completed
   */
  def destroy() {
    destroy(blocking = true)
  }

  /**
   * Destroy all data and metadata related to this broadcast variable. Use this with caution;
   * once a broadcast variable has been destroyed, it cannot be used again.
   * @param blocking Whether to block until destroy has completed
   * 销毁与此广播变量有关的所有数据和元数据。请谨慎使用；广播变量一旦被销毁，将无法再次使用。
   */
  private[spark] def destroy(blocking: Boolean) {
    assertValid()
    _isValid = false
    _destroySite = Utils.getCallSite().shortForm
    logInfo("Destroying %s (from %s)".format(toString, _destroySite))
    doDestroy(blocking)
  }

  /**
   * Whether this Broadcast is actually usable. This should be false once persisted state is
   * removed from the driver.
   */
  private[spark] def isValid: Boolean = {
    _isValid
  }

  /**
   * Actually get the broadcasted value. Concrete implementations of Broadcast class must
   * define their own way to get the value.
   * 实际获得广播的值。 Broadcast类的具体实现必须定义自己的获取值的方式。
   */
  protected def getValue(): T

  /**
   * Actually unpersist the broadcasted value on the executors. Concrete implementations of
   * Broadcast class must define their own logic to unpersist their own data.
   * Broadcast类的具体实现必须定义自己的逻辑，以取消持久化自己的数据。
   */
  protected def doUnpersist(blocking: Boolean)

  /**
   * Actually destroy all data and metadata related to this broadcast variable.
   * Implementation of Broadcast class must define their own logic to destroy their own
   * state.
   */
  protected def doDestroy(blocking: Boolean)

  /** Check if this broadcast is valid. If not valid, exception is thrown.
   * 检查此广播是否有效。如果无效，则引发异常。
   */
  protected def assertValid() {
    if (!_isValid) {
      throw new SparkException(
        "Attempted to use %s after it was destroyed (%s) ".format(toString, _destroySite))
    }
  }

  /**
   * toString 方法
   * @return
   */
  override def toString: String = "Broadcast(" + id + ")"
}
