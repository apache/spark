package spark.serializer

import java.util.concurrent.ConcurrentHashMap


/**
 * A service that returns a serializer object given the serializer's class name. If a previous
 * instance of the serializer object has been created, the get method returns that instead of
 * creating a new one.
 */
private[spark] class SerializerManager {

  private val serializers = new ConcurrentHashMap[String, Serializer]
  private var _default: Serializer = _

  def default = _default

  def setDefault(clsName: String): Serializer = {
    _default = get(clsName)
    _default
  }

  def get(clsName: String): Serializer = {
    if (clsName == null) {
      default
    } else {
      var serializer = serializers.get(clsName)
      if (serializer != null) {
        // If the serializer has been created previously, reuse that.
        serializer
      } else this.synchronized {
        // Otherwise, create a new one. But make sure no other thread has attempted
        // to create another new one at the same time.
        serializer = serializers.get(clsName)
        if (serializer == null) {
          val clsLoader = Thread.currentThread.getContextClassLoader
          serializer =
            Class.forName(clsName, true, clsLoader).newInstance().asInstanceOf[Serializer]
          serializers.put(clsName, serializer)
        }
        serializer
      }
    }
  }
}
