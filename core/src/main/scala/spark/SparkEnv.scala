package spark

class SparkEnv (
  val cache: Cache,
  val serializer: Serializer,
  val cacheTracker: CacheTracker,
  val mapOutputTracker: MapOutputTracker
)

object SparkEnv {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(isMaster: Boolean): SparkEnv = {
    val cacheClass = System.getProperty("spark.cache.class", "spark.SoftReferenceCache")
    val cache = Class.forName(cacheClass).newInstance().asInstanceOf[Cache]
    
    val serClass = System.getProperty("spark.serializer", "spark.JavaSerializer")
    val ser = Class.forName(serClass).newInstance().asInstanceOf[Serializer]

    val cacheTracker = new CacheTracker(isMaster, cache)

    val mapOutputTracker = new MapOutputTracker(isMaster)

    new SparkEnv(cache, ser, cacheTracker, mapOutputTracker)
  }
}
