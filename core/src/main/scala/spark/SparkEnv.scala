package spark

import akka.actor.Actor

import spark.storage.BlockManager
import spark.storage.BlockManagerMaster
import spark.network.ConnectionManager

class SparkEnv (
    val cache: Cache,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheTracker: CacheTracker,
    val mapOutputTracker: MapOutputTracker,
    val shuffleFetcher: ShuffleFetcher,
    val shuffleManager: ShuffleManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager
  ) {

  /** No-parameter constructor for unit tests. */
  def this() = this(null, new JavaSerializer, new JavaSerializer, null, null, null, null, null, null)
}

object SparkEnv {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(isMaster: Boolean, isLocal: Boolean): SparkEnv = {
    val serializerClass = System.getProperty("spark.serializer", "spark.KryoSerializer")
    val serializer = Class.forName(serializerClass).newInstance().asInstanceOf[Serializer]
    
    BlockManagerMaster.startBlockManagerMaster(isMaster, isLocal)
    
    var blockManager = new BlockManager(serializer)
    
    val connectionManager = blockManager.connectionManager 
    
    val shuffleManager = new ShuffleManager()

    val closureSerializerClass =
      System.getProperty("spark.closure.serializer", "spark.JavaSerializer")
    val closureSerializer =
      Class.forName(closureSerializerClass).newInstance().asInstanceOf[Serializer]
    val cacheClass = System.getProperty("spark.cache.class", "spark.BoundedMemoryCache")
    val cache = Class.forName(cacheClass).newInstance().asInstanceOf[Cache]

    val cacheTracker = new CacheTracker(isMaster, blockManager)
    blockManager.cacheTracker = cacheTracker

    val mapOutputTracker = new MapOutputTracker(isMaster)

    val shuffleFetcherClass = 
      System.getProperty("spark.shuffle.fetcher", "spark.BlockStoreShuffleFetcher")
    val shuffleFetcher = 
      Class.forName(shuffleFetcherClass).newInstance().asInstanceOf[ShuffleFetcher]

    /*
    if (System.getProperty("spark.stream.distributed", "false") == "true") {
      val blockManagerClass = classOf[spark.storage.BlockManager].asInstanceOf[Class[_]] 
      if (isLocal || !isMaster) { 
        (new Thread() {
          override def run() {
            println("Wait started") 
            Thread.sleep(60000)
            println("Wait ended")
            val receiverClass = Class.forName("spark.stream.TestStreamReceiver4")
            val constructor = receiverClass.getConstructor(blockManagerClass)
            val receiver = constructor.newInstance(blockManager)
            receiver.asInstanceOf[Thread].start()
          }
        }).start()
      }
    }
    */

    new SparkEnv(cache, serializer, closureSerializer, cacheTracker, mapOutputTracker, shuffleFetcher,
        shuffleManager, blockManager, connectionManager)
  }
}
