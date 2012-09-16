package spark

import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.remote.RemoteActorRefProvider

import spark.broadcast.BroadcastManager
import spark.storage.BlockManager
import spark.storage.BlockManagerMaster
import spark.network.ConnectionManager
import spark.util.AkkaUtils

class SparkEnv (
    val actorSystem: ActorSystem,
    val cache: Cache,
    val serializer: Serializer,
    val closureSerializer: Serializer,
    val cacheTracker: CacheTracker,
    val mapOutputTracker: MapOutputTracker,
    val shuffleFetcher: ShuffleFetcher,
    val shuffleManager: ShuffleManager,
    val broadcastManager: BroadcastManager,
    val blockManager: BlockManager,
    val connectionManager: ConnectionManager,
    val httpFileServer: HttpFileServer
  ) {

  /** No-parameter constructor for unit tests. */
  def this() = {
    this(null, null, new JavaSerializer, new JavaSerializer, null, null, null, null, null, null, null, null)
  }

  def stop() {
    httpFileServer.stop()
    mapOutputTracker.stop()
    cacheTracker.stop()
    shuffleFetcher.stop()
    shuffleManager.stop()
    broadcastManager.stop()
    blockManager.stop()
    blockManager.master.stop()
    actorSystem.shutdown()
    actorSystem.awaitTermination()
    // Akka's awaitTermination doesn't actually wait until the port is unbound, so sleep a bit
    Thread.sleep(100)
  }
}

object SparkEnv {
  private val env = new ThreadLocal[SparkEnv]

  def set(e: SparkEnv) {
    env.set(e)
  }

  def get: SparkEnv = {
    env.get()
  }

  def createFromSystemProperties(
      hostname: String,
      port: Int,
      isMaster: Boolean,
      isLocal: Boolean
    ) : SparkEnv = {

    val (actorSystem, boundPort) = AkkaUtils.createActorSystem("spark", hostname, port)

    // Bit of a hack: If this is the master and our port was 0 (meaning bind to any free port),
    // figure out which port number Akka actually bound to and set spark.master.port to it.
    if (isMaster && port == 0) {
      System.setProperty("spark.master.port", boundPort.toString)
    }

    val serializerClass = System.getProperty("spark.serializer", "spark.JavaSerializer")
    val serializer = Class.forName(serializerClass).newInstance().asInstanceOf[Serializer]
    
    val blockManagerMaster = new BlockManagerMaster(actorSystem, isMaster, isLocal)

    val blockManager = new BlockManager(blockManagerMaster, serializer)
    
    val connectionManager = blockManager.connectionManager 
    
    val shuffleManager = new ShuffleManager()

    val broadcastManager = new BroadcastManager(isMaster)

    val closureSerializerClass =
      System.getProperty("spark.closure.serializer", "spark.JavaSerializer")
    val closureSerializer =
      Class.forName(closureSerializerClass).newInstance().asInstanceOf[Serializer]
    val cacheClass = System.getProperty("spark.cache.class", "spark.BoundedMemoryCache")
    val cache = Class.forName(cacheClass).newInstance().asInstanceOf[Cache]

    val cacheTracker = new CacheTracker(actorSystem, isMaster, blockManager)
    blockManager.cacheTracker = cacheTracker

    val mapOutputTracker = new MapOutputTracker(actorSystem, isMaster)

    val shuffleFetcherClass = 
      System.getProperty("spark.shuffle.fetcher", "spark.BlockStoreShuffleFetcher")
    val shuffleFetcher = 
      Class.forName(shuffleFetcherClass).newInstance().asInstanceOf[ShuffleFetcher]
    
    val httpFileServer = new HttpFileServer()
    httpFileServer.initialize()
    System.setProperty("spark.fileserver.uri", httpFileServer.serverUri)
    
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

    new SparkEnv(
      actorSystem,
      cache,
      serializer,
      closureSerializer,
      cacheTracker,
      mapOutputTracker,
      shuffleFetcher,
      shuffleManager,
      broadcastManager,
      blockManager,
      connectionManager,
      httpFileServer)
  }
}
