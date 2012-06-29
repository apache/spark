package spark

import akka.actor.ActorSystem
import akka.actor.ActorSystemImpl
import akka.remote.RemoteActorRefProvider

import com.typesafe.config.ConfigFactory

import spark.storage.BlockManager
import spark.storage.BlockManagerMaster
import spark.network.ConnectionManager

class SparkEnv (
    val actorSystem: ActorSystem,
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
  def this() = this(null, null, new JavaSerializer, new JavaSerializer, null, null, null, null, null, null)
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

    val akkaConf = ConfigFactory.parseString("""
      akka.daemonic = on
      akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
      akka.remote.netty.hostname = "%s"
      akka.remote.netty.port = %d
      """.format(hostname, port))

    val actorSystem = ActorSystem("spark", akkaConf, getClass.getClassLoader)

    // Bit of a hack: If this is the master and our port was 0 (meaning bind to any free port),
    // figure out which port number Akka actually bound to and set spark.master.port to it.
    // Unfortunately Akka doesn't yet provide an API for this except if you cast objects as below.
    if (isMaster && port == 0) {
      val provider = actorSystem.asInstanceOf[ActorSystemImpl].provider
      val port = provider.asInstanceOf[RemoteActorRefProvider].transport.address.port.get
      System.setProperty("spark.master.port", port.toString)
    }

    val serializerClass = System.getProperty("spark.serializer", "spark.KryoSerializer")
    val serializer = Class.forName(serializerClass).newInstance().asInstanceOf[Serializer]
    
    BlockManagerMaster.startBlockManagerMaster(actorSystem, isMaster, isLocal)
    
    var blockManager = new BlockManager(serializer)
    
    val connectionManager = blockManager.connectionManager 
    
    val shuffleManager = new ShuffleManager()

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
      blockManager,
      connectionManager)
  }
}
