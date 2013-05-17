package spark.util

import akka.actor.{ActorRef, Props, ActorSystemImpl, ActorSystem}
import com.typesafe.config.ConfigFactory
import akka.util.duration._
import akka.pattern.ask
import akka.remote.RemoteActorRefProvider
import cc.spray.Route
import cc.spray.io.IoWorker
import cc.spray.{SprayCanRootService, HttpService}
import cc.spray.can.server.HttpServer
import cc.spray.io.pipelines.MessageHandlerDispatch.SingletonHandler
import akka.dispatch.Await
import spark.{Utils, SparkException}
import java.util.concurrent.TimeoutException

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils {

  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   *
   * Note: the `name` parameter is important, as even if a client sends a message to right
   * host + port, if the system name is incorrect, Akka will drop the message.
   */
  def createActorSystem(name: String, host: String, port: Int): (ActorSystem, Int) = {
    val akkaThreads = System.getProperty("spark.akka.threads", "4").toInt
    val akkaBatchSize = System.getProperty("spark.akka.batchSize", "15").toInt
    val akkaTimeout = System.getProperty("spark.akka.timeout", "20").toInt
    val akkaFrameSize = System.getProperty("spark.akka.frameSize", "10").toInt
    val lifecycleEvents = if (System.getProperty("spark.akka.logLifecycleEvents", "false").toBoolean) "on" else "off"
    // 10 seconds is the default akka timeout, but in a cluster, we need higher by default.
    val akkaWriteTimeout = System.getProperty("spark.akka.writeTimeout", "30").toInt
    
    val akkaConf = ConfigFactory.parseString("""
      akka.daemonic = on
      akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
      akka.stdout-loglevel = "ERROR"
      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
      akka.remote.netty.hostname = "%s"
      akka.remote.netty.port = %d
      akka.remote.netty.connection-timeout = %ds
      akka.remote.netty.message-frame-size = %d MiB
      akka.remote.netty.execution-pool-size = %d
      akka.actor.default-dispatcher.throughput = %d
      akka.remote.log-remote-lifecycle-events = %s
      akka.remote.netty.write-timeout = %ds
      """.format(host, port, akkaTimeout, akkaFrameSize, akkaThreads, akkaBatchSize,
        lifecycleEvents, akkaWriteTimeout))

    val actorSystem = ActorSystem(name, akkaConf, getClass.getClassLoader)

    // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
    // hack because Akka doesn't let you figure out the port through the public API yet.
    val provider = actorSystem.asInstanceOf[ActorSystemImpl].provider
    val boundPort = provider.asInstanceOf[RemoteActorRefProvider].transport.address.port.get
    return (actorSystem, boundPort)
  }

  /**
   * Creates a Spray HTTP server bound to a given IP and port with a given Spray Route object to
   * handle requests. Returns the bound port or throws a SparkException on failure.
   * TODO: Not changing ip to host here - is it required ?
   */
  def startSprayServer(actorSystem: ActorSystem, ip: String, port: Int, route: Route,
      name: String = "HttpServer"): ActorRef = {
    val ioWorker = new IoWorker(actorSystem).start()
    val httpService = actorSystem.actorOf(Props(new HttpService(route)))
    val rootService = actorSystem.actorOf(Props(new SprayCanRootService(httpService)))
    val server = actorSystem.actorOf(
      Props(new HttpServer(ioWorker, SingletonHandler(rootService))), name = name)
    actorSystem.registerOnTermination { ioWorker.stop() }
    val timeout = 3.seconds
    val future = server.ask(HttpServer.Bind(ip, port))(timeout)
    try {
      Await.result(future, timeout) match {
        case bound: HttpServer.Bound =>
          return server
        case other: Any =>
          throw new SparkException("Failed to bind web UI to port " + port + ": " + other)
      }
    } catch {
      case e: TimeoutException =>
        throw new SparkException("Failed to bind web UI to port " + port)
    }
  }
}
