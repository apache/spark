package spark.util

import akka.actor.{Props, ActorSystemImpl, ActorSystem}
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
import spark.SparkException
import java.util.concurrent.TimeoutException

/**
 * Various utility classes for working with Akka.
 */
private[spark] object AkkaUtils {
  /**
   * Creates an ActorSystem ready for remoting, with various Spark features. Returns both the
   * ActorSystem itself and its port (which is hard to get from Akka).
   */
  def createActorSystem(name: String, host: String, port: Int): (ActorSystem, Int) = {
    val akkaThreads = System.getProperty("spark.akka.threads", "4").toInt
    val akkaBatchSize = System.getProperty("spark.akka.batchSize", "15").toInt
    val akkaConf = ConfigFactory.parseString("""
      akka.daemonic = on
      akka.event-handlers = ["akka.event.slf4j.Slf4jEventHandler"]
      akka.actor.provider = "akka.remote.RemoteActorRefProvider"
      akka.remote.transport = "akka.remote.netty.NettyRemoteTransport"
      akka.remote.netty.hostname = "%s"
      akka.remote.netty.port = %d
      akka.remote.netty.connection-timeout = 1s
      akka.remote.netty.execution-pool-size = %d
      akka.actor.default-dispatcher.throughput = %d
      """.format(host, port, akkaThreads, akkaBatchSize))

    val actorSystem = ActorSystem("spark", akkaConf, getClass.getClassLoader)

    // Figure out the port number we bound to, in case port was passed as 0. This is a bit of a
    // hack because Akka doesn't let you figure out the port through the public API yet.
    val provider = actorSystem.asInstanceOf[ActorSystemImpl].provider
    val boundPort = provider.asInstanceOf[RemoteActorRefProvider].transport.address.port.get
    return (actorSystem, boundPort)
  }

  /**
   * Creates a Spray HTTP server bound to a given IP and port with a given Spray Route object to
   * handle requests. Throws a SparkException if this fails.
   */
  def startSprayServer(actorSystem: ActorSystem, ip: String, port: Int, route: Route) {
    val ioWorker = new IoWorker(actorSystem).start()
    val httpService = actorSystem.actorOf(Props(new HttpService(route)))
    val rootService = actorSystem.actorOf(Props(new SprayCanRootService(httpService)))
    val server = actorSystem.actorOf(
      Props(new HttpServer(ioWorker, SingletonHandler(rootService))), name = "HttpServer")
    actorSystem.registerOnTermination { ioWorker.stop() }
    val timeout = 3.seconds
    val future = server.ask(HttpServer.Bind(ip, port))(timeout)
    try {
      Await.result(future, timeout) match {
        case bound: HttpServer.Bound =>
          return
        case other: Any =>
          throw new SparkException("Failed to bind web UI to port " + port + ": " + other)
      }
    } catch {
      case e: TimeoutException =>
        throw new SparkException("Failed to bind web UI to port " + port)
    }
  }
}
