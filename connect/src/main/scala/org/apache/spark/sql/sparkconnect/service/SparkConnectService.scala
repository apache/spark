/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.sparkconnect.service

import java.util
import java.util.concurrent.TimeUnit

import com.google.common.base.Ticker
import com.google.common.cache.CacheBuilder
import io.grpc.netty.shaded.io.grpc.netty.NettyServerBuilder
import io.grpc.protobuf.services.ProtoReflectionService
import io.grpc.stub.StreamObserver
import scala.collection.JavaConverters._
import scala.concurrent.{ExecutionContext, Future}

import org.apache.spark.{SparkContext, SparkEnv}
import org.apache.spark.api.plugin.{DriverPlugin, ExecutorPlugin, PluginContext, SparkPlugin}
import org.apache.spark.connect.proto.{AnalyzeResponse, Request, Response, SparkConnectServiceGrpc}
import org.apache.spark.sql.SparkSession

class SparkConnectService(debug: Boolean) extends SparkConnectServiceGrpc.SparkConnectService {
  override def executePlan(request: Request, responseObserver: StreamObserver[Response]): Unit =
    new SparkConnectStreamHandler(responseObserver).handle(request)

  override def analyzePlan(request: Request): Future[AnalyzeResponse] = {
    Future.successful(AnalyzeResponse(clientId = request.clientId))
  }
}

case class SessionHolder(userId: String, session: SparkSession) {}

object SparkConnectService {

  private val userSessionMapping =
    cacheBuilder(100, 3600).build[String, SessionHolder]()

  // Used to create cache instances
  private def cacheBuilder(cacheSize: Int, timeoutSeconds: Int): CacheBuilder[Object, Object] = {
    var cacheBuilder = CacheBuilder.newBuilder().ticker(Ticker.systemTicker())
    if (cacheSize >= 0) {
      cacheBuilder = cacheBuilder.maximumSize(cacheSize)
    }
    if (timeoutSeconds >= 0) {
      cacheBuilder.expireAfterAccess(timeoutSeconds, TimeUnit.SECONDS)
    }
    cacheBuilder
  }

  def getOrCreateIsolatedSession(userId: String): SessionHolder = {
    userSessionMapping.get(userId, () => {
      SessionHolder(userId, newIsolatedSession())
    })
  }

  private def newIsolatedSession(): SparkSession = {
    SparkSession.active.newSession()
  }

  def startGRPCService(): Unit = {
    val debugMode = SparkEnv.get.conf.getBoolean("spark.connect.grpc.debug.enabled", true)
    val port = 15001
    val server = NettyServerBuilder
      .forPort(port)
      .addService(
        SparkConnectServiceGrpc
          .bindService(new SparkConnectService(debugMode), ExecutionContext.global))

    // If debug mode is configured,
    if (debugMode) {
      server.addService(ProtoReflectionService.newInstance())
    }

    server.build.start
  }

//  def startHttpService(): Unit = {
//    val bossGroup = new NioEventLoopGroup(1)
//    val workerGroup = new NioEventLoopGroup()
//    try {
//      val b = new ServerBootstrap()
//      b.group(bossGroup, workerGroup)
//        .channel(classOf[NioServerSocketChannel])
//        .handler(new LoggingHandler(LogLevel.DEBUG))
//        .childHandler(new ChannelInitializer[SocketChannel] {
//          override def initChannel(c: SocketChannel): Unit = {
//            val p = c.pipeline()
//            p.addLast(new HttpRequestDecoder())
//            p.addLast(new HttpResponseEncoder())
//            // 10MB max content length
//            p.addLast(new HttpObjectAggregator(10048576))
//            p.addLast(new SparkConnectHttpServerHandler())
//          }
//        })
//
//      val f = b.bind(15001).sync()
//      f.channel().closeFuture().sync()
//    } finally {
//      bossGroup.shutdownGracefully()
//      workerGroup.shutdownGracefully()
//    }
//  }

  // Starts the service
  def start(): Unit = {
    val shouldStartHttpService =
      SparkEnv.get.conf.getBoolean("spark.connect.http_server", defaultValue = false)
    if (shouldStartHttpService) {
//      val t = new Thread {
//        override def run(): Unit = {
//          startHttpService()
//        }
//      }
//      t.start()
    } else {
      startGRPCService()
    }
  }
}

class SparkConnectPlugin extends SparkPlugin {

  /**
   * Return the plugin's driver-side component.
   *
   * @return The driver-side component, or null if one is not needed.
   */
  override def driverPlugin(): DriverPlugin = new DriverPlugin {

    override def init(sc: SparkContext, pluginContext: PluginContext): util.Map[String, String] = {
      SparkConnectService.start()
      Map.empty[String, String].asJava
    }

    override def shutdown(): Unit = {
      super.shutdown()
    }
  }

  override def executorPlugin(): ExecutorPlugin = null
}
