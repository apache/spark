/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.sql.hive.thriftserver.cli.thrift

import java.util
import java.util.concurrent._

import org.apache.hadoop.hive.conf.HiveConf
import org.apache.hadoop.hive.conf.HiveConf.ConfVars
import org.apache.hadoop.hive.shims.ShimLoader
import org.apache.thrift.TProcessorFactory
import org.apache.thrift.protocol.TBinaryProtocol
import org.apache.thrift.server.TThreadPoolServer
import org.apache.thrift.transport.{TServerSocket, TTransportFactory}

import org.apache.spark.internal.Logging
import org.apache.spark.sql.hive.thriftserver.auth.HiveAuthFactory
import org.apache.spark.sql.hive.thriftserver.cli.CLIService
import org.apache.spark.sql.hive.thriftserver.server.NamedThreadFactory


class ThriftBinaryCLIService(cliService: CLIService)
  extends ThriftCLIService(cliService, classOf[ThriftBinaryCLIService].getSimpleName)
    with Logging {

  override def run(): Unit = {
    try { // Server thread pool
      val threadPoolName: String = "HiveServer2-Handler-Pool"
      val executorService: ExecutorService =
        new ThreadPoolExecutor(minWorkerThreads,
          maxWorkerThreads,
          workerKeepAliveTime,
          TimeUnit.SECONDS,
          new SynchronousQueue[Runnable],
          new NamedThreadFactory(threadPoolName))
      // Thrift configs
      hiveAuthFactory = new HiveAuthFactory(hiveConf)
      val transportFactory: TTransportFactory = hiveAuthFactory.getAuthTransFactory
      val processorFactory: TProcessorFactory = hiveAuthFactory.getAuthProcFactory(this)
      var serverSocket: TServerSocket = null
      val sslVersionBlacklist: util.List[String] = new util.ArrayList[String]
      for (sslVersion <- hiveConf.getVar(ConfVars.HIVE_SSL_PROTOCOL_BLACKLIST).split(",")) {
        sslVersionBlacklist.add(sslVersion)
      }
      if (!hiveConf.getBoolVar(ConfVars.HIVE_SERVER2_USE_SSL)) {
        serverSocket = HiveAuthFactory.getServerSocket(hiveHost, portNum)
      } else {
        val keyStorePath: String = hiveConf.getVar(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH).trim
        if (keyStorePath.isEmpty) {
          throw new IllegalArgumentException(ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PATH.varname +
            " Not configured for SSL connection")
        }
        val keyStorePassword: String = ShimLoader.getHadoopShims
          .getPassword(hiveConf, HiveConf.ConfVars.HIVE_SERVER2_SSL_KEYSTORE_PASSWORD.varname)
        serverSocket = HiveAuthFactory.getServerSSLSocket(hiveHost, portNum, keyStorePath,
          keyStorePassword, sslVersionBlacklist)
      }
      // Server args
      val maxMessageSize: Int =
        hiveConf.getIntVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_MAX_MESSAGE_SIZE)
      val requestTimeout: Int =
        hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_TIMEOUT,
          TimeUnit.SECONDS).toInt
      val beBackoffSlotLength: Int =
        hiveConf.getTimeVar(HiveConf.ConfVars.HIVE_SERVER2_THRIFT_LOGIN_BEBACKOFF_SLOT_LENGTH,
          TimeUnit.MILLISECONDS).toInt
      val sargs: TThreadPoolServer.Args =
        new TThreadPoolServer.Args(serverSocket)
          .processorFactory(processorFactory)
          .transportFactory(transportFactory)
          .protocolFactory(new TBinaryProtocol.Factory)
          .inputProtocolFactory(
            new TBinaryProtocol.Factory(true, true, maxMessageSize, maxMessageSize))
          .requestTimeout(requestTimeout)
          .requestTimeoutUnit(TimeUnit.SECONDS)
          .beBackoffSlotLength(beBackoffSlotLength)
          .beBackoffSlotLengthUnit(TimeUnit.MILLISECONDS)
          .executorService(executorService)
      // TCP Server
      server = new TThreadPoolServer(sargs)
      server.setServerEventHandler(serverEventHandler)
      val msg: String = "Starting " + classOf[ThriftBinaryCLIService].getSimpleName +
        " on port " + serverSocket.getServerSocket.getLocalPort + " with " +
        minWorkerThreads + "..." + maxWorkerThreads + " worker threads"
      logInfo(msg)
      server.serve()
    } catch {
      case t: Throwable =>
        logError("Error starting SparkThriftServer: could not start " +
          classOf[ThriftBinaryCLIService].getSimpleName, t)
        System.exit(-1)
    }
  }
}
