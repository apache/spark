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

package org.apache.spark.deploy

import scala.concurrent.Promise
import scala.concurrent.duration._

import org.scalatest.matchers.must.Matchers
import org.scalatest.matchers.should.Matchers._

import org.apache.spark.{SecurityManager, SparkConf, SparkFunSuite}
import org.apache.spark.deploy.DeployMessages.RequestSubmitDriver
import org.apache.spark.deploy.master.Master
import org.apache.spark.deploy.rest.RestSubmissionClient
import org.apache.spark.internal.config.STANDALONE_SUBMIT_FILTER_ENVIRONMENT
import org.apache.spark.rpc.{RpcCallContext, RpcEndpoint, RpcEnv}
import org.apache.spark.util.ThreadUtils

class ClientSuite extends SparkFunSuite with Matchers {
  test("correctly validates driver jar URL's") {
    ClientArguments.isValidJarUrl("http://someHost:8080/foo.jar") should be (true)
    ClientArguments.isValidJarUrl("https://someHost:8080/foo.jar") should be (true)

    // file scheme with authority and path is valid.
    ClientArguments.isValidJarUrl("file://somehost/path/to/a/jarFile.jar") should be (true)

    // file scheme without path is not valid.
    // In this case, jarFile.jar is recognized as authority.
    ClientArguments.isValidJarUrl("file://jarFile.jar") should be (false)

    // file scheme without authority but with triple slash is valid.
    ClientArguments.isValidJarUrl("file:///some/path/to/a/jarFile.jar") should be (true)
    ClientArguments.isValidJarUrl("hdfs://someHost:1234/foo.jar") should be (true)

    ClientArguments.isValidJarUrl("hdfs://someHost:1234/foo") should be (false)
    ClientArguments.isValidJarUrl("/missing/a/protocol/jarfile.jar") should be (false)
    ClientArguments.isValidJarUrl("not-even-a-path.jar") should be (false)

    // This URI doesn't have authority and path.
    ClientArguments.isValidJarUrl("hdfs:someHost:1234/jarfile.jar") should be (false)

    // Invalid syntax.
    ClientArguments.isValidJarUrl("hdfs:") should be (false)
  }

  /**
   * Launches a driver through a [[ClientEndpoint]] wired to a fake master and returns the
   * [[Command]] carried by the [[RequestSubmitDriver]] message the client sends.
   */
  private def submittedCommand(conf: SparkConf): Command = {
    val env = RpcEnv.create("ClientSuite", "localhost", 0, conf, new SecurityManager(conf))
    try {
      val submitted = Promise[RequestSubmitDriver]()
      val master = env.setupEndpoint(Master.ENDPOINT_NAME, new RpcEndpoint {
        override val rpcEnv: RpcEnv = env
        // Record the submission without replying, so the client neither polls the driver
        // status nor exits the JVM.
        override def receiveAndReply(context: RpcCallContext): PartialFunction[Any, Unit] = {
          case request: RequestSubmitDriver => submitted.success(request)
        }
      })
      val args = new ClientArguments(
        Array("launch", "spark://localhost:7077", "file:///path/to/app.jar", "MainClass"))
      env.setupEndpoint("client", new ClientEndpoint(env, args, Seq(master), conf))
      ThreadUtils.awaitResult(submitted.future, 10.seconds).driverDescription.command
    } finally {
      env.shutdown()
      env.awaitTermination()
    }
  }

  test("SPARK-59404: forward only Spark-related environment variables to the driver") {
    // The submitting process always has non-Spark variables such as PATH, so forwarding
    // sys.env unfiltered would fail the assertion below.
    assert(sys.env.keys.exists(!_.startsWith("SPARK_")))
    val command = submittedCommand(new SparkConf())
    command.environment should be (RestSubmissionClient.filterSystemEnvironment(sys.env))
  }

  test("SPARK-59404: forward the full environment when filtering is disabled") {
    val conf = new SparkConf().set(STANDALONE_SUBMIT_FILTER_ENVIRONMENT, false)
    submittedCommand(conf).environment should be (sys.env)
  }
}
