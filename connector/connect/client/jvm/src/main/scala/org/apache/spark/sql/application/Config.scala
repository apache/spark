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
package org.apache.spark.sql.application

import scala.annotation.tailrec
import scala.util.control.NonFatal

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.connect.client.SparkConnectClient

/**
 * Configuration for the REPL, and other Spark Connect applications.
 */
case class Config(
    remote: Option[String] = None,
    host: Option[String] = None,
    port: Option[Int] = None,
    token: Option[String] = None,
    userId: Option[String] = None,
    userName: Option[String] = None,
    userAgent: Option[String] = None,
    options: Seq[(String, String)] = Nil) {

  /**
   * Create a [[SparkSession]] based on the configurations set.
   */
  def createSession(): SparkSession = {
    org.apache.spark.sql.SparkSession.builder()
      .client(createClient())
      .build()
  }

  /**
   * Create a [[SparkConnectClient]] based on the configurations set.
   */
  def createClient(): SparkConnectClient = {
    val remoteEnvVar = sys.env.get("SPARK_REMOTE")

    def ignoringRemoteEnvVar(): Unit = {
      if (remoteEnvVar.isDefined) {
        // scalastyle:off println
        println("Ignoring SPARK_REMOTE env variable.")
        // scalastyle:on println
      }
    }

    val builder = SparkConnectClient.builder()
    val hasHostPortToken = host.isDefined || port.isDefined || token.isDefined
    if (remote.isDefined && hasHostPortToken) {
      throw new IllegalArgumentException(
        "Both a remote URI and a host/port/token have been configured. " +
          "This is ambiguous an is not supported. Either configure the remote, " +
          "or configure everything through the individual host/port/token configurations.")
    } else if (remote.isDefined) {
      ignoringRemoteEnvVar()
      remote.foreach(builder.connectionString)
    } else if (hasHostPortToken) {
      ignoringRemoteEnvVar()
      host.foreach(builder.host)
      port.foreach(builder.port)
      token.foreach(builder.token)
    } else {
      remoteEnvVar.foreach(builder.connectionString)
    }
    userId.foreach(builder.userId)
    userName.foreach(builder.userName)
    userAgent.foreach(builder.userAgent)
    options.foreach {
      case (k, v) =>
        builder.option(k, v)
    }
    builder.build()
  }
}

object Config {
  def usage(): String =
    s"""
       |Options:
       |   --remote REMOTE          URI of the Spark Connect Server to connect to.
       |   --host HOST              Host where the Spark Connect Server is running.
       |   --port PORT              Port where the Spark Connect Server is running.
       |   --token TOKEN            Token to use for authentication.
       |   --user_id USER_ID        Id of the user connecting.
       |   --user_name USER_NAME    Name of the user connecting.
       |   --option KEY=VALUE       Key-value pair that is used to further configure the session.
     """.stripMargin

  /**
   * Parse the arguments and try to create a [[Config]].
   */
  def apply(args: Array[String]): Config = parse(args.toList, new Config())

  /**
   * Parse the arguments and try to create a [[Config]]. If we fail, we exit the application.
   */
  def parseOrExit(application: String, args: Array[String]): Config = {
    try {
      parse(args.toList, new Config())
        .copy(userAgent = Option(application))
    } catch {
      case NonFatal(e) =>
        // scalastyle:off println
        println(
          s"""
             |$application
             |${e.getMessage}
             |${usage()}
             |""".stripMargin)
        // scalastyle:on println
        System.exit(1)
        null
    }
  }

  @tailrec
  private def parse(args: List[String], config: Config): Config = {
    args match {
      case Nil =>
        config
      case "--remote" :: tail =>
        val (value, remainder) = extract("--remote", tail)
        parse(remainder, config.copy(remote = Option(value)))
      case "--host" :: tail =>
        val (value, remainder) = extract("--host", tail)
        parse(remainder, config.copy(host = Option(value)))
      case "--port" :: tail =>
        val (value, remainder) = extract("--port", tail)
        parse(remainder, config.copy(port = Option(value.toInt)))
      case "--token" :: tail =>
        val (value, remainder) = extract("--token", tail)
        parse(remainder, config.copy(token = Option(value)))
      case "--user_id" :: tail =>
        val (value, remainder) = extract("--user_id", tail)
        parse(remainder, config.copy(userId = Option(value)))
      case "--user_name" :: tail =>
        val (value, remainder) = extract("--user_name", tail)
        parse(remainder, config.copy(userName = Option(value)))
      case "--option" :: tail =>
        if (args.isEmpty) {
          throw new IllegalArgumentException("--option requires a key-value pair")
        }
        val Array(key, value, rest@_*) = tail.head.split('=')
        if (rest.nonEmpty) {
          throw new IllegalArgumentException(
            s"--option should contain key=value, found ${tail.head} instead")
        }
        parse(tail.tail, config.copy(options = config.options :+ (key, value)))
      case unsupported :: _ =>
        throw new IllegalArgumentException(s"$unsupported is an unsupported argument.")
    }
  }

  private def extract(name: String, args: List[String]): (String, List[String]) = {
    require(args.nonEmpty, s"$name option requires a value")
    (args.head, args.tail)
  }
}
