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
package org.apache.spark.sql.connect.client

import scala.annotation.tailrec

/**
 * Parser that takes an array of (CLI) arguments and configures a [[SparkConnectClient]] with
 * them.
 */
private[sql] object SparkConnectClientParser {

  // scalastyle:off line.size.limit
  /**
   * @return
   *   usage string.
   */
  def usage(): String =
    s"""
       |Options:
       |   --remote REMOTE              URI of the Spark Connect Server to connect to.
       |   --host HOST                  Host where the Spark Connect Server is running.
       |   --port PORT                  Port where the Spark Connect Server is running.
       |   --use_ssl                    Connect to the server using SSL.
       |   --token TOKEN                Token to use for authentication.
       |   --user_id USER_ID            Id of the user connecting.
       |   --user_name USER_NAME        Name of the user connecting.
       |   --user_agent USER_AGENT      The User-Agent Client information (only intended for logging purposes by the server).
       |   --session_id SESSION_ID      Session Id of the user connecting.
       |   --grpc_max_message_size SIZE Maximum message size allowed for gRPC messages in bytes.
       |   --option KEY=VALUE           Key-value pair that is used to further configure the session.
     """.stripMargin
  // scalastyle:on line.size.limit

  /**
   * Parse the command line and configure the builder.
   */
  @tailrec
  def parse(args: List[String], builder: SparkConnectClient.Builder): Unit = {
    args match {
      case Nil => ()
      case "--remote" :: tail =>
        val (value, remainder) = extract("--remote", tail)
        parse(remainder, builder.connectionString(value))
      case "--host" :: tail =>
        val (value, remainder) = extract("--host", tail)
        parse(remainder, builder.host(value))
      case "--port" :: tail =>
        val (value, remainder) = extract("--port", tail)
        parse(remainder, builder.port(value.toInt))
      case "--token" :: tail =>
        val (value, remainder) = extract("--token", tail)
        parse(remainder, builder.token(value))
      case "--use_ssl" :: tail =>
        parse(tail, builder.enableSsl())
      case "--user_id" :: tail =>
        val (value, remainder) = extract("--user_id", tail)
        parse(remainder, builder.userId(value))
      case "--user_name" :: tail =>
        val (value, remainder) = extract("--user_name", tail)
        parse(remainder, builder.userName(value))
      case "--user_agent" :: tail =>
        val (value, remainder) = extract("--user_agent", tail)
        parse(remainder, builder.userAgent(value))
      case "--session_id" :: tail =>
        val (value, remainder) = extract("--session_id", tail)
        parse(remainder, builder.sessionId(value))
      case "--option" :: tail =>
        if (args.isEmpty) {
          throw new IllegalArgumentException("--option requires a key-value pair")
        }
        val Array(key, value, rest @ _*) = tail.head.split('=')
        if (rest.nonEmpty) {
          throw new IllegalArgumentException(
            s"--option should contain key=value, found ${tail.head} instead")
        }
        parse(tail.tail, builder.option(key, value))
      case "--grpc_max_message_size" :: tail =>
        val (value, remainder) = extract("--grpc_max_message_size", tail)
        parse(remainder, builder.grpcMaxMessageSize(value.toInt))
      case unsupported :: _ =>
        throw new IllegalArgumentException(s"$unsupported is an unsupported argument.")
    }
  }

  private def extract(name: String, args: List[String]): (String, List[String]) = {
    require(args.nonEmpty, s"$name option requires a value")
    (args.head, args.tail)
  }
}
