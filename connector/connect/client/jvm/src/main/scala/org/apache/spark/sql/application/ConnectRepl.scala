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

import ammonite.compiler.CodeClassWrapper
import ammonite.util.Bind

import org.apache.spark.annotation.DeveloperApi

/**
 * REPL for spark connect.
 */
@DeveloperApi
object ConnectRepl {
  private val splash =
    """
      |Spark session available as 'spark'.
      |   _____                  __      ______                            __
      |  / ___/____  ____ ______/ /__   / ____/___  ____  ____  ___  _____/ /_
      |  \__ \/ __ \/ __ `/ ___/ //_/  / /   / __ \/ __ \/ __ \/ _ \/ ___/ __/
      | ___/ / /_/ / /_/ / /  / ,<    / /___/ /_/ / / / / / / /  __/ /__/ /_
      |/____/ .___/\__,_/_/  /_/|_|   \____/\____/_/ /_/_/ /_/\___/\___/\__/
      |    /_/
      |""".stripMargin

  def main(args: Array[String]): Unit = {
    // Build the session.
    val spark = Config.parseOrExit("Spark Connect REPL", args).createSession()
    val bind = new Bind("spark", spark)

    // Add the proper imports.
    val imports =
      """
        |import org.apache.spark.sql.functions._
        |import spark.implicits._
        |import spark.sql
        |""".stripMargin

     val main = ammonite.Main(
       welcomeBanner = Option(splash),
       predefCode = imports,
       replCodeWrapper = CodeClassWrapper,
       scriptCodeWrapper = CodeClassWrapper)
    main.run(bind)
  }
}
