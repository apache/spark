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
package org.apache.spark.security

import org.apache.spark.{SparkConf, SparkFunSuite}
import org.apache.spark.internal.config._

trait EncryptionFunSuite {

  this: SparkFunSuite =>

  /**
   * Runs a test twice, initializing a SparkConf object with encryption off, then on. It's ok
   * for the test to modify the provided SparkConf.
   */
  final protected def encryptionTest(name: String)(fn: SparkConf => Unit) {
    Seq(false, true).foreach { encrypt =>
      test(s"$name (encryption = ${ if (encrypt) "on" else "off" })") {
        val conf = new SparkConf().set(IO_ENCRYPTION_ENABLED, encrypt)
        fn(conf)
      }
    }
  }

}
