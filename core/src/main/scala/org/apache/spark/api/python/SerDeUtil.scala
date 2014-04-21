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

package org.apache.spark.api.python

import scala.util.Try
import org.apache.spark.rdd.RDD
import org.apache.spark.Logging
import scala.util.Success
import scala.util.Failure
import net.razorvine.pickle.Pickler

/**
 * Utilities for serialization / deserialization between Python and Java, using MsgPack.
 * Also contains utilities for converting [[org.apache.hadoop.io.Writable]] ->
 * Scala objects and primitives
 */
private[python] object SerDeUtil extends Logging {

  /**
   * Convert an RDD of key-value pairs to an RDD of serialized Python objects, that is usable
   * by PySpark. By default, if serialization fails, toString is called and the string
   * representation is serialized
   */
  def rddToPython[K, V](rdd: RDD[(K, V)]): RDD[Array[Byte]] = {
    rdd.mapPartitions{ iter =>
      val pickle = new Pickler
      var keyFailed = false
      var valueFailed = false
      var firstRecord = true
      iter.map{ case (k, v) =>
        if (firstRecord) {
          Try {
            pickle.dumps(Array(k, v))
          } match {
            case Success(b) =>
            case Failure(err) =>
              val kt = Try {
                pickle.dumps(k)
              }
              val vt = Try {
                pickle.dumps(v)
              }
              (kt, vt) match {
                case (Failure(kf), Failure(vf)) =>
                  log.warn(s"""Failed to pickle Java object as key: ${k.getClass.getSimpleName};
                    Error: ${kf.getMessage}""")
                  log.warn(s"""Failed to pickle Java object as value: ${v.getClass.getSimpleName};
                    Error: ${vf.getMessage}""")
                  keyFailed = true
                  valueFailed = true
                case (Failure(kf), _) =>
                  log.warn(s"""Failed to pickle Java object as key: ${k.getClass.getSimpleName};
                    Error: ${kf.getMessage}""")
                  keyFailed = true
                case (_, Failure(vf)) =>
                  log.warn(s"""Failed to pickle Java object as value: ${v.getClass.getSimpleName};
                    Error: ${vf.getMessage}""")
                  valueFailed = true
              }
          }
          firstRecord = false
        }
        (keyFailed, valueFailed) match {
          case (true, true) => pickle.dumps(Array(k.toString, v.toString))
          case (true, false) => pickle.dumps(Array(k.toString, v))
          case (false, true) => pickle.dumps(Array(k, v.toString))
          case (false, false) => pickle.dumps(Array(k, v))
        }
      }
    }
  }

}

