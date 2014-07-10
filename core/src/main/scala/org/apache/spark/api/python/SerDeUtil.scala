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


/** Utilities for serialization / deserialization between Python and Java, using Pickle. */
private[python] object SerDeUtil extends Logging {

  private def checkPickle(t: (Any, Any)): (Boolean, Boolean) = {
    val pickle = new Pickler
    val kt = Try {
      pickle.dumps(t._1)
    }
    val vt = Try {
      pickle.dumps(t._2)
    }
    (kt, vt) match {
      case (Failure(kf), Failure(vf)) =>
        logWarning(s"""
               |Failed to pickle Java object as key: ${t._1.getClass.getSimpleName}, falling back
               |to 'toString'. Error: ${kf.getMessage}""".stripMargin)
        logWarning(s"""
               |Failed to pickle Java object as value: ${t._2.getClass.getSimpleName}, falling back
               |to 'toString'. Error: ${vf.getMessage}""".stripMargin)
        (true, true)
      case (Failure(kf), _) =>
        logWarning(s"""
               |Failed to pickle Java object as key: ${t._1.getClass.getSimpleName}, falling back
               |to 'toString'. Error: ${kf.getMessage}""".stripMargin)
        (true, false)
      case (_, Failure(vf)) =>
        logWarning(s"""
               |Failed to pickle Java object as value: ${t._2.getClass.getSimpleName}, falling back
               |to 'toString'. Error: ${vf.getMessage}""".stripMargin)
        (false, true)
      case _ =>
        (false, false)
    }
  }

  /**
   * Convert an RDD of key-value pairs to an RDD of serialized Python objects, that is usable
   * by PySpark. By default, if serialization fails, toString is called and the string
   * representation is serialized
   */
  def rddToPython(rdd: RDD[(Any, Any)]): RDD[Array[Byte]] = {
    val (keyFailed, valueFailed) = checkPickle(rdd.first())
    rdd.mapPartitions { iter =>
      val pickle = new Pickler
      iter.map { case (k, v) =>
        if (keyFailed && valueFailed) {
          pickle.dumps(Array(k.toString, v.toString))
        } else if (keyFailed) {
          pickle.dumps(Array(k.toString, v))
        } else if (!keyFailed && valueFailed) {
          pickle.dumps(Array(k, v.toString))
        } else {
          pickle.dumps(Array(k, v))
        }
      }
    }
  }

}

