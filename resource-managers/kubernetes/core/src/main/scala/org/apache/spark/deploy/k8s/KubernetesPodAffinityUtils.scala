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
package org.apache.spark.deploy.k8s

import java.util.NoSuchElementException

import scala.util.{Failure, Success, Try}

import org.apache.spark.SparkConf


private[spark] object KubernetesPodAffinityUtils {
  /**
   * Extract toleration configuration properties with a given name prefix.
   *
   * @param sparkConf Spark configuration
   * @param prefix the given property name prefix
   * @return a Seq of KubernetesTolerationSpec
   */
  def parsePodAffinityWithPrefix(
    sparkConf: SparkConf,
    prefix: String):
      KubernetesPodAffinitySpec[_ <: KubernetesPodAffinitySpecificConf]
   = {
    val affinity = sparkConf.getAllWithPrefix(prefix).toMap


    val weight = affinity.get("weight") match {
      case None => "100"
      case Some(x) => x
    }
    val key = affinity.get("key") match {
      case None => null
      case Some(x) => x
    }
    val operator = affinity.get("operator") match {
      case None => "In"
      case Some(x) => x
    }
    val topology = affinity.get("topology") match {
      case None => "kubernetes.io/hostname"
      case Some(x) => x
    }
    val value = affinity.get("value") match {
      case None => null
      case Some(x) => x
    }


    Try {
      KubernetesPodAffinitySpec(
        key,
        operator,
        value,
        weight,
        topology)
    }.get

  }

  private def getAffinityConfig(
    properties: Map[String, String]
  ): Map[String, Map[String, String]] = {

    var affinityConfig = Map[String, Map[String, String]]()

    var a = properties.keys foreach { k =>
      val l = k.split('.').toList
      var m = affinityConfig.getOrElse(l.head, Map[String, String]())
      m = m + {l.last -> properties(k)}
      affinityConfig = affinityConfig + { l.head -> m }

    }

    return affinityConfig

  }


  /**
   * Convenience wrapper to accumulate key lookup errors
   */
  implicit private class MapOps[A, B](m: Map[A, B]) {
    def getTry(key: A): Try[B] = {
      m
        .get(key)
        .fold[Try[B]](Failure(new NoSuchElementException(key.toString)))(Success(_))
    }
  }
}
