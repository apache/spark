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
package org.apache.spark.deploy.k8s.integrationtest

import java.io.Closeable
import java.net.URI

object Utils extends Logging {

  def tryWithResource[R <: Closeable, T](createResource: => R)(f: R => T): T = {
    val resource = createResource
    try f.apply(resource) finally resource.close()
  }

  def tryWithSafeFinally[T](block: => T)(finallyBlock: => Unit): T = {
    var originalThrowable: Throwable = null
    try {
      block
    } catch {
      case t: Throwable =>
        // Purposefully not using NonFatal, because even fatal exceptions
        // we don't want to have our finallyBlock suppress
        originalThrowable = t
        throw originalThrowable
    } finally {
      try {
        finallyBlock
      } catch {
        case t: Throwable =>
          if (originalThrowable != null) {
            originalThrowable.addSuppressed(t)
            logWarning(s"Suppressing exception in finally: " + t.getMessage, t)
            throw originalThrowable
          } else {
            throw t
          }
      }
    }
  }

  def checkAndGetK8sMasterUrl(rawMasterURL: String): String = {
    require(rawMasterURL.startsWith("k8s://"),
      "Kubernetes master URL must start with k8s://.")
    val masterWithoutK8sPrefix = rawMasterURL.substring("k8s://".length)

    // To handle master URLs, e.g., k8s://host:port.
    if (!masterWithoutK8sPrefix.contains("://")) {
      val resolvedURL = s"https://$masterWithoutK8sPrefix"
      logInfo("No scheme specified for kubernetes master URL, so defaulting to https. Resolved " +
        s"URL is $resolvedURL.")
      return s"k8s://$resolvedURL"
    }

    val masterScheme = new URI(masterWithoutK8sPrefix).getScheme
    val resolvedURL = masterScheme.toLowerCase match {
      case "https" =>
        masterWithoutK8sPrefix
      case "http" =>
        logWarning("Kubernetes master URL uses HTTP instead of HTTPS.")
        masterWithoutK8sPrefix
      case null =>
        val resolvedURL = s"https://$masterWithoutK8sPrefix"
        logInfo("No scheme specified for kubernetes master URL, so defaulting to https. Resolved " +
          s"URL is $resolvedURL.")
        resolvedURL
      case _ =>
        throw new IllegalArgumentException("Invalid Kubernetes master scheme: " + masterScheme)
    }

    s"k8s://$resolvedURL"
  }
}
