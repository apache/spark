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

package org.apache.spark.util

import java.io.PrintStream
import java.net.URI
import java.nio.file.{Path, Paths}
import java.util.concurrent.CancellationException
import java.util.concurrent.locks.ReentrantLock

import org.apache.spark.util.ArrayImplicits._

/** Resolves runtime Ivy dependencies using one immutable Spark configuration snapshot. */
private[spark] final class RuntimeDependencyResolver(
    ivySettingsPath: Option[String],
    configuredRepositories: Seq[String],
    ivyPath: Option[String]) {
  import RuntimeDependencyResolver._

  /** Resolve an ivy URI. Calls are serialized because Ivy mutates process-wide state. */
  def resolve(
      uri: URI,
      connectTimeoutMs: Int,
      readTimeoutMs: Int,
      repositoryPolicy: RepositoryPolicy = AllowRequestedRepositories,
      isCancelled: () => Boolean = () => false): Seq[Path] = {
    checkCancelled(isCancelled)
    try {
      ivyLock.lockInterruptibly()
    } catch {
      case _: InterruptedException =>
        Thread.currentThread().interrupt()
        throw new CancellationException("Runtime Maven dependency resolution was cancelled")
    }

    try {
      require(uri.getScheme == "ivy", s"Expected an ivy URI, found: $uri")
      val authority = Option(uri.getAuthority).getOrElse {
        throw new IllegalArgumentException(
          s"Invalid Ivy URI authority in uri $uri: Expected 'org:module:version', found null.")
      }
      if (authority.split(":").length != 3) {
        throw new IllegalArgumentException(
          s"Invalid Ivy URI authority in uri $uri: " +
            s"Expected 'org:module:version', found $authority.")
      }

      checkCancelled(isCancelled)
      val (transitive, exclusions, requestedRepositories) = MavenUtils.parseQueryParams(uri)
      val requested = requestedRepositories
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toImmutableArraySeq
      val repositories = (configuredRepositories ++ repositoryPolicy.validate(requested))
        .iterator
        .map(_.trim)
        .filter(_.nonEmpty)
        .toSeq
        .distinct

      implicit val printStream: PrintStream = System.err
      val ivySettings = ivySettingsPath.filter(_.trim.nonEmpty) match {
        case Some(path) =>
          MavenUtils.loadIvySettings(path, repositoriesOption(repositories), ivyPath)
        case None => MavenUtils.buildIvySettings(repositoriesOption(repositories), ivyPath)
      }
      MavenUtils.setResolverTimeouts(ivySettings, connectTimeoutMs, readTimeoutMs)

      checkCancelled(isCancelled)
      val exclusionsList = exclusions
        .split(",")
        .map(_.trim)
        .filter(_.nonEmpty)
        .toImmutableArraySeq
      val result = MavenUtils.resolveMavenCoordinates(
        authority,
        ivySettings,
        transitive = transitive,
        exclusions = exclusionsList)
      checkCancelled(isCancelled)
      result.map(Paths.get(_))
    } finally {
      ivyLock.unlock()
    }
  }
}

private[spark] object RuntimeDependencyResolver {
  private val ivyLock = new ReentrantLock()

  trait RepositoryPolicy {
    def validate(requestedRepositories: Seq[String]): Seq[String]
  }

  object AllowRequestedRepositories extends RepositoryPolicy {
    override def validate(requestedRepositories: Seq[String]): Seq[String] = requestedRepositories
  }

  object RejectRequestedRepositories extends RepositoryPolicy {
    override def validate(requestedRepositories: Seq[String]): Seq[String] = {
      if (requestedRepositories.nonEmpty) {
        throw new IllegalArgumentException(
          "Server-side Maven dependencies do not allow repositories from the ivy URI")
      }
      Nil
    }
  }

  private def repositoriesOption(repositories: Seq[String]): Option[String] =
    Option(repositories.mkString(",")).filter(_.nonEmpty)

  private def checkCancelled(isCancelled: () => Boolean): Unit = {
    if (isCancelled() || Thread.currentThread().isInterrupted) {
      throw new CancellationException("Runtime Maven dependency resolution was cancelled")
    }
  }
}
