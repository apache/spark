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
package org.apache.spark.api.conda

import java.io.File
import java.net.URI
import java.nio.file.Path
import java.util.{Map => JMap}
import javax.ws.rs.core.UriBuilder

import scala.collection.mutable

import org.apache.spark.internal.Logging

/**
 * A stateful class that describes a Conda environment and also keeps track of packages that have
 * been added, as well as additional channels.
 *
 * @param rootPath  The root path under which envs/ and pkgs/ are located.
 * @param envName   The name of the environment.
 */
final class CondaEnvironment(val manager: CondaEnvironmentManager,
                             val rootPath: Path,
                             val envName: String,
                             bootstrapPackages: Seq[String],
                             bootstrapChannels: Seq[String],
                             extraArgs: Seq[String] = Nil,
                             envVars: Map[String, String] = Map.empty) extends Logging {

  import CondaEnvironment._

  private[this] val packages = mutable.Buffer(bootstrapPackages: _*)
  private[this] val channels = bootstrapChannels.iterator.map(AuthenticatedChannel.apply).toBuffer

  val condaEnvDir: Path = rootPath.resolve("envs").resolve(envName)

  def activatedEnvironment(startEnv: Map[String, String] = Map.empty): Map[String, String] = {
    require(!startEnv.contains("PATH"), "Defining PATH in a CondaEnvironment's startEnv is " +
      s"prohibited; found PATH=${startEnv("PATH")}")
    import collection.JavaConverters._
    val newVars = System.getenv().asScala.toIterator ++ startEnv ++ List(
      "CONDA_PREFIX" -> condaEnvDir.toString,
      "CONDA_DEFAULT_ENV" -> condaEnvDir.toString,
      "PATH" -> (condaEnvDir.resolve("bin").toString +
        sys.env.get("PATH").map(File.pathSeparator + _).getOrElse(""))
    )
    newVars.toMap
  }

  def addChannel(url: String): Unit = {
    channels += AuthenticatedChannel(url)
  }

  def setChannels(urls: Seq[String]): Unit = {
    channels.clear()
    channels ++= urls.iterator.map(AuthenticatedChannel.apply)
  }

  def installPackages(packages: Seq[String]): Unit = {
    manager.runCondaProcess(rootPath,
      List("install", "-n", envName, "-y")
        ::: extraArgs.toList
        ::: "--" :: packages.toList,
      description = s"install dependencies in conda env $condaEnvDir",
      channels = channels.iterator.map(_.url).toList,
      envVars = envVars
    )

    this.packages ++= packages
  }

  /**
   * Clears the given java environment and replaces all variables with the environment
   * produced after calling `activate` inside this conda environment.
   */
  def initializeJavaEnvironment(env: JMap[String, String]): Unit = {
    env.clear()
    val activatedEnv = activatedEnvironment()
    activatedEnv.foreach { case (k, v) => env.put(k, v) }
    logDebug(s"Initialised environment from conda: $activatedEnv")
  }

  /**
   * This is for sending the instructions to the executors so they can replicate the same steps.
   */
  def buildSetupInstructions: CondaSetupInstructions = {
    CondaSetupInstructions(packages.toList, channels.toList, extraArgs, envVars)
  }
}

object CondaEnvironment {
  private[this] case class ChannelWithCreds(unauthenticatedChannel: UnauthenticatedChannel,
                                            userInfo: Option[String])

  private[this] case class ChannelsWithCreds(unauthenticatedChannels: Seq[UnauthenticatedChannel],
                                             userInfos: Map[UnauthenticatedChannel, String])

  /** A channel URI that might have credentials set. */
  private[CondaEnvironment] case class AuthenticatedChannel(url: String) extends AnyVal {
    def split(): ChannelWithCreds = {
      val uri = UriBuilder.fromUri(url).build()
      ChannelWithCreds(
        UnauthenticatedChannel(UriBuilder.fromUri(uri).userInfo(null).build()),
        Option(uri.getUserInfo))
    }
  }

  /** A channel that definitely does not have credentials set. */
  private[CondaEnvironment] case class UnauthenticatedChannel(uri: URI) {
    require(uri.getUserInfo == null)
  }

  private[this] def authenticateChannels(channels: Seq[UnauthenticatedChannel],
                                         userInfos: Map[UnauthenticatedChannel, String])
      : Seq[AuthenticatedChannel] = {
    channels.map { channel =>
      val authenticatedUrl = userInfos.get(channel)
        .map(userInfo => UriBuilder.fromUri(channel.uri).userInfo(userInfo).build().toString)
        .getOrElse(channel.uri.toString)
      AuthenticatedChannel(authenticatedUrl)
    }
  }

  /**
   * Helper method that strips and separates credentials from the given [[AuthenticatedChannel]]s.
   */
  private[this] def unauthenticateChannels(channels: Seq[AuthenticatedChannel])
      : ChannelsWithCreds = {
    val creds = Map.newBuilder[UnauthenticatedChannel, String]
    val unauthenticatedChannels = channels.map { channel =>
      val ChannelWithCreds(unauthed, userInfo) = channel.split()
      userInfo.foreach(creds += unauthed -> _)
      unauthed
    }
    ChannelsWithCreds(unauthenticatedChannels, creds.result())
  }

  /**
   * Channel credentials are separated from the channels in order for an executor to be re-usable
   * when only the credentials differ.
   * Note that only the first parameter list is used by implementations of toString, equals etc.
   */
  case class CondaSetupInstructions(
         packages: Seq[String],
         unauthenticatedChannels: Seq[UnauthenticatedChannel],
         extraArgs: Seq[String],
         envVars: Map[String, String])
        (userInfos: Map[UnauthenticatedChannel, String]) {
    require(unauthenticatedChannels.nonEmpty)
    require(packages.nonEmpty)

    /**
     * Channels with authentication applied.
     */
    def channels: Seq[String] = authenticateChannels(unauthenticatedChannels, userInfos).map(_.url)
  }

  object CondaSetupInstructions {
    def apply(packages: Seq[String], channels: Seq[AuthenticatedChannel], extraArgs: Seq[String],
              envVars: Map[String, String])
        : CondaSetupInstructions = {
      val ChannelsWithCreds(unauthed, userInfos) = unauthenticateChannels(channels)
      CondaSetupInstructions(packages, unauthed, extraArgs, envVars)(userInfos)
    }
  }
}
