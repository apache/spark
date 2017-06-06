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

package org.apache.spark.scheduler.cluster.nomad

import java.net.URI
import java.util
import java.util.Collections.singletonList

import scala.collection.JavaConverters._

import com.hashicorp.nomad.apimodel._
import org.apache.commons.io.FilenameUtils
import org.apache.commons.lang3.StringUtils

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry}
import org.apache.spark.network.util.ByteUnit
import org.apache.spark.scheduler.cluster.nomad.SparkNomadJob.{applyConf, JOB_TEMPLATE, SPARK_DISTRIBUTION}
import org.apache.spark.util.Utils

/**
 * Defines configuration for a type of Nomad task that is created by Spark.
 */
private[spark] abstract class SparkNomadTaskType(
    val role: String,
    val fullName: String,
    memoryConfigEntry: ConfigEntry[Long]) {

  val CPU =
    ConfigBuilder(s"spark.nomad.$role.cpu")
      .doc(s"How many MHz of CPU power Nomad should reserve for $fullName tasks")
      .intConf
      .createWithDefault(1000)

  val MEMORY_OVERHEAD =
    ConfigBuilder(s"spark.nomad.$role.memoryOverhead")
      .doc("The amount of memory that should be allocated to the nomad task above the heap " +
        s"memory specified by ${memoryConfigEntry.key}")
      .bytesConf(ByteUnit.MiB)
      .createOptional

  val NETWORK_MBITS =
    ConfigBuilder(s"spark.nomad.$role.networkMBits")
      .doc(s"The network bandwidth Nomad should allocate to $fullName tasks during bin packing")
      .intConf
      .createWithDefault(1)

  val LOG_MAX_FILES =
    ConfigBuilder(s"spark.nomad.$role.logMaxFiles")
      .doc(s"Number of log files Nomad should keep from $fullName tasks")
      .intConf
      .createWithDefault(5)

  val LOG_MAX_FILE_SIZE_MIB =
    ConfigBuilder(s"spark.nomad.$role.logMaxFileSize")
      .doc(s"Maximum size that Nomad should keep in log files from $fullName tasks")
      .bytesConf(ByteUnit.MiB)
      .createWithDefaultString("1m")

  val MEMORY_OVERHEAD_FACTOR = 0.10
  val MEMORY_OVERHEAD_MIN = 384L

  case class ConfigurablePort(label: String) {
    def placeholder: String = s"$${NOMAD_PORT_$label}"
    def ipPlaceholder: String = s"$${NOMAD_IP_$label}"
    def placeholderInSiblingTasks(task: Task): String = s"$${NOMAD_PORT_${task.getName}_$label}"

    override def toString: String = placeholder
  }

  def newBlankTask(): Task =
    new Task().addMeta(SparkNomadTaskType.ROLE_META_KEY, role)

  def isTypeOf(task: Task): Boolean =
    SparkNomadTaskType.roleOf(task).contains(role)

  protected def configure(
      jobConf: SparkNomadJob.CommonConf,
      conf: SparkConf,
      task: Task,
      ports: Seq[ConfigurablePort],
      sparkCommand: String
  ): Unit = {

    configureResources(conf, task, ports)

    if (StringUtils.isEmpty(task.getName)) {
      task.setName(fullName.replaceAll(" ", "-"))
    }

    def setConfigIfMissing(key: String)(value: => String) =
      if (Option(task.getConfig).flatMap(config => Option(config.get(key))).isEmpty) {
        task.addConfig(key, value)
      }

    def configureDocker(): Unit = {
      jobConf.dockerImage.foreach(setConfigIfMissing("image")(_))
      setConfigIfMissing("network_mode")("host")
      setConfigIfMissing("work_dir")("/local")
      jobConf.dockerAuth.foreach { properties =>
        task.getConfig.get("auth") match {
          case null =>
            task.addConfig("auth", singletonList(properties.asJava))
          case authList: java.util.List[_] if authList.isEmpty =>
            task.addConfig("auth", singletonList(properties.asJava))
          case authList: java.util.List[_] =>
            val auth = authList.get(0).asInstanceOf[java.util.Map[String, AnyRef]]
            properties.foreach { case (k, v) =>
              auth.put(k, v)
            }
        }
      }
    }

    task.getDriver match {
      case "docker" => configureDocker()
      case null | "" if jobConf.dockerImage.isDefined =>
        task.setDriver("docker")
        configureDocker()
      case null | "" =>
        task.setDriver("exec")

        if (task.getConstraints == null ||
          task.getConstraints.asScala.forall(_.getLTarget != "driver.java.version")) {

          task.addConstraints(
            new Constraint()
              .setLTarget("driver.java.version")
              .setOperand(">=")
              .setRTarget("1.8.0")
          )
        }
      case _ =>
    }

    if (Option(task.getEnv).flatMap(env => Option(env.get("SPARK_LOCAL_IP"))).isEmpty) {
      task.addEnv("SPARK_LOCAL_IP", ports.head.ipPlaceholder)
    }

    setConfigIfMissing("command") {
      val sparkDistributionUrl = jobConf.sparkDistribution.getOrElse {
        throw new SparkException(
          s"Don't know where to find spark for $fullName task. " +
            s"You must either set ${SPARK_DISTRIBUTION.key} or provide a ${JOB_TEMPLATE.key} " +
            s"""with a command provided for the task with meta spark.nomad.role = "$role"""")
      }
      val (sparkHomeUrl, sparkArtifact) = asFileAndArtifact(jobConf, sparkDistributionUrl)
      sparkArtifact.foreach(task.addArtifacts(_))
      val sparkDir =
        if (sparkDistributionUrl.getScheme == "local") sparkHomeUrl.getPath
        else FilenameUtils.removeExtension(sparkHomeUrl.getPath)
      s"$sparkDir/bin/$sparkCommand"
    }
  }

  protected def appendArguments(
      task: Task,
      arguments: Seq[String],
      removeOld: Boolean = false
  ): Unit = {
    Option(task.getConfig).flatMap(opts => Option(opts.get("args"))) match {
      case None => task.addConfig("args", new util.ArrayList(arguments.asJava))
      case Some(javaArgs) =>
        val args = javaArgs.asInstanceOf[java.util.List[String]].asScala
        if (removeOld) {
          args.remove(args.length - arguments.length, arguments.length)
        }
        args.appendAll(arguments)
    }
  }

  protected def asFileIn(jobConf: SparkNomadJob.CommonConf, task: Task)(url: String): String = {
    val (file, artifact) = asFileAndArtifact(jobConf, new URI(url))
    artifact.foreach(task.addArtifacts(_))
    file.toString
  }

  protected def jvmMemory(conf: SparkConf, task: Task): String = {
    val megabytes: Long = conf.getOption(memoryConfigEntry.key) match {
      case Some(stringValue) =>
        memoryConfigEntry.valueConverter(stringValue)
      case None =>
        val memoryWithOverhead = task.getResources.getMemoryMb
        val overhead = conf.get(MEMORY_OVERHEAD).getOrElse(
          math.max(
            (memoryWithOverhead / (1 + 1 / MEMORY_OVERHEAD_FACTOR)).toLong,
            MEMORY_OVERHEAD_MIN
          )
        )
        memoryWithOverhead - overhead
    }
    megabytes + "m"
  }

  private def configureResources(
      conf: SparkConf, task: Task, ports: Seq[ConfigurablePort]): Unit = {

    val resources = Option(task.getResources).getOrElse {
      val r = new Resources
      task.setResources(r)
      r
    }

    applyConf(conf, CPU, resources.getCpu)(resources.setCpu(_))

    (conf.getOption(memoryConfigEntry.key), conf.get(MEMORY_OVERHEAD)) match {
      case (Some(explicitMemoryString), Some(explicitOverhead)) =>
        resources.setMemoryMb(
          (memoryConfigEntry.valueConverter(explicitMemoryString) + explicitOverhead).toInt)
      case _ if resources.getMemoryMb != null =>
        // use the value that is already set
      case (_, overheadOpt) =>
        val memory = conf.get(memoryConfigEntry)
        val overhead = overheadOpt.getOrElse(
          math.max(
            (MEMORY_OVERHEAD_FACTOR * memory).toLong,
            MEMORY_OVERHEAD_MIN
          )
        )
        resources.setMemoryMb((memory + overhead).toInt)
    }

    val network = Option(resources.getNetworks).flatMap(_.asScala.headOption).getOrElse {
      val n = new NetworkResource
      resources.addNetworks(n)
      n
    }
    applyConf(conf, NETWORK_MBITS, network.getMBits)(network.setMBits(_))

    ports.foreach { port =>
      def alreadyContainsPort(ports: java.util.List[Port]): Boolean =
        Option(ports).exists(_.asScala.exists(_.getLabel == port.label))

      if (!alreadyContainsPort(network.getDynamicPorts)
        && !alreadyContainsPort(network.getReservedPorts)
      ) {
        network.addDynamicPorts(new Port().setLabel(port.label))
      }
    }

    val logConfig = Option(task.getLogConfig).getOrElse {
      val l = new LogConfig
      task.setLogConfig(l)
      l
    }
    applyConf(conf, LOG_MAX_FILES, logConfig.getMaxFiles)(logConfig.setMaxFiles(_))
    applyConf(conf, LOG_MAX_FILE_SIZE_MIB, logConfig.getMaxFileSizeMb)(s =>
      logConfig.setMaxFileSizeMb(s.toInt)
    )
  }

  private def asFileAndArtifact(
      jobConf: SparkNomadJob.CommonConf,
      url: URI
  ): (URI, Option[TaskArtifact]) = {
    url.getScheme match {
      case "local" => url -> None
      case null | "file" =>
        throw new SparkException(
          "Bare paths and the \"file:\" URL scheme are not supported in Nomad cluster mode. " +
            "Use a remote (e.g. HTTP/S) URL that nomad can download, or use a \"local:\" URL " +
            "if the resource is available at a well-known path on all nomad clients that might " +
            "run the task. Offending path/URL: " + url
        )
      case "http" | "https" | "s3" =>
        val workDir =
          if (jobConf.dockerImage.isDefined) "/local/"
          else "/"
        val file = new URI("file://" + workDir + Utils.decodeFileNameInURI(url))
        val artifact = new TaskArtifact()
          .setRelativeDest(workDir)
          .setGetterSource(url.toString)
        file -> Some(artifact)
    }
  }

}

private[spark] object SparkNomadTaskType {

  val ROLE_META_KEY = "spark.nomad.role"

  def roleOf(task: Task): Option[String] =
    task.getMeta match {
      case null => None
      case meta => Option(meta.get(SparkNomadTaskType.ROLE_META_KEY))
    }

}
