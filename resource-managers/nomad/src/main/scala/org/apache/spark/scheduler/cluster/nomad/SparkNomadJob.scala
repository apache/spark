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

import java.io.File
import java.math.BigInteger
import java.net.URI
import java.nio.charset.StandardCharsets.UTF_8
import java.util.Date

import scala.collection.JavaConverters._

import com.google.common.io.Files
import com.hashicorp.nomad.apimodel.{Job, Task, TaskGroup}
import com.hashicorp.nomad.javasdk.NomadJson
import org.apache.commons.lang3.time.DateFormatUtils.formatUTC
import org.apache.http.HttpHost

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.nomad.ApplicationRunCommand
import org.apache.spark.internal.Logging
import org.apache.spark.internal.config.{ConfigBuilder, ConfigEntry, OptionalConfigEntry}

private[spark] object SparkNomadJob extends Logging {

  val REGION =
    ConfigBuilder("spark.nomad.region")
      .doc("The Nomad region to use (defaults to the region of the first Nomad server contacted)")
      .stringConf
      .createOptional

  val DATACENTERS =
    ConfigBuilder("spark.nomad.datacenters")
      .doc("Comma-separated list of Nomad datacenters to use" +
        " (defaults to the datacenter of the first Nomad server contacted)")
      .stringConf
      .createWithDefault("dc1")

  val NOMAD_JOB_PRIORITY =
    ConfigBuilder("spark.nomad.priority")
      .doc("The priority of the Nomad job that runs the application or its executors")
      .intConf
      .createWithDefault(40)

  val JOB_TEMPLATE =
    ConfigBuilder("spark.nomad.job.template")
      .doc("The path to a JSON file containing a Nomad job to use as a template")
      .stringConf
      .createOptional

  val DOCKER_IMAGE =
    ConfigBuilder("spark.nomad.dockerImage")
      .doc("A [docker image](https://www.nomadproject.io/docs/drivers/docker.html#image) " +
        "to use to run Spark with Nomad's `docker` driver. " +
        "When not specified, Nomad's `exec` driver will be used instead.")
      .stringConf
      .createOptional

  val DOCKER_AUTH_USERNAME =
    ConfigBuilder("spark.nomad.docker.username")
      .doc(s"Username used when downloading the docker image specified by `${DOCKER_IMAGE.key}` " +
        "from the docker registry. " +
        "(https://www.nomadproject.io/docs/drivers/docker.html#authentication) ")
      .stringConf
      .createOptional

  val DOCKER_AUTH_PASSWORD =
    ConfigBuilder("spark.nomad.docker.password")
      .doc(s"Password used when downloading the docker image specified by `${DOCKER_IMAGE.key}` " +
        "from the docker registry. " +
        "(https://www.nomadproject.io/docs/drivers/docker.html#authentication) ")
      .stringConf
      .createOptional

  val DOCKER_AUTH_EMAIL =
    ConfigBuilder("spark.nomad.docker.email")
      .doc("Email address used when downloading the docker image specified by " +
        s"`${DOCKER_IMAGE.key}` from the docker registry. " +
        "(https://www.nomadproject.io/docs/drivers/docker.html#authentication) ")
      .stringConf
      .createOptional

  val DOCKER_AUTH_SERVER_ADDRESS =
    ConfigBuilder("spark.nomad.docker.serverAddress")
      .doc("Server address (domain/IP without the protocol) used when downloading the docker " +
        s"image specified by `${DOCKER_IMAGE.key}` from the docker registry. " +
        "Docker Hub is used by default." +
        "(https://www.nomadproject.io/docs/drivers/docker.html#authentication) ")
      .stringConf
      .createOptional

  val SPARK_DISTRIBUTION =
    ConfigBuilder("spark.nomad.sparkDistribution")
      .doc("The location of the spark distribution tgz file to use. " +
        "If this isn't set, commands for all spark tasks must be explicitly set in the jobspec " +
        s"at `${SparkNomadJob.JOB_TEMPLATE.key}`")
      .stringConf
      .createOptional

  val UTC_TIMESTAMP_FORMAT = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'"

  val SPARK_NOMAD_CLUSTER_MODE = "spark.nomad.clusterMode"

  case class CommonConf(
      appId: String,
      appName: String,
      dockerImage: Option[String],
      dockerAuth: Option[Map[String, String]],
      sparkDistribution: Option[URI]
  )

  object CommonConf {
    def apply(conf: SparkConf): CommonConf = {
      val appName = conf.get("spark.app.name")
      CommonConf(
        appName = appName,
        appId = conf.getOption("spark.app.id") match {
          case None => s"${appName.replaceAll(" ", "_")}-$utcTimestamp"
          case Some(id) =>
            if (id.contains(" ")) {
              throw new SparkException(
                "spark.app.id cannot contain a space when using a Nomad master")
            }
            id
        },
        dockerImage = conf.get(DOCKER_IMAGE),
        dockerAuth = {
          val entries = Seq(
            conf.get(DOCKER_AUTH_USERNAME).map("username" -> _),
            conf.get(DOCKER_AUTH_PASSWORD).map("Password" -> _),
            conf.get(DOCKER_AUTH_EMAIL).map("email" -> _),
            conf.get(DOCKER_AUTH_SERVER_ADDRESS).map("server_address" -> _)
          ).flatten
          if (entries.nonEmpty) Some(entries.toMap) else None
        },
        sparkDistribution = conf.get(SPARK_DISTRIBUTION).map(new URI(_))
      )
    }
  }

  def apply(
      conf: SparkConf,
      nomadUrl: Option[HttpHost],
      driverCommand: Option[ApplicationRunCommand]
  ): Job = {

    val job = conf.get(JOB_TEMPLATE) match {
      case None =>
        logInfo("Creating job without a template")
        new Job()
      case Some(file) =>
        logInfo("Creating job from provided template")
        val job = NomadJson.readJobSpec(Files.toString(new File(file), UTF_8))
        if (job == null) {
          throw new SparkException(s"$file did not contain a valid JSON job specification")
        }
        job
    }

    val jobConf = CommonConf(conf)

    job
      .setId(jobConf.appId)
      .setName(jobConf.appName)
      .addMeta("spark.nomad.role", "application")
      .setJobModifyIndex(BigInteger.ZERO)

    applyDefault(job.getType)(job.setType("batch"))
    applyConf(conf, NOMAD_JOB_PRIORITY, job.getPriority)(job.setPriority(_))
    applyConf(conf, REGION, job.getRegion)(job.setRegion)

    def asJavaList(commaSeparated: String): java.util.List[String] =
      commaSeparated.split(",").toSeq
        .map(_.trim)
        .filter(_.nonEmpty)
        .asJava

    conf.getOption(DATACENTERS.key) match {
      case Some(explicit) => job.setDatacenters(asJavaList(explicit))
      case None if job.getDatacenters == null => DATACENTERS.defaultValue
        .foreach(dcs => job.setDatacenters(asJavaList(dcs)))
    }

    logInfo(
      s"Will run as Nomad job [${job.getId}]" +
        s" with priority ${job.getPriority}" +
        s" in datacenter(s) [${job.getDatacenters.asScala.mkString(",")}]" +
        Option(job.getRegion).fold("")(r => s" in region [$r]")
    )

    val driverTemplate = find(job, DriverTaskGroup)
    driverCommand match {
      case Some(command) =>
        val group = driverTemplate.getOrElse {
          val g = new TaskGroup()
          job.addTaskGroups(g)
          g
        }
        val parameters = DriverTask.Parameters(command, nomadUrl)
        DriverTaskGroup.configure(jobConf, conf, group, parameters)
      case None =>
        driverTemplate.foreach(job.getTaskGroups.remove)
    }

    val group = find(job, ExecutorTaskGroup).getOrElse {
      val g = new TaskGroup()
      job.addTaskGroups(g)
      g
    }
    ExecutorTaskGroup.configure(jobConf, conf, group, false)

    job
  }

  def utcTimestamp: String =
    formatUTC(new Date(), UTC_TIMESTAMP_FORMAT)

  def applyConf[A](
      conf: SparkConf,
      entry: OptionalConfigEntry[A],
      getter: => AnyRef
  )(
      setter: A => Any
  ): Unit = {
    conf.get(entry).foreach(setter)
  }

  def applyConf[A](
      conf: SparkConf,
      entry: ConfigEntry[A],
      getter: => AnyRef
  )(
      setter: A => Any
  ): Unit = {
    conf.getOption(entry.key) match {
      case Some(explicit) =>
        setter(entry.valueConverter(explicit))
      case None =>
        if (getter == null) {
          entry.defaultValue.foreach(setter)
        }
    }
  }

  def find(job: Job, groupType: SparkNomadTaskGroupType): Option[TaskGroup] = {
    val expectedRoles = groupType.tasks.map(_.role).toSet
    val groups = Option(job.getTaskGroups).map(_.asScala).getOrElse(Nil)
      .filter(g => g.getTasks != null && g.getTasks.asScala
        .exists(taskMustBelongToGroup(groupType, _))
      )
    groups match {
      case Seq() => None
      case Seq(group) =>
        val tasks = group.getTasks.asScala
        val tasksWithUnexpectedRoles = tasks
          .filter(!taskMustBelongToGroup(groupType, _))
          .flatMap(t => SparkNomadTaskType.roleOf(t).map(r => s"task $t with role $r"))
        if (tasksWithUnexpectedRoles.nonEmpty) {
          throw new SparkException(
            s"Tasks with roles in $expectedRoles can be mixed with role-less tasks, " +
              s"but not with tasks with other roles. Task group ${group.getName} " +
              s"has tasks with these roles mixed with roles $tasksWithUnexpectedRoles.")
        }
        groupType.tasks.foreach { taskType =>
          val tasksWithThisType = tasks.filter(taskType.isTypeOf)
          if (tasksWithThisType.size > 1) {
            val taskNames = tasksWithThisType.map(_.getName)
            throw new SparkException(
              s"There should be at most one template task with the ${taskType.role} role, " +
                s"but task group ${group.getName} has multiple tasks with this role: $taskNames")
          }
        }
        Some(group)
      case _ =>
        throw new SparkException(
          s"All tasks with roles in $expectedRoles should be in a single task group, " +
            s"but they are split over the following groups: ${groups.map(_.getName)}"
        )
    }
  }

  private def applyDefault[A](getter: => AnyRef)(set: => Unit): Unit =
    if (getter == null) {
      set
    }

  private def taskMustBelongToGroup(groupType: SparkNomadTaskGroupType, task: Task): Boolean =
    groupType.tasks.exists(_.isTypeOf(task))

}
