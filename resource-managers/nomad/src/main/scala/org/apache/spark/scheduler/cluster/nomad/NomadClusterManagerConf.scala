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

import com.hashicorp.nomad.apimodel.Job
import com.hashicorp.nomad.javasdk.NomadApiConfiguration
import com.hashicorp.nomad.javasdk.NomadApiConfiguration.nomadAddressAsHttpHost
import org.apache.http.HttpHost

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.nomad.ApplicationRunCommand
import org.apache.spark.internal.config._
import org.apache.spark.scheduler.cluster.nomad.NomadClusterManagerConf._

/**
 * Configuration for running a Spark application on a Nomad cluster.
 *
 * This contains all the configuration used by [[NomadClusterSchedulerBackend]],
 * other than configuration that is simply passed through to executors.
 *
 * By extracting all of the configuration as early as possible,
 * we can fail fast in the face of configuration errors,
 * rather than encountering them after we've already started interacting with Nomad.
 */
private[spark] case class NomadClusterManagerConf(
    jobDescriptor: JobDescriptor,
    nomadApi: NomadApiConfiguration,
    staticExecutorInstances: Option[Int]
)

private[spark] object NomadClusterManagerConf {

  val TLS_CA_CERT =
    ConfigBuilder("spark.nomad.tls.caCert")
      .doc("Path to a .pem file containing the certificate authority to validate the Nomad " +
        "server's TLS certificate against")
      .stringConf
      .createOptional

  val TLS_CERT =
    ConfigBuilder("spark.nomad.tls.cert")
      .doc("Path to a .pem file containing the TLS certificate to present to the Nomad server")
      .stringConf
      .createOptional

  val TLS_KEY =
    ConfigBuilder("spark.nomad.tls.trustStorePassword")
      .doc("Path to a .pem file containing the private key corresponding to the certificate in " +
        TLS_CERT.key)
      .stringConf
      .createOptional

  val DEFAULT_EXECUTOR_INSTANCES = 2

  sealed trait JobDescriptor {
    def id: String
    def region: Option[String]
  }

  case class ExistingJob(
      override val id: String,
      override val region: Option[String]
  ) extends JobDescriptor

  case class NewJob(job: Job) extends JobDescriptor {
    override def id: String = job.getId
    override def region: Option[String] = Option(job.getRegion)
  }

  case class KeyPair(certFile: String, keyFile: String)

  object KeyPair {
    def apply(conf: SparkConf,
        certEntry: ConfigEntry[Option[String]],
        keyEntry: ConfigEntry[Option[String]]): Option[KeyPair] =
      (conf.get(certEntry), conf.get(keyEntry)) match {
        case (Some(cert), Some(file)) => Some(KeyPair(cert, file))
        case (None, None) => None
        case _ => throw new SparkException(
          s"You can either provide both ${certEntry.key} and ${keyEntry.key}, " +
            "or neither of them, but you can't provide one an not the other."
        )
      }
  }

  def extractNomadUrl(conf: SparkConf): Option[HttpHost] = (conf.get("spark.master") match {
    case "nomad" => sys.env.get("NOMAD_ADDR").map(_.trim).filter(_.nonEmpty)
    case master if master.startsWith("nomad:") => Some(master.stripPrefix("nomad:"))
    case invalid => throw new SparkException(
      "Nomad master can either be \"nomad:\" followed by an explicit HTTP or HTTPS URL " +
        "(e.g. nomad:http://nomad.example.com), or simply \"nomad\" which signals that " +
        "the NOMAD_ADDR environment variable should be used if set and falls back to  " +
        "http://localhost:4646"
    )
  }).map(nomadAddressAsHttpHost)

  def extractApiConf(
      nomadUrl: Option[HttpHost],
      region: Option[String],
      conf: SparkConf
  ): NomadApiConfiguration = {
    val builder = new NomadApiConfiguration.Builder()

    builder.setFromEnvironmentVariables(System.getenv())

    nomadUrl.foreach(address => builder.setAddress(address))

    conf.get(TLS_CA_CERT).foreach(builder.setTlsCaFile)
    KeyPair.apply(conf, TLS_CERT, TLS_KEY)
      .foreach(p => builder.setTlsCertAndKeyFiles(p.certFile, p.keyFile))

    region.foreach(builder.setRegion)

    builder.build()
  }

  def apply(
      conf: SparkConf,
      command: Option[ApplicationRunCommand]
  ): NomadClusterManagerConf = {

    val nomadUrl = extractNomadUrl(conf)

    val jobDescriptor =
      if (conf.getBoolean(SparkNomadJob.SPARK_NOMAD_CLUSTER_MODE, defaultValue = false)) {
        val jobConf = SparkNomadJob.CommonConf(conf)
        ExistingJob(jobConf.appId, conf.get(SparkNomadJob.REGION))
      } else {
        NewJob(SparkNomadJob(conf, nomadUrl, command))
    }

    NomadClusterManagerConf(
      jobDescriptor = jobDescriptor,
      nomadApi = extractApiConf(nomadUrl, jobDescriptor.region, conf),
      staticExecutorInstances = conf.get(EXECUTOR_INSTANCES)
    )
  }

}
