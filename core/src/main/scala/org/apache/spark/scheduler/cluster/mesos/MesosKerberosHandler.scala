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

package org.apache.spark.scheduler.cluster.mesos

import java.io.{ByteArrayInputStream, DataInputStream}
import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions
import java.security.PrivilegedExceptionAction
import javax.xml.bind.DatatypeConverter

import scala.collection.JavaConverters._

import org.apache.hadoop.fs.Path
import org.apache.hadoop.hdfs.security.token.delegation.DelegationTokenIdentifier
import org.apache.hadoop.security.{Credentials, UserGroupInformation}

import org.apache.spark.{SparkConf, SparkException}
import org.apache.spark.deploy.SparkHadoopUtil
import org.apache.spark.internal.Logging


/**
 * DelegationTokenBroadcaster is a callback interface to broadcast new tokens
 * to the executors.
 */
trait DelegationTokenBroadcaster {
  def broadcastDelegationTokens(tokens: Array[Byte]): Unit
}

/**
 * MesosKerberosHandler implements the Kerberos logic for Mesos
 */
private[spark]
class MesosKerberosHandler(conf: SparkConf,
    principal: String,
    broadcaster: DelegationTokenBroadcaster)
  extends Object with Logging {

  @volatile private var renewalCredentials: Credentials = null
  @volatile private var stopRenewal = false
  var renewalThread: Thread = null

  def start(): Unit = {
    logInfo("Starting delegation token renewer")
    renewalThread = new Thread(new Runnable {
      def run() {
        renewLoop()
      }
    })
    renewalThread.start()
  }

  def stop(): Unit = {
    logWarning("Stopping delegation token renewer")
    stopRenewal = true
    if (renewalThread != null) {
      renewalThread.interrupt()
    }
  }

  def createHDFSDelegationTokens: Array[Byte] = {
    // get keytab or tgt, and login
    val keytab64 = conf.get("spark.mesos.kerberos.keytabBase64", null)
    val tgt64 = conf.get("spark.mesos.kerberos.tgtBase64", null)
    require(keytab64 != null || tgt64 != null, "keytab or tgt required")
    require(keytab64 == null || tgt64 == null, "keytab and tgt cannot be used at the same time")

    val mode = if (keytab64 != null) "keytab" else "tgt"
    logInfo(s"Logging in as $principal with $mode to retrieve HDFS delegation tokens")

    // write keytab or tgt into a temporary file
    val bytes = DatatypeConverter.parseBase64Binary(if (keytab64 != null) keytab64 else tgt64)
    val kerberosSecretFile = Files.createTempFile("spark-mesos-kerberos-token", ".tmp",
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
    kerberosSecretFile.toFile.deleteOnExit() // just to be sure
    Files.write(kerberosSecretFile, bytes)

    // login
    try {
      // login with _new_ user in order to start without any token (necessary to make sure that
      // new tokens are really downloaded, even when not yet expired)
      val ugi = if (keytab64 != null) {
        UserGroupInformation.loginUserFromKeytabAndReturnUGI(principal, kerberosSecretFile.toString)
      }
      else {
        UserGroupInformation.getUGIFromTicketCache(kerberosSecretFile.toString, principal)
      }

      // get tokens
      val ugiCreds = getHDFSDelegationTokens(ugi)
      logInfo(s"Got ${ugiCreds.numberOfTokens()} HDFS delegation tokens")

      // write tokens into a memory file to transfer it to the executors
      val tokenBuf = new java.io.ByteArrayOutputStream(1024 * 1024)
      ugiCreds.writeTokenStorageToStream(new java.io.DataOutputStream(tokenBuf))
      logDebug(s"Wrote ${tokenBuf.size()} bytes of token data")

      // store the renewal credentials, needed to get the waiting time for
      // the next renewal
      renewalCredentials = ugiCreds

      // make new ugi active
      UserGroupInformation.setLoginUser(ugi)

      tokenBuf.toByteArray
    }
    finally {
      kerberosSecretFile.toFile.delete()
    }
  }

  private def getHDFSDelegationTokens(ugi: UserGroupInformation): Credentials = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    val namenodes = Option(hadoopConf.get("dfs.ha.namenodes.hdfs", null)).
      map(_.split(",")).getOrElse(Array[String]()).
      flatMap(id => Option(hadoopConf.get(s"dfs.namenode.rpc-address.hdfs.$id", null))).
      map(hostPort => new Path(s"hdfs://$hostPort")).
      toSet
    logInfo(s"Found these HDFS namenodes: $namenodes")
    val ugiCreds = ugi.getCredentials
    ugi.doAs(new PrivilegedExceptionAction[Unit] {
      override def run() = {
        // use the job principal itself to renew the tokens
        SparkHadoopUtil.get.obtainTokensForNamenodes(
          namenodes, hadoopConf, ugiCreds, Some(principal)
        )
      }
    })
    ugiCreds
  }

  private def renewLoop(): Unit = {
    while (!stopRenewal) {
      try {
        val msLeft = getHDFSTokenRenewalInterval(renewalCredentials)
        val msWait = Math.max(msLeft / 2, 30 * 1000)
        logInfo(s"Waiting ${msWait / 1000} seconds until delegation token renewal")
        Thread.sleep(msWait)

        val tokens = createHDFSDelegationTokens
        broadcaster.broadcastDelegationTokens(tokens)
      }
      catch {
        case e: SparkNoDelegationTokenException =>
          logError(s"Stopping delegation token renewal due to: $e")
          return
        case e: InterruptedException =>
          return
        case e: Exception =>
          logError(s"Exception during token renewal: $e")
          Thread.sleep(10000)
      }
    }
  }

  private def getHDFSTokenRenewalInterval(creds: Credentials): Long = {
    val hadoopConf = SparkHadoopUtil.get.newConfiguration(conf)
    // filter for HDFS delgation tokens. There might be others, but our renewal only
    // supports HDFS for the moment.
    val ts = creds.getAllTokens.asScala
      .filter(_.getKind == DelegationTokenIdentifier.HDFS_DELEGATION_KIND)
    if (ts.isEmpty) {
      throw new SparkNoDelegationTokenException
    }
    val intervals = ts.map(t => {
      val newExpiration = t.renew(hadoopConf)
      val identifier = new DelegationTokenIdentifier()
      identifier.readFields(new DataInputStream(new ByteArrayInputStream(t.getIdentifier)))
      newExpiration - identifier.getIssueDate
    })
    intervals.min
  }
}

private[spark] case class SparkNoDelegationTokenException()
  extends SparkException(s"No delegation token to renew")
