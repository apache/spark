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

package org.apache.spark.deploy.yarn

import java.net.URI
import java.nio.ByteBuffer
import java.security.PrivilegedExceptionAction

import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.{FileStatus, FileSystem, Path}
import org.apache.hadoop.io.DataOutputBuffer
import org.apache.hadoop.net.NetUtils
import org.apache.hadoop.security.UserGroupInformation
import org.apache.hadoop.yarn.api._
import org.apache.hadoop.yarn.api.records._
import org.apache.hadoop.yarn.api.protocolrecords._
import org.apache.hadoop.yarn.conf.YarnConfiguration
import org.apache.hadoop.yarn.ipc.YarnRPC
import org.apache.hadoop.yarn.util.{Apps, ConverterUtils, Records, ProtoUtils}
import org.apache.hadoop.yarn.api.ApplicationConstants.Environment

import scala.collection.JavaConversions._
import scala.collection.mutable.HashMap

import org.apache.spark.Logging
import org.apache.spark.util.Utils

class WorkerRunnable(container: Container, conf: Configuration, masterAddress: String,
    slaveId: String, hostname: String, workerMemory: Int, workerCores: Int) 
    extends Runnable with Logging {
  
  var rpc: YarnRPC = YarnRPC.create(conf)
  var cm: ContainerManager = null
  val yarnConf: YarnConfiguration = new YarnConfiguration(conf)
  
  def run = {
    logInfo("Starting Worker Container")
    cm = connectToCM
    startContainer
  }
  
  def startContainer = {
    logInfo("Setting up ContainerLaunchContext")
    
    val ctx = Records.newRecord(classOf[ContainerLaunchContext])
      .asInstanceOf[ContainerLaunchContext]
    
    ctx.setContainerId(container.getId())
    ctx.setResource(container.getResource())
    val localResources = prepareLocalResources
    ctx.setLocalResources(localResources)
    
    val env = prepareEnvironment
    ctx.setEnvironment(env)
    
    // Extra options for the JVM
    var JAVA_OPTS = ""
    // Set the JVM memory
    val workerMemoryString = workerMemory + "m"
    JAVA_OPTS += "-Xms" + workerMemoryString + " -Xmx" + workerMemoryString + " "
    if (env.isDefinedAt("SPARK_JAVA_OPTS")) {
      JAVA_OPTS += env("SPARK_JAVA_OPTS") + " "
    }

    JAVA_OPTS += " -Djava.io.tmpdir=" + new Path(Environment.PWD.$(),
                                                 YarnConfiguration.DEFAULT_CONTAINER_TEMP_DIR)

    // Commenting it out for now - so that people can refer to the properties if required. Remove it once cpuset version is pushed out.
    // The context is, default gc for server class machines end up using all cores to do gc - hence if there are multiple containers in same
    // node, spark gc effects all other containers performance (which can also be other spark containers)
    // Instead of using this, rely on cpusets by YARN to enforce spark behaves 'properly' in multi-tenant environments. Not sure how default java gc behaves if it is
    // limited to subset of cores on a node.
/*
    else {
      // If no java_opts specified, default to using -XX:+CMSIncrementalMode
      // It might be possible that other modes/config is being done in SPARK_JAVA_OPTS, so we dont want to mess with it.
      // In our expts, using (default) throughput collector has severe perf ramnifications in multi-tennent machines
      // The options are based on
      // http://www.oracle.com/technetwork/java/gc-tuning-5-138395.html#0.0.0.%20When%20to%20Use%20the%20Concurrent%20Low%20Pause%20Collector|outline
      JAVA_OPTS += " -XX:+UseConcMarkSweepGC "
      JAVA_OPTS += " -XX:+CMSIncrementalMode "
      JAVA_OPTS += " -XX:+CMSIncrementalPacing "
      JAVA_OPTS += " -XX:CMSIncrementalDutyCycleMin=0 "
      JAVA_OPTS += " -XX:CMSIncrementalDutyCycle=10 "
    }
*/

    ctx.setUser(UserGroupInformation.getCurrentUser().getShortUserName())

    val credentials = UserGroupInformation.getCurrentUser().getCredentials()
    val dob = new DataOutputBuffer()
    credentials.writeTokenStorageToStream(dob)
    ctx.setContainerTokens(ByteBuffer.wrap(dob.getData()))

    var javaCommand = "java";
    val javaHome = System.getenv("JAVA_HOME")
    if ((javaHome != null && !javaHome.isEmpty()) || env.isDefinedAt("JAVA_HOME")) {
      javaCommand = Environment.JAVA_HOME.$() + "/bin/java"
    }

    val commands = List[String](javaCommand +
      " -server " +
      // Kill if OOM is raised - leverage yarn's failure handling to cause rescheduling.
      // Not killing the task leaves various aspects of the worker and (to some extent) the jvm in an inconsistent state.
      // TODO: If the OOM is not recoverable by rescheduling it on different node, then do 'something' to fail job ... akin to blacklisting trackers in mapred ?
      " -XX:OnOutOfMemoryError='kill %p' " +
      JAVA_OPTS +
      " org.apache.spark.executor.StandaloneExecutorBackend " +
      masterAddress + " " +
      slaveId + " " +
      hostname + " " +
      workerCores +
      " 1> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stdout" +
      " 2> " + ApplicationConstants.LOG_DIR_EXPANSION_VAR + "/stderr")
    logInfo("Setting up worker with commands: " + commands)
    ctx.setCommands(commands)
    
    // Send the start request to the ContainerManager
    val startReq = Records.newRecord(classOf[StartContainerRequest])
    .asInstanceOf[StartContainerRequest]
    startReq.setContainerLaunchContext(ctx)
    cm.startContainer(startReq)
  }
  
  
  def prepareLocalResources: HashMap[String, LocalResource] = {
    logInfo("Preparing Local resources")
    val locaResources = HashMap[String, LocalResource]()
    
    // Spark JAR
    val sparkJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    sparkJarResource.setType(LocalResourceType.FILE)
    sparkJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    sparkJarResource.setResource(ConverterUtils.getYarnUrlFromURI(
      new URI(System.getenv("SPARK_YARN_JAR_PATH"))))
    sparkJarResource.setTimestamp(System.getenv("SPARK_YARN_JAR_TIMESTAMP").toLong)
    sparkJarResource.setSize(System.getenv("SPARK_YARN_JAR_SIZE").toLong)
    locaResources("spark.jar") = sparkJarResource
    // User JAR
    val userJarResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
    userJarResource.setType(LocalResourceType.FILE)
    userJarResource.setVisibility(LocalResourceVisibility.APPLICATION)
    userJarResource.setResource(ConverterUtils.getYarnUrlFromURI(
      new URI(System.getenv("SPARK_YARN_USERJAR_PATH"))))
    userJarResource.setTimestamp(System.getenv("SPARK_YARN_USERJAR_TIMESTAMP").toLong)
    userJarResource.setSize(System.getenv("SPARK_YARN_USERJAR_SIZE").toLong)
    locaResources("app.jar") = userJarResource

    // Log4j conf - if available
    if (System.getenv("SPARK_YARN_LOG4J_PATH") != null) {
      val log4jConfResource = Records.newRecord(classOf[LocalResource]).asInstanceOf[LocalResource]
      log4jConfResource.setType(LocalResourceType.FILE)
      log4jConfResource.setVisibility(LocalResourceVisibility.APPLICATION)
      log4jConfResource.setResource(ConverterUtils.getYarnUrlFromURI(
        new URI(System.getenv("SPARK_YARN_LOG4J_PATH"))))
      log4jConfResource.setTimestamp(System.getenv("SPARK_YARN_LOG4J_TIMESTAMP").toLong)
      log4jConfResource.setSize(System.getenv("SPARK_YARN_LOG4J_SIZE").toLong)
      locaResources("log4j.properties") = log4jConfResource
    }

    
    logInfo("Prepared Local resources " + locaResources)
    return locaResources
  }
  
  def prepareEnvironment: HashMap[String, String] = {
    val env = new HashMap[String, String]()

    // If log4j present, ensure ours overrides all others
    if (System.getenv("SPARK_YARN_LOG4J_PATH") != null) {
      // Which is correct ?
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./log4j.properties")
      Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./")
    }

    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "./*")
    Apps.addToEnvironment(env, Environment.CLASSPATH.name, "$CLASSPATH")
    Client.populateHadoopClasspath(yarnConf, env)

    // allow users to specify some environment variables
    Apps.setEnvFromInputString(env, System.getenv("SPARK_YARN_USER_ENV"))

    System.getenv().filterKeys(_.startsWith("SPARK")).foreach { case (k,v) => env(k) = v }
    return env
  }
  
  def connectToCM: ContainerManager = {
    val cmHostPortStr = container.getNodeId().getHost() + ":" + container.getNodeId().getPort()
    val cmAddress = NetUtils.createSocketAddr(cmHostPortStr)
    logInfo("Connecting to ContainerManager at " + cmHostPortStr)

    // use doAs and remoteUser here so we can add the container token and not 
    // pollute the current users credentials with all of the individual container tokens
    val user = UserGroupInformation.createRemoteUser(container.getId().toString());
    val containerToken = container.getContainerToken();
    if (containerToken != null) {
      user.addToken(ProtoUtils.convertFromProtoFormat(containerToken, cmAddress))
    }

    val proxy = user
        .doAs(new PrivilegedExceptionAction[ContainerManager] {
          def run: ContainerManager = {
            return rpc.getProxy(classOf[ContainerManager],
                cmAddress, conf).asInstanceOf[ContainerManager]
          }
        });
    return proxy;
  }
  
}
