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

package org.apache.spark.api.r

import java.io.{ByteArrayInputStream, ByteArrayOutputStream, DataInputStream, DataOutputStream}
import java.util.concurrent.TimeUnit

import scala.language.existentials

import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}
import io.netty.channel.ChannelHandler.Sharable
import io.netty.handler.timeout.ReadTimeoutException

import org.apache.spark.api.r.SerDe._
import org.apache.spark.internal.Logging
import org.apache.spark.SparkConf
import org.apache.spark.util.{ThreadUtils, Utils}

/**
 * Handler for RBackend
 * TODO: This is marked as sharable to get a handle to RBackend. Is it safe to re-use
 * this across connections ?
 */
@Sharable
private[r] class RBackendHandler(server: RBackend)
  extends SimpleChannelInboundHandler[Array[Byte]] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]): Unit = {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // First bit is isStatic
    val isStatic = readBoolean(dis)
    val objId = readString(dis)
    val methodName = readString(dis)
    val numArgs = readInt(dis)

    if (objId == "SparkRHandler") {
      methodName match {
        // This function is for test-purpose only
        case "echo" =>
          val args = readArgs(numArgs, dis)
          assert(numArgs == 1)

          writeInt(dos, 0)
          writeObject(dos, args(0), server.jvmObjectTracker)
        case "stopBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()
        case "rm" =>
          try {
            val t = readObjectType(dis)
            assert(t == 'c')
            val objToRemove = readString(dis)
            server.jvmObjectTracker.remove(JVMObjectId(objToRemove))
            writeInt(dos, 0)
            writeObject(dos, null, server.jvmObjectTracker)
          } catch {
            case e: Exception =>
              logError(s"Removing $objId failed", e)
              writeInt(dos, -1)
              writeString(dos, s"Removing $objId failed: ${e.getMessage}")
          }
        case _ =>
          dos.writeInt(-1)
          writeString(dos, s"Error: unknown method $methodName")
      }
    } else {
      // To avoid timeouts when reading results in SparkR driver, we will be regularly sending
      // heartbeat responses. We use special code +1 to signal the client that backend is
      // alive and it should continue blocking for result.
      val execService = ThreadUtils.newDaemonSingleThreadScheduledExecutor("SparkRKeepAliveThread")
      val pingRunner = new Runnable {
        override def run(): Unit = {
          val pingBaos = new ByteArrayOutputStream()
          val pingDaos = new DataOutputStream(pingBaos)
          writeInt(pingDaos, +1)
          ctx.write(pingBaos.toByteArray)
        }
      }
      val conf = new SparkConf()
      val heartBeatInterval = conf.getInt(
        "spark.r.heartBeatInterval", SparkRDefaults.DEFAULT_HEARTBEAT_INTERVAL)
      val backendConnectionTimeout = conf.getInt(
        "spark.r.backendConnectionTimeout", SparkRDefaults.DEFAULT_CONNECTION_TIMEOUT)
      val interval = Math.min(heartBeatInterval, backendConnectionTimeout - 1)

      execService.scheduleAtFixedRate(pingRunner, interval, interval, TimeUnit.SECONDS)
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos)
      execService.shutdown()
      execService.awaitTermination(1, TimeUnit.SECONDS)
    }

    val reply = bos.toByteArray
    ctx.write(reply)
  }

  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    cause match {
      case timeout: ReadTimeoutException =>
        // Do nothing. We don't want to timeout on read
        logWarning("Ignoring read timeout in RBackendHandler")
      case _ =>
        // Close the connection when an exception is raised.
        cause.printStackTrace()
        ctx.close()
    }
  }

  def handleMethodCall(
      isStatic: Boolean,
      objId: String,
      methodName: String,
      numArgs: Int,
      dis: DataInputStream,
      dos: DataOutputStream): Unit = {
    var obj: Object = null
    try {
      val cls = if (isStatic) {
        Utils.classForName(objId)
      } else {
        obj = server.jvmObjectTracker(JVMObjectId(objId))
        obj.getClass
      }

      val args = readArgs(numArgs, dis)

      val methods = cls.getMethods
      val selectedMethods = methods.filter(m => m.getName == methodName)
      if (selectedMethods.length > 0) {
        val index = findMatchedSignature(
          selectedMethods.map(_.getParameterTypes),
          args)

        if (index.isEmpty) {
          logWarning(s"cannot find matching method ${cls}.$methodName. "
            + s"Candidates are:")
          selectedMethods.foreach { method =>
            logWarning(s"$methodName(${method.getParameterTypes.mkString(",")})")
          }
          throw new Exception(s"No matched method found for $cls.$methodName")
        }

        val ret = selectedMethods(index.get).invoke(obj, args : _*)

        // Write status bit
        writeInt(dos, 0)
        writeObject(dos, ret.asInstanceOf[AnyRef], server.jvmObjectTracker)
      } else if (methodName == "<init>") {
        // methodName should be "<init>" for constructor
        val ctors = cls.getConstructors
        val index = findMatchedSignature(
          ctors.map(_.getParameterTypes),
          args)

        if (index.isEmpty) {
          logWarning(s"cannot find matching constructor for ${cls}. "
            + s"Candidates are:")
          ctors.foreach { ctor =>
            logWarning(s"$cls(${ctor.getParameterTypes.mkString(",")})")
          }
          throw new Exception(s"No matched constructor found for $cls")
        }

        val obj = ctors(index.get).newInstance(args : _*)

        writeInt(dos, 0)
        writeObject(dos, obj.asInstanceOf[AnyRef], server.jvmObjectTracker)
      } else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Exception =>
        logError(s"$methodName on $objId failed", e)
        writeInt(dos, -1)
        // Writing the error message of the cause for the exception. This will be returned
        // to user in the R process.
        writeString(dos, Utils.exceptionString(e.getCause))
    }
  }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { _ =>
      readObject(dis, server.jvmObjectTracker)
    }.toArray
  }

  // Find a matching method signature in an array of signatures of constructors
  // or methods of the same name according to the passed arguments. Arguments
  // may be converted in order to match a signature.
  //
  // Note that in Java reflection, constructors and normal methods are of different
  // classes, and share no parent class that provides methods for reflection uses.
  // There is no unified way to handle them in this function. So an array of signatures
  // is passed in instead of an array of candidate constructors or methods.
  //
  // Returns an Option[Int] which is the index of the matched signature in the array.
  def findMatchedSignature(
      parameterTypesOfMethods: Array[Array[Class[_]]],
      args: Array[Object]): Option[Int] = {
    val numArgs = args.length

    for (index <- parameterTypesOfMethods.indices) {
      val parameterTypes = parameterTypesOfMethods(index)

      if (parameterTypes.length == numArgs) {
        var argMatched = true
        var i = 0
        while (i < numArgs && argMatched) {
          val parameterType = parameterTypes(i)

          if (parameterType == classOf[Seq[Any]] && args(i).getClass.isArray) {
            // The case that the parameter type is a Scala Seq and the argument
            // is a Java array is considered matching. The array will be converted
            // to a Seq later if this method is matched.
          } else {
            var parameterWrapperType = parameterType

            // Convert native parameters to Object types as args is Array[Object] here
            if (parameterType.isPrimitive) {
              parameterWrapperType = parameterType match {
                case java.lang.Integer.TYPE => classOf[java.lang.Integer]
                case java.lang.Long.TYPE => classOf[java.lang.Integer]
                case java.lang.Double.TYPE => classOf[java.lang.Double]
                case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
                case _ => parameterType
              }
            }
            if ((parameterType.isPrimitive || args(i) != null) &&
                !parameterWrapperType.isInstance(args(i))) {
              argMatched = false
            }
          }

          i = i + 1
        }

        if (argMatched) {
          // For now, we return the first matching method.
          // TODO: find best method in matching methods.

          // Convert args if needed
          val parameterTypes = parameterTypesOfMethods(index)

          for (i <- 0 until numArgs) {
            if (parameterTypes(i) == classOf[Seq[Any]] && args(i).getClass.isArray) {
              // Convert a Java array to scala Seq
              args(i) = args(i).asInstanceOf[Array[_]].toSeq
            }
          }

          return Some(index)
        }
      }
    }
    None
  }
}


