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

import scala.collection.mutable.HashMap

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.{ChannelHandlerContext, SimpleChannelInboundHandler}

import org.apache.spark.Logging
import org.apache.spark.api.r.SerDe._

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
        case "stopBackend" =>
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()
        case "rm" =>
          try {
            val t = readObjectType(dis)
            assert(t == 'c')
            val objToRemove = readString(dis)
            JVMObjectTracker.remove(objToRemove)
            writeInt(dos, 0)
            writeObject(dos, null)
          } catch {
            case e: Exception =>
              logError(s"Removing $objId failed", e)
              writeInt(dos, -1)
          }
        case _ => dos.writeInt(-1)
      }
    } else {
      handleMethodCall(isStatic, objId, methodName, numArgs, dis, dos)
    }

    val reply = bos.toByteArray
    ctx.write(reply)
  }
  
  override def channelReadComplete(ctx: ChannelHandlerContext): Unit = {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable): Unit = {
    // Close the connection when an exception is raised.
    cause.printStackTrace()
    ctx.close()
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
        Class.forName(objId)
      } else {
        JVMObjectTracker.get(objId) match {
          case None => throw new IllegalArgumentException("Object not found " + objId)
          case Some(o) =>
            obj = o
            o.getClass
        }
      }

      val args = readArgs(numArgs, dis)

      val methods = cls.getMethods
      val selectedMethods = methods.filter(m => m.getName == methodName)
      if (selectedMethods.length > 0) {
        val methods = selectedMethods.filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes)
        }
        if (methods.isEmpty) {
          logWarning(s"cannot find matching method ${cls}.$methodName. "
            + s"Candidates are:")
          selectedMethods.foreach { method =>
            logWarning(s"$methodName(${method.getParameterTypes.mkString(",")})")
          }
          throw new Exception(s"No matched method found for $cls.$methodName")
        }
        val ret = methods.head.invoke(obj, args:_*)

        // Write status bit
        writeInt(dos, 0)
        writeObject(dos, ret.asInstanceOf[AnyRef])
      } else if (methodName == "<init>") {
        // methodName should be "<init>" for constructor
        val ctor = cls.getConstructors.filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes)
        }.head

        val obj = ctor.newInstance(args:_*)

        writeInt(dos, 0)
        writeObject(dos, obj.asInstanceOf[AnyRef])
      } else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Exception =>
        logError(s"$methodName on $objId failed", e)
        writeInt(dos, -1)
    }
  }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] = {
    (0 until numArgs).map { arg =>
      readObject(dis)
    }.toArray
  }

  // Checks if the arguments passed in args matches the parameter types.
  // NOTE: Currently we do exact match. We may add type conversions later.
  def matchMethod(
      numArgs: Int,
      args: Array[java.lang.Object],
      parameterTypes: Array[Class[_]]): Boolean = {
    if (parameterTypes.length != numArgs) {
      return false
    }

    for (i <- 0 to numArgs - 1) {
      val parameterType = parameterTypes(i)
      var parameterWrapperType = parameterType

      // Convert native parameters to Object types as args is Array[Object] here
      if (parameterType.isPrimitive) {
        parameterWrapperType = parameterType match {
          case java.lang.Integer.TYPE => classOf[java.lang.Integer]
          case java.lang.Double.TYPE => classOf[java.lang.Double]
          case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
          case _ => parameterType
        }
      }
      if (!parameterWrapperType.isInstance(args(i))) {
        return false
      }
    }
    true
  }
}

/**
 * Helper singleton that tracks JVM objects returned to R.
 * This is useful for referencing these objects in RPC calls.
 */
private[r] object JVMObjectTracker {

  // TODO: This map should be thread-safe if we want to support multiple
  // connections at the same time
  private[this] val objMap = new HashMap[String, Object]

  // TODO: We support only one connection now, so an integer is fine.
  // Investigate using use atomic integer in the future.
  private[this] var objCounter: Int = 0

  def getObject(id: String): Object = {
    objMap(id)
  }

  def get(id: String): Option[Object] = {
    objMap.get(id)
  }

  def put(obj: Object): String = {
    val objId = objCounter.toString
    objCounter = objCounter + 1
    objMap.put(objId, obj)
    objId
  }

  def remove(id: String): Option[Object] = {
    objMap.remove(id)
  }

}
