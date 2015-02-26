package edu.berkeley.cs.amplab.sparkr

import org.apache.spark.Logging

import scala.collection.mutable.HashMap

import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.DataInputStream
import java.io.DataOutputStream

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

import edu.berkeley.cs.amplab.sparkr.SerDe._

/**
 * Handler for SparkRBackend
 * TODO: This is marked as sharable to get a handle to SparkRBackend. Is it safe to re-use
 * this across connections ?
 */
@Sharable
class SparkRBackendHandler(server: SparkRBackend)
  extends SimpleChannelInboundHandler[Array[Byte]] with Logging {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]) {
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
              System.err.println(s"Removing $objId failed with " + e)
              e.printStackTrace()
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
  
  override def channelReadComplete(ctx: ChannelHandlerContext) {
    ctx.flush()
  }

  override def exceptionCaught(ctx: ChannelHandlerContext, cause: Throwable) {
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
      dos: DataOutputStream) {
    var obj: Object = null
    var cls: Option[Class[_]] = None
    try {
      if (isStatic) {
        cls = Some(Class.forName(objId))
      } else {
        JVMObjectTracker.get(objId) match {
          case None => throw new IllegalArgumentException("Object not found " + objId)
          case Some(o) =>
            cls = Some(o.getClass)
            obj = o
        }
      }

      val args = readArgs(numArgs, dis)

      val methods = cls.get.getMethods
      val selectedMethods = methods.filter(m => m.getName == methodName)
      if (selectedMethods.length > 0) {
        val methods = selectedMethods.filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes)
        }
        if (methods.isEmpty) {
          throw new Exception(s"No matched method found for $cls.$methodName")
        }
        val ret = methods.head.invoke(obj, args:_*)

        // Write status bit
        writeInt(dos, 0)
        writeObject(dos, ret.asInstanceOf[AnyRef])
      } else if (methodName == "<init>") {
        // methodName should be "<init>" for constructor
        val ctor = cls.get.getConstructors.filter { x =>
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
        System.err.println(s"$methodName on $objId failed with " + e)
        e.printStackTrace()
        writeInt(dos, -1)
    }
  }

  // Read a number of arguments from the data input stream
  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] =  {
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
      logInfo(s"num of arguments numArgs not match with ${parameterTypes.length}")
      return false
    }

    for (i <- 0 to numArgs - 1) {
      val parameterType = parameterTypes(i)
      var parameterWrapperType = parameterType

      if (parameterType.isPrimitive) {
        parameterWrapperType = parameterType match {
          case java.lang.Integer.TYPE => classOf[java.lang.Integer]
          case java.lang.Double.TYPE => classOf[java.lang.Double]
          case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
          case _ => parameterType
        }
      }
      if (!parameterWrapperType.isInstance(args(i))) {
        logInfo(s"arg $i not match: type $parameterWrapperType != ${args(i)}")
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
object JVMObjectTracker {

  // TODO: This map should be thread-safe if we want to support multiple
  // connections at the same time
  val objMap = new HashMap[String, Object]

  // TODO: We support only one connection now, so an integer is fine.
  // Investiage using use atomic integer in the future.
  var objCounter: Int = 0

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
