package edu.berkeley.cs.amplab.sparkr

import scala.collection.mutable.HashMap
import java.io._

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

import edu.berkeley.cs.amplab.sparkr.SerializeJavaR._

/**
 * Handler for SparkRBackend
 * TODO: This is marked as sharable to get a handle to SparkRBackend. Is it safe to re-use
 * this across connections ?
 */
@Sharable
class SparkRBackendHandler(server: SparkRBackend) extends SimpleChannelInboundHandler[Array[Byte]] {

  val objMap = new HashMap[String, Object]

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
        case "stopBackend" => {
          // dos.write(0)
          writeInt(dos, 0)
          writeType(dos, "void")
          server.close()
        }
        case "rm" => {
          try {
            val t = readObjectType(dis)
            assert(t == 'c')
            val objToRemove = readString(dis)
            objMap.remove(objToRemove)
            writeInt(dos, 0)
            writeObject(dos, null, objMap)
          } catch {
            case e: Exception => {
              System.err.println(s"Removing $objId failed with " + e)
              e.printStackTrace()
              writeInt(dos, -1)
            }
          }
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

  def handleMethodCall(isStatic: Boolean, objId: String, methodName: String,
    numArgs: Int, dis: DataInputStream, dos: DataOutputStream) {

    var obj: Object = null
    var cls: Option[Class[_]] = None
    try {
      if (isStatic) {
        cls = Some(Class.forName(objId))
      } else {
        objMap.get(objId) match {
          case None => throw new IllegalArgumentException("Object not found " + objId)
          case Some(o) => {
            cls = Some(o.getClass)
            obj = o
          }
        }
      }

      val args = readArgs(numArgs, dis)

      val methods = cls.get.getMethods()
      val selectedMethods = methods.filter(m => m.getName() == methodName)
      if (selectedMethods.length > 0) {
        val selectedMethod = selectedMethods.filter { x => 
          matchMethod(numArgs, args, x.getParameterTypes())
        }.head

        val ret = selectedMethod.invoke(obj, args:_*)

        // Write status bit
        writeInt(dos, 0)
        writeObject(dos, ret.asInstanceOf[AnyRef], objMap)
      } else if (methodName == "new") {
        // methodName should be "new" for constructor
        val ctor = cls.get.getConstructors().filter { x =>
          matchMethod(numArgs, args, x.getParameterTypes())
        }.head

        val obj = ctor.newInstance(args:_*)

        writeInt(dos, 0)
        writeObject(dos, obj.asInstanceOf[AnyRef], objMap)
      } else {
        throw new IllegalArgumentException("invalid method " + methodName + " for object " + objId)
      }
    } catch {
      case e: Exception => {
        System.err.println(s"$methodName on $objId failed with " + e)
        e.printStackTrace()
        writeInt(dos, -1)
      }
    }
  }

  def readArgs(numArgs: Int, dis: DataInputStream): Array[java.lang.Object] =  {
    val args = new Array[java.lang.Object](numArgs)
    for (i <- 0 to numArgs - 1) {
      args(i) = readObject(dis, objMap)
    }
    args
  }

  def matchMethod(numArgs: Int, args: Array[java.lang.Object], parameterTypes: Array[Class[_]]): Boolean = {
    if (parameterTypes.length != numArgs) {
      return false
    }
    // Currently we do exact match. We may add type conversion later.
    for (i <- 0 to numArgs - 1) {
      val parameterType = parameterTypes(i)
      var parameterWrapperType = parameterType

      if (parameterType.isPrimitive()) {
        parameterWrapperType = parameterType match {
          case java.lang.Integer.TYPE => classOf[java.lang.Integer]
          case java.lang.Double.TYPE => classOf[java.lang.Double]
          case java.lang.Boolean.TYPE => classOf[java.lang.Boolean]
          case _ => parameterType
        }
      }
      if(!parameterWrapperType.isInstance(args(i))) {
        return false
      }
    }
    return true
  }
}
