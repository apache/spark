package edu.berkeley.cs.amplab.sparkr

import java.io._

import io.netty.channel.ChannelHandler.Sharable
import io.netty.channel.ChannelHandlerContext
import io.netty.channel.SimpleChannelInboundHandler

import edu.berkeley.cs.amplab.sparkr.SerializeJavaR._

/**
 * Handler for SparkRBackend
 */
class SparkRBackendHandler(backend: SparkRBackendInterface, server: SparkRBackend)
    extends SimpleChannelInboundHandler[Array[Byte]] {

  override def channelRead0(ctx: ChannelHandlerContext, msg: Array[Byte]) {
    val bis = new ByteArrayInputStream(msg)
    val dis = new DataInputStream(bis)

    val bos = new ByteArrayOutputStream()
    val dos = new DataOutputStream(bos)

    // Read the RPC name first and then pass on the DataInputStream to
    // each handle function
    val rpcName = readString(dis)

    System.err.println(s"Handling $rpcName")

    rpcName match {
      case "createSparkContext" => handleCreateSparkContext(dis, dos)
      case "stopBackend" => {
        dos.write(0)
        server.close()
      }
      case _ => dos.writeInt(-1)
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

  // Handler functions read arguments off the DataInputStream and
  // call the backend. 
  def handleCreateSparkContext(dis: DataInputStream, dos: DataOutputStream) = {
    try {
      val master = readString(dis)
      val appName = readString(dis)
      val sparkHome = readString(dis)
      val jars = readStringArr(dis)
      val sparkEnvirMap = readStringMap(dis)
      val sparkExecutorEnvMap = readStringMap(dis)
      val appId = backend.createSparkContext(master, appName, sparkHome, jars, sparkEnvirMap,
        sparkExecutorEnvMap)

      writeInt(dos, 0)
      writeString(dos, appId)
    } catch {
      case e: Exception => {
        System.err.println("handleCreateSparkContext failed with " + e)
        writeInt(dos, -1)
      }
    }
  }

}
