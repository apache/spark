package spark.network.netty

import java.util.concurrent.Executors

import io.netty.buffer.ByteBuf
import io.netty.channel.ChannelHandlerContext
import io.netty.util.CharsetUtil

import spark.Logging
import spark.network.ConnectionManagerId


private[spark] class ShuffleCopier extends Logging {

  def getBlock(cmId: ConnectionManagerId, blockId: String,
      resultCollectCallback: (String, Long, ByteBuf) => Unit) {

    val handler = new ShuffleCopier.ShuffleClientHandler(resultCollectCallback)
    val fc = new FileClient(handler)
    fc.init()
    fc.connect(cmId.host, cmId.port)
    fc.sendRequest(blockId)
    fc.waitForClose()
    fc.close()
  }

  def getBlocks(cmId: ConnectionManagerId,
    blocks: Seq[(String, Long)],
    resultCollectCallback: (String, Long, ByteBuf) => Unit) {

    for ((blockId, size) <- blocks) {
      getBlock(cmId, blockId, resultCollectCallback)
    }
  }
}


private[spark] object ShuffleCopier extends Logging {

  private class ShuffleClientHandler(resultCollectCallBack: (String, Long, ByteBuf) => Unit)
    extends FileClientHandler with Logging {

    override def handle(ctx: ChannelHandlerContext, in: ByteBuf, header: FileHeader) {
      logDebug("Received Block: " + header.blockId + " (" + header.fileLen + "B)");
      resultCollectCallBack(header.blockId, header.fileLen.toLong, in.readBytes(header.fileLen))
    }
  }

  def echoResultCollectCallBack(blockId: String, size: Long, content: ByteBuf) {
    logInfo("File: " + blockId + " content is : \" " + content.toString(CharsetUtil.UTF_8) + "\"")
  }

  def runGetBlock(host:String, port:Int, file:String){
    val handler = new ShuffleClientHandler(echoResultCollectCallBack)
    val fc = new FileClient(handler)
    fc.init();
    fc.connect(host, port)
    fc.sendRequest(file)
    fc.waitForClose();
    fc.close()
  }

  def main(args: Array[String]) {
    if (args.length < 3) {
      System.err.println("Usage: ShuffleCopier <host> <port> <shuffle_block_id> <threads>")
      System.exit(1)
    }
    val host = args(0)
    val port = args(1).toInt
    val file = args(2)
    val threads = if (args.length > 3) args(3).toInt else 10

    val copiers = Executors.newFixedThreadPool(80)
    for (i <- Range(0, threads)) {
      val runnable = new Runnable() {
        def run() {
          runGetBlock(host, port, file)
        }
      }
      copiers.execute(runnable)
    }
    copiers.shutdown
  }
}
