package spark.network.netty

import io.netty.buffer._

import spark.Logging

private[spark] class FileHeader (
  val fileLen: Int,
  val blockId: String) extends Logging {

  lazy val buffer = {
    val buf = Unpooled.buffer()
    buf.capacity(FileHeader.HEADER_SIZE)
    buf.writeInt(fileLen)
    buf.writeInt(blockId.length)
    blockId.foreach((x: Char) => buf.writeByte(x))
    //padding the rest of header
    if (FileHeader.HEADER_SIZE - buf.readableBytes > 0 ) {
      buf.writeZero(FileHeader.HEADER_SIZE - buf.readableBytes)
    } else {
      throw new Exception("too long header " + buf.readableBytes) 
      logInfo("too long header") 
    }
    buf
  }

}

private[spark] object FileHeader {

  val HEADER_SIZE = 40

  def getFileLenOffset = 0
  def getFileLenSize = Integer.SIZE/8

  def create(buf: ByteBuf): FileHeader = {
    val length = buf.readInt
    val idLength = buf.readInt
    val idBuilder = new StringBuilder(idLength)
    for (i <- 1 to idLength) {
      idBuilder += buf.readByte().asInstanceOf[Char]
    }
    val blockId = idBuilder.toString()
    new FileHeader(length, blockId)
  }


  def main (args:Array[String]){

    val header = new FileHeader(25,"block_0");
    val buf = header.buffer;
    val newheader = FileHeader.create(buf);
    System.out.println("id="+newheader.blockId+",size="+newheader.fileLen)

  }
}

