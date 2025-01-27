// To generate a jar from the source file:
// `scalac StubClassDummyUdf.scala -d udf.jar`
// To remove class A from the jar:
// `jar -xvf udf.jar` -> delete A.class and A$.class
// `jar -cvf udf_noA.jar org/`
class StubClassDummyUdf {
  val udf: Int => Int = (x: Int) => x + 1
  val dummy = (x: Int) => A(x)
}

case class A(x: Int) { def get: Int = x + 5 }

// The code to generate the udf file
object StubClassDummyUdf {
  import java.io.{BufferedOutputStream, File, FileOutputStream}

  import org.apache.spark.sql.catalyst.encoders.AgnosticEncoders.PrimitiveIntEncoder
  import org.apache.spark.sql.connect.common.UdfPacket

  def packDummyUdf(): String = {
    val byteArray =
      Utils.serialize[UdfPacket](
        new UdfPacket(
          new StubClassDummyUdf().udf,
          Seq(PrimitiveIntEncoder),
          PrimitiveIntEncoder
        )
      )
    val file = new File("src/test/resources/udf")
    val target = new BufferedOutputStream(new FileOutputStream(file))
    try {
      target.write(byteArray)
      file.getAbsolutePath
    } finally {
      target.close
    }
  }
}
