package org.apache.spark.sql.proto

import com.google.protobuf.DescriptorProtos.FileDescriptorSet
import org.apache.spark.sql.QueryTest
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.proto.SimpleMessageProtos.SimpleMessage
import java.io.{ByteArrayOutputStream, InputStream}

class ProtoFunctionsSuite extends QueryTest with SharedSparkSession with Serializable {
  import testImplicits._

  test("roundtrip in to_proto and from_proto - int and string") {
    //val df = spark.range(10).select($"id", $"id".cast("string").as("str"))
    val otherMessage = OtherMessageProtos
      .OtherMessage.newBuilder()
      .setKey("other_key")
      .setOther(123).build()

    val simpleMessage = SimpleMessage.newBuilder()
      .setKey("123")
      .setQuery("spark-query")
      .setTstamp(12090)
      .setResultsPerPage(123)
      .addArrayKey("value1")
      .addArrayKey("value2")
      .setOther(otherMessage)
      .build()

    val df = Seq(simpleMessage.toByteArray).toDF("value")
    val simpleMessageObj = SimpleMessage.newBuilder().build()
    val dfRes = df
      .select(functions.from_proto($"value", simpleMessageObj).as("value"))
      .select($"value.*")
    dfRes.show()
  }

  test("reading proto files from .pb format") {
    withTempPath { dir =>
      val df = spark.read.format("proto").load("/Users/sandishkumarhn/Downloads/latest.pb")
      df.show()
    }
  }

  def parseSchema(intputStream : InputStream) = {
    val buf = new Array[Byte](4096)
    val baos = new ByteArrayOutputStream()
    var len: Int = intputStream.read(buf)
    while(len > 0) {
      baos.write(buf, 0, len)
      len = intputStream.read(buf)
    }
    FileDescriptorSet.parseFrom(baos.toByteArray())
  }
//  public static DynamicSchema parseFrom (InputStream schemaDescIn) throws Descriptors.DescriptorValidationException
//  , IOException {
//    try {
//      byte[] buf = new byte[4096];
//      ByteArrayOutputStream baos = new ByteArrayOutputStream();
//
//      int len;
//      while ((len = schemaDescIn.read(buf)) > 0) {
//        baos.write(buf, 0, len);
//      }
//
//      DynamicSchema var4 = parseFrom(baos.toByteArray());
//      return var4;
//    } finally {
//      schemaDescIn.close();
//    }
//  }
}
