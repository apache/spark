package org.apache.spark.sql.proto

import com.google.protobuf.DynamicMessage
import com.google.protobuf.Descriptors.Descriptor
import org.apache.spark.sql.catalyst.NoopFilters
import org.apache.spark.sql.catalyst.util.RebaseDateTime.RebaseSpec
import org.apache.spark.sql.internal.SQLConf.LegacyBehaviorPolicy.CORRECTED
import org.apache.spark.sql.proto.SchemaConverters.IncompatibleSchemaException
import org.apache.spark.sql.test.SharedSparkSession
import org.apache.spark.sql.types.{IntegerType, StructType}

/**
 * Tests for [[ProtoSerializer]] and [[ProtoDeserializer]]
 * with a more specific focus on those classes.
 */
class ProtoSerdeSuite extends SharedSparkSession {
  import ProtoSerdeSuite._
  import ProtoSerdeSuite.MatchType._
  val testFileDesc = testFile("protobuf/proto_serde_suite.desc").replace("file:/", "/")

  test("Test basic conversion") {
    withFieldMatchType { fieldMatch =>
      val (top, nest) = fieldMatch match {
        case BY_NAME => ("foo", "bar")
        case BY_POSITION => ("NOTfoo", "NOTbar")
      }
      val  protoFile = ProtoUtils.buildDescriptor(testFileDesc, "CleanMessage")

      val dynamicMessageFoo = DynamicMessage.newBuilder(protoFile.getFile.findMessageTypeByName("Foo")).setField(
          protoFile.getFile.findMessageTypeByName("Foo").findFieldByName("bar"), 10902).build()

      val dynamicMessage = DynamicMessage.newBuilder(protoFile)
        .setField(protoFile.findFieldByName("foo"), dynamicMessageFoo).build()

      val serializer = Serializer.create(CATALYST_STRUCT, protoFile, fieldMatch)
      val deserializer = Deserializer.create(CATALYST_STRUCT, protoFile, fieldMatch)

      assert(serializer.serialize(deserializer.deserialize(dynamicMessage).get) === dynamicMessage)
    }
  }

  test("Fail to convert with field type mismatch") {
    val protoFile = ProtoUtils.buildDescriptor(testFileDesc, "MissMatchTypeInRoot")

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(protoFile, Deserializer, fieldMatch,
        "Cannot convert Proto field 'foo' to SQL field 'foo' because schema is incompatible " +
          s"""(protoType = org.apache.spark.sql.proto.MissMatchTypeInRoot.foo LABEL_OPTIONAL LONG INT64, sqlType = ${CATALYST_STRUCT.head.dataType.sql})""")

      assertFailedConversionMessage(protoFile, Serializer, fieldMatch,
        s"Cannot convert SQL field 'foo' to Proto field 'foo' because schema is incompatible " +
          s"""(sqlType = ${CATALYST_STRUCT.head.dataType.sql}, protoType = LONG)""")
    }
  }

  test("Fail to convert with missing nested Proto fields") {
    val protoFile = ProtoUtils.buildDescriptor(testFileDesc, "FieldMissingInProto")

    val nonnullCatalyst = new StructType()
      .add("foo", new StructType().add("bar", IntegerType, nullable = false))
    // Positional matching will work fine with the name change, so add a new field
    val extraNonnullCatalyst = new StructType().add("foo",
      new StructType().add("bar", IntegerType).add("baz", IntegerType, nullable = false))

    // deserialize should have no issues when 'bar' is nullable but fail when it is nonnull
    Deserializer.create(CATALYST_STRUCT, protoFile, BY_NAME)
    assertFailedConversionMessage(protoFile, Deserializer, BY_NAME,
      "Cannot find field 'foo.bar' in Proto schema",
      nonnullCatalyst)
    assertFailedConversionMessage(protoFile, Deserializer, BY_POSITION,
      "Cannot find field at position 1 of field 'foo' from Proto schema (using positional matching)",
      extraNonnullCatalyst)

    // serialize fails whether or not 'bar' is nullable
    val byNameMsg = "Cannot find field 'foo.bar' in Proto schema"
    assertFailedConversionMessage(protoFile, Serializer, BY_NAME, byNameMsg)
    assertFailedConversionMessage(protoFile, Serializer, BY_NAME, byNameMsg, nonnullCatalyst)
    assertFailedConversionMessage(protoFile, Serializer, BY_POSITION,
      "Cannot find field at position 1 of field 'foo' from Proto schema (using positional matching)",
      extraNonnullCatalyst)
  }

  test("Fail to convert with deeply nested field type mismatch") {
    val protoFile = ProtoUtils.buildDescriptor(testFileDesc, "MissMatchTypeInDeepNested")
    val catalyst = new StructType().add("top", CATALYST_STRUCT)

    withFieldMatchType { fieldMatch =>
      assertFailedConversionMessage(protoFile, Deserializer, fieldMatch,
        "Cannot convert Proto field 'top.foo.bar' to SQL field 'top.foo.bar' because schema " +
          """is incompatible (protoType = org.apache.spark.sql.proto.TypeMiss.bar LABEL_OPTIONAL LONG INT64, sqlType = INT)""",
        catalyst)

      assertFailedConversionMessage(protoFile, Serializer, fieldMatch,
        "Cannot convert SQL field 'top.foo.bar' to Proto field 'top.foo.bar' because schema is " +
          """incompatible (sqlType = INT, protoType = LONG)""",
        catalyst)
    }
  }

  test("Fail to convert with missing Catalyst fields") {
    val protoFile = ProtoUtils.buildDescriptor(testFileDesc, "FieldMissingInSQLRoot")

    // serializing with extra fails if extra field is missing in SQL Schema
    assertFailedConversionMessage(protoFile, Serializer, BY_NAME,
      "Found field 'boo' in Proto schema but there is no match in the SQL schema")
    assertFailedConversionMessage(protoFile, Serializer, BY_POSITION,
      "Found field 'boo' at position 1 of top-level record from Proto schema but there is no " +
        "match in the SQL schema at top-level record (using positional matching)")

    // deserializing should work regardless of whether the extra field is missing in SQL Schema or not
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoFile, _))
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoFile, _))


    val protoNestedFile = ProtoUtils.buildDescriptor(testFileDesc, "FieldMissingInSQLNested")

    // serializing with extra fails if extra field is missing in SQL Schema
    assertFailedConversionMessage(protoNestedFile, Serializer, BY_NAME,
      "Found field 'foo.baz' in Proto schema but there is no match in the SQL schema")
    assertFailedConversionMessage(protoNestedFile, Serializer, BY_POSITION,
      s"Found field 'baz' at position 1 of field 'foo' from Proto schema but there is no match " +
        s"in the SQL schema at field 'foo' (using positional matching)")

    // deserializing should work regardless of whether the extra field is missing in SQL Schema or not
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoNestedFile, _))
    withFieldMatchType(Deserializer.create(CATALYST_STRUCT, protoNestedFile, _))
  }

  /**
   * Attempt to convert `catalystSchema` to `protoSchema` (or vice-versa if `deserialize` is true),
   * assert that it fails, and assert that the _cause_ of the thrown exception has a message
   * matching `expectedCauseMessage`.
   */
  private def assertFailedConversionMessage(protoSchema: Descriptor,
                                            serdeFactory: SerdeFactory[_],
                                            fieldMatchType: MatchType,
                                            expectedCauseMessage: String,
                                            catalystSchema: StructType = CATALYST_STRUCT): Unit = {
    val e = intercept[IncompatibleSchemaException] {
      serdeFactory.create(catalystSchema, protoSchema, fieldMatchType)
    }
    val expectMsg = serdeFactory match {
      case Deserializer =>
        s"Cannot convert Proto type ${protoSchema.getName} to SQL type ${catalystSchema.sql}."
      case Serializer =>
        s"Cannot convert SQL type ${catalystSchema.sql} to Proto type ${protoSchema.getName}."
    }

    assert(e.getMessage === expectMsg)
    assert(e.getCause.getMessage === expectedCauseMessage)
  }

  def withFieldMatchType(f: MatchType => Unit): Unit = {
    MatchType.values.foreach { fieldMatchType =>
      withClue(s"fieldMatchType == $fieldMatchType") {
        f(fieldMatchType)
      }
    }
  }
}


object ProtoSerdeSuite {

  private val CATALYST_STRUCT =
    new StructType().add("foo", new StructType().add("bar", IntegerType))

  /**
   * Specifier for type of field matching to be used for easy creation of tests that do both
   * positional and by-name field matching.
   */
  private object MatchType extends Enumeration {
    type MatchType = Value
    val BY_NAME, BY_POSITION = Value

    def isPositional(fieldMatchType: MatchType): Boolean = fieldMatchType == BY_POSITION
  }

  import MatchType._
  /**
   * Specifier for type of serde to be used for easy creation of tests that do both
   * serialization and deserialization.
   */
  private sealed trait SerdeFactory[T] {
    def create(sqlSchema: StructType, descriptor: Descriptor, fieldMatchType: MatchType): T
  }
  private object Serializer extends SerdeFactory[ProtoSerializer] {
    override def create(sql: StructType, descriptor: Descriptor, matchType: MatchType): ProtoSerializer =
      new ProtoSerializer(sql, descriptor, false, isPositional(matchType))
  }
  private object Deserializer extends SerdeFactory[ProtoDeserializer] {
    override def create(sql: StructType, descriptor: Descriptor, matchType: MatchType): ProtoDeserializer =
      new ProtoDeserializer(
        descriptor,
        sql,
        isPositional(matchType),
        RebaseSpec(CORRECTED),
        new NoopFilters)
  }
}
