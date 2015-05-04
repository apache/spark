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

package org.apache.spark.sql.json

import java.io.ByteArrayOutputStream
import java.sql.Timestamp

import scala.collection.Map

import com.fasterxml.jackson.core._

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.catalyst.analysis.HiveTypeCoercion
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.types._
import org.apache.spark.Logging

private[sql] object JsonRDD2 extends Logging {
  def jsonStringToRow(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[Row] = {
    parseJson(json, schema, columnNameOfCorruptRecords)
  }

  /**
   * Infer the type of a collection of json records in three stages:
   *   1. Infer the type of each record
   *   2. Merge types by choosing the lowest type necessary to cover equal keys
   *   3. Replace any remaining null fields with string, the top type
   */
  def inferSchema(
      json: RDD[String],
      samplingRatio: Double = 1.0,
      columnNameOfCorruptRecords: String): StructType = {
    require(samplingRatio > 0, s"samplingRatio ($samplingRatio) should be greater than 0")
    val schemaData = if (samplingRatio > 0.99) {
      json
    } else {
      json.sample(withReplacement = false, samplingRatio, 1)
    }

    // perform schema inference on each row and merge afterwards
    schemaData.mapPartitions { iter =>
      val factory = new JsonFactory()
      iter.map { row =>
        try {
          val parser = factory.createParser(row)
          parser.nextToken()
          inferField(parser)
        } catch {
          case _: JsonParseException =>
            StructType(Seq(StructField(columnNameOfCorruptRecords, StringType)))
        }
      }
    }.treeAggregate[DataType](StructType(Seq()))(compatibleRootType, compatibleRootType) match {
      case st: StructType => nullTypeToStringType(st)
    }
  }

  /**
   * Infer the type of a json document from the parser's token stream
   */
  private def inferField(parser: JsonParser): DataType = {
    import com.fasterxml.jackson.core.JsonToken._
    parser.getCurrentToken match {
      case null | VALUE_NULL => NullType

      case FIELD_NAME =>
        parser.nextToken()
        inferField(parser)

      case VALUE_STRING if parser.getTextLength < 1 =>
        // Zero length strings and nulls have special handling to deal
        // with JSON generators that do not distinguish between the two.
        // To accurately infer types for empty strings that are really
        // meant to represent nulls we assume that the two are isomorphic
        // but will defer treating null fields as strings until all the
        // record fields' types have been combined.
        NullType

      case VALUE_STRING => StringType
      case START_OBJECT =>
        val builder = Seq.newBuilder[StructField]
        while (nextUntil(parser, END_OBJECT)) {
          builder += StructField(parser.getCurrentName, inferField(parser), nullable = true)
        }

        StructType(builder.result().sortBy(_.name))

      case START_ARRAY =>
        // If this JSON array is empty, we use NullType as a placeholder.
        // If this array is not empty in other JSON objects, we can resolve
        // the type as we pass through all JSON objects.
        var elementType: DataType = NullType
        while (nextUntil(parser, END_ARRAY)) {
          elementType = compatibleType(elementType, inferField(parser))
        }

        ArrayType(elementType)

      case VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT =>
        import JsonParser.NumberType._
        parser.getNumberType match {
          // For Integer values, use LongType by default.
          case INT | LONG => LongType
          // Since we do not have a data type backed by BigInteger,
          // when we see a Java BigInteger, we use DecimalType.
          case BIG_INTEGER | BIG_DECIMAL => DecimalType.Unlimited
          case FLOAT | DOUBLE => DoubleType
        }

      case VALUE_TRUE | VALUE_FALSE => BooleanType
    }
  }

  def nullTypeToStringType(struct: StructType): StructType = {
    val fields = struct.fields.map {
      case StructField(fieldName, dataType, nullable, _) =>
        val newType = dataType match {
          case NullType => StringType
          case ArrayType(NullType, containsNull) => ArrayType(StringType, containsNull)
          case ArrayType(struct: StructType, containsNull) =>
            ArrayType(nullTypeToStringType(struct), containsNull)
          case struct: StructType =>nullTypeToStringType(struct)
          case other: DataType => other
        }

        StructField(fieldName, newType, nullable)
    }

    StructType(fields)
  }

  /**
   * Advance the parser until a null or a specific token is found
   */
  private def nextUntil(parser: JsonParser, stopOn: JsonToken): Boolean = {
    parser.nextToken() match {
      case null => false
      case x => x != stopOn
    }
  }

  /**
   * Remove top-level ArrayType wrappers and merge the remaining schemas
   */
  private def compatibleRootType: (DataType, DataType) => DataType = {
    case (ArrayType(ty1, _), ty2) => compatibleRootType(ty1, ty2)
    case (ty1, ArrayType(ty2, _)) => compatibleRootType(ty1, ty2)
    case (ty1, ty2) => compatibleType(ty1, ty2)
  }

  /**
   * Returns the most general data type for two given data types.
   */
  private[json] def compatibleType(t1: DataType, t2: DataType): DataType = {
    HiveTypeCoercion.findTightestCommonType(t1, t2).getOrElse {
      // t1 or t2 is a StructType, ArrayType, or an unexpected type.
      (t1, t2) match {
        case (other: DataType, NullType) => other
        case (NullType, other: DataType) => other
        case (StructType(fields1), StructType(fields2)) =>
          val newFields = (fields1 ++ fields2).groupBy(field => field.name).map {
            case (name, fieldTypes) =>
              val dataType = fieldTypes.view.map(_.dataType).reduce(compatibleType)
              StructField(name, dataType, nullable = true)
          }
          StructType(newFields.toSeq.sortBy(_.name))

        case (ArrayType(elementType1, containsNull1), ArrayType(elementType2, containsNull2)) =>
          ArrayType(compatibleType(elementType1, elementType2), containsNull1 || containsNull2)

        // strings and every string is a Json object.
        case (_, _) => StringType
      }
    }
  }

  /**
   * Parse the current token (and related children) according to a desired schema
   */
  private[sql] def convertField(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Any = {
    import com.fasterxml.jackson.core.JsonToken._
    (parser.getCurrentToken, schema) match {
      case (null | VALUE_NULL, _) =>
        null

      case (FIELD_NAME, _) =>
        parser.nextToken()
        convertField(factory, parser, schema)

      case (VALUE_STRING, StringType) =>
        UTF8String(parser.getText)

      case (VALUE_STRING, _) if parser.getTextLength < 1 =>
        // guard the non string type
        null

      case (VALUE_STRING, DateType) =>
        DateUtils.millisToDays(DateUtils.stringToTime(parser.getText).getTime)

      case (VALUE_STRING, TimestampType) =>
        new Timestamp(DateUtils.stringToTime(parser.getText).getTime)

      case (VALUE_NUMBER_INT, TimestampType) =>
        new Timestamp(parser.getLongValue)

      case (_, StringType) =>
        val writer = new ByteArrayOutputStream()
        val generator = factory.createGenerator(writer, JsonEncoding.UTF8)
        generator.copyCurrentStructure(parser)
        generator.close()
        UTF8String(writer.toByteArray)

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, FloatType) =>
        parser.getFloatValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DoubleType) =>
        parser.getDoubleValue

      case (VALUE_NUMBER_INT | VALUE_NUMBER_FLOAT, DecimalType()) =>
        // TODO: add fixed precision and scale handling
        Decimal(parser.getDecimalValue)

      case (VALUE_NUMBER_INT, ByteType) =>
        parser.getByteValue

      case (VALUE_NUMBER_INT, ShortType) =>
        parser.getShortValue

      case (VALUE_NUMBER_INT, IntegerType) =>
        parser.getIntValue

      case (VALUE_NUMBER_INT, LongType) =>
        parser.getLongValue

      case (VALUE_TRUE, BooleanType) =>
        true

      case (VALUE_FALSE, BooleanType) =>
        false

      case (START_OBJECT, st: StructType) =>
        convertObject(factory, parser, st)

      case (START_ARRAY, ArrayType(st, _)) =>
        convertList(factory, parser, st)

      case (START_OBJECT, ArrayType(st, _)) =>
        // the business end of SPARK-3308:
        // when an object is found but an array is requested just wrap it in a list
        convertField(factory, parser, st) :: Nil

      case (START_OBJECT, MapType(StringType, kt, _)) =>
        convertMap(factory, parser, kt)

      case (_, udt: UserDefinedType[_]) =>
        udt.deserialize(convertField(factory, parser, udt.sqlType))
    }
  }

  /**
   * Parse an object from the token stream into a new Row representing the schema.
   *
   * Fields in the json that are not defined in the requested schema will be dropped.
   */
  private def convertObject(factory: JsonFactory, parser: JsonParser, schema: StructType): Row = {
    val row = new GenericMutableRow(schema.length)
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      schema.getFieldIndex(parser.getCurrentName) match {
        case Some(index) =>
          row.update(index, convertField(factory, parser, schema(index).dataType))

        case None =>
          parser.skipChildren()
      }
    }

    row
  }

  /**
   * Parse an object as a Map, preserving all fields
   */
  private def convertMap(
      factory: JsonFactory,
      parser: JsonParser,
      valueType: DataType): Map[String, Any] = {
    val builder = Map.newBuilder[String, Any]
    while (nextUntil(parser, JsonToken.END_OBJECT)) {
      builder += parser.getCurrentName -> convertField(factory, parser, valueType)
    }

    builder.result()
  }

  private def convertList(
      factory: JsonFactory,
      parser: JsonParser,
      schema: DataType): Seq[Any] = {
    val builder = Seq.newBuilder[Any]
    while (nextUntil(parser, JsonToken.END_ARRAY)) {
      builder += convertField(factory, parser, schema)
    }

    builder.result()
  }

  private def parseJson(
      json: RDD[String],
      schema: StructType,
      columnNameOfCorruptRecords: String): RDD[Row] = {

    def failedRecord(record: String): Seq[Row] = {
      // create a row even if no corrupt record column is present
      val row = new GenericMutableRow(schema.length)
      for (corruptIndex <- schema.getFieldIndex(columnNameOfCorruptRecords)) {
        require(schema(corruptIndex).dataType == StringType)
        row.update(corruptIndex, record)
      }

      Seq(row)
    }

    json.mapPartitions { iter =>
      val factory = new JsonFactory()

      iter.flatMap { record =>
        try {
          val parser = factory.createParser(record)
          parser.nextToken()

          // to support both object and arrays (see SPARK-3308) we'll start
          // by converting the StructType schema to an ArrayType and let
          // convertField wrap an object into a single value array when necessary.
          convertField(factory, parser, ArrayType(schema)) match {
            case null => failedRecord(record)
            case list: Seq[Row @unchecked] => list
            case _ =>
              sys.error(
                s"Failed to parse record $record. Please make sure that each line of the file " +
                  "(or each string in the RDD) is a valid JSON object or an array of JSON objects.")
          }
        } catch {
          case _: JsonProcessingException =>
            failedRecord(record)
        }
      }
    }
  }

  /** Transforms a single Row to JSON using Jackson
    *
    * @param rowSchema the schema object used for conversion
    * @param gen a JsonGenerator object
    * @param row The row to convert
    */
  private[sql] def rowToJSON(rowSchema: StructType, gen: JsonGenerator)(row: Row) = {
    def valWriter: (DataType, Any) => Unit = {
      case (_, null) | (NullType, _)  => gen.writeNull()
      case (StringType, v: String) => gen.writeString(v)
      case (TimestampType, v: java.sql.Timestamp) => gen.writeString(v.toString)
      case (IntegerType, v: Int) => gen.writeNumber(v)
      case (ShortType, v: Short) => gen.writeNumber(v)
      case (FloatType, v: Float) => gen.writeNumber(v)
      case (DoubleType, v: Double) => gen.writeNumber(v)
      case (LongType, v: Long) => gen.writeNumber(v)
      case (DecimalType(), v: java.math.BigDecimal) => gen.writeNumber(v)
      case (ByteType, v: Byte) => gen.writeNumber(v.toInt)
      case (BinaryType, v: Array[Byte]) => gen.writeBinary(v)
      case (BooleanType, v: Boolean) => gen.writeBoolean(v)
      case (DateType, v) => gen.writeString(v.toString)
      case (udt: UserDefinedType[_], v) => valWriter(udt.sqlType, udt.serialize(v))

      case (ArrayType(ty, _), v: Seq[_] ) =>
        gen.writeStartArray()
        v.foreach(valWriter(ty,_))
        gen.writeEndArray()

      case (MapType(kv,vv, _), v: Map[_,_]) =>
        gen.writeStartObject()
        v.foreach { p =>
          gen.writeFieldName(p._1.toString)
          valWriter(vv,p._2)
        }
        gen.writeEndObject()

      case (StructType(ty), v: Row) =>
        gen.writeStartObject()
        ty.zip(v.toSeq).foreach {
          case (_, null) =>
          case (field, v) =>
            gen.writeFieldName(field.name)
            valWriter(field.dataType, v)
        }
        gen.writeEndObject()
    }

    valWriter(rowSchema, row)
  }
}
